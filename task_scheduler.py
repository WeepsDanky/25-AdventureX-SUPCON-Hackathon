import os
import paho.mqtt.client as mqtt
import json
import logging
import time
import csv
from datetime import datetime
from collections import deque
from typing import Dict, Any, Optional, List, Tuple
from itertools import cycle

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("TaskSchedulerAgentV2")

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "supos-ce-instance4.supos.app")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
TOPIC_ROOT = os.getenv("TOPIC_ROOT") or os.getenv("USERNAME") or os.getenv("USER") or "marks"

# --- Constants ---
# [MODIFIED] Updated location mapping for P3 rework
LOCATION_MAPPING = {
    "RawMaterial": "P0",
    "StationA": "P1",
    "StationB": "P3",
    "Conveyor_CQ_upper": "P6",
    "Conveyor_CQ_lower": "P6",
    "QualityCheck_output": "P8",
    "Warehouse": "P9",
    "Charging": "P10"
}
FACTORY_LINES = ["line1", "line2", "line3"]
AGV_CAPACITY = 2
JOB_TIMEOUT_SECONDS = 120  # 2 minutes

# [NEW] Proactive charging thresholds
LOW_BATTERY_THRESHOLD = 40.0
TARGET_CHARGE_LEVEL = 90.0

class EnhancedTaskScheduler:
    """
    An intelligent agent that schedules factory AGV tasks with dynamic roles,
    proactive charging, and awareness of the factory's real-time state.
    """
    def __init__(self, broker_host: str, broker_port: int, topic_root: str):
        self.topic_root = topic_root
        self.client_id = f"{topic_root}_enhanced_scheduler_agent_{int(time.time())}"

        # --- State Management ---
        # [MODIFIED] Unified task queues per line
        self.task_queues: Dict[str, Dict[str, deque]] = {
            line: {"feeder": deque(), "finisher": deque(), "rework": deque()}
            for line in FACTORY_LINES
        }
        
        # [NEW] State models for devices
        self.agv_states: Dict[str, Dict[str, Any]] = {}
        self.device_states: Dict[str, Dict[str, Any]] = {} # For stations and conveyors

        self.agv_jobs: Dict[str, Dict[str, Any]] = {}
        self.tasks_created_for_product: set[str] = set()
        self.line_cycler = cycle(FACTORY_LINES)

        # --- KPI Data Storage ---
        self.latest_kpi_data: Optional[Dict[str, Any]] = None

        # --- MQTT Client ---
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        logger.info(f"Connecting to MQTT Broker {broker_host}:{broker_port}")
        self.client.connect(broker_host, broker_port, 60)

    def on_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            logger.info("Successfully connected to MQTT Broker!")
            self._subscribe_to_topics()
        else:
            logger.error(f"Failed to connect, return code: {rc}")

    def _subscribe_to_topics(self):
        """[MODIFIED] Subscribe to all necessary topics for enhanced awareness."""
        topics = [
            (f"{self.topic_root}/warehouse/RawMaterial/status", 1),
            (f"{self.topic_root}/+/station/+/status", 1),
            (f"{self.topic_root}/+/conveyor/+/status", 1),
            (f"{self.topic_root}/+/agv/+/status", 1),
            (f"{self.topic_root}/+/alerts", 1),
            (f"{self.topic_root}/response/+", 1),
            (f"{self.topic_root}/kpi/status", 1)
        ]
        for topic, qos in topics:
            logger.info(f"Subscribing to topic: {topic}")
            self.client.subscribe(topic, qos)

    def on_message(self, client, userdata, msg):
        """[MODIFIED] Main message handler, routes messages to specific handlers."""
        try:
            topic_parts = msg.topic.split('/')
            payload = json.loads(msg.payload.decode('utf-8'))
            
            # Route based on topic structure
            if "kpi/status" in msg.topic: self.handle_kpi_status(payload)
            elif "warehouse/RawMaterial/status" in msg.topic: self.handle_raw_material_status(payload)
            elif "station" in msg.topic: self.handle_station_status(topic_parts[1], topic_parts[3], payload)
            elif "conveyor" in msg.topic: self.handle_conveyor_status(topic_parts[1], topic_parts[3], payload)
            elif "agv" in msg.topic: self.handle_agv_status(topic_parts[1], topic_parts[3], payload)
            elif "alerts" in msg.topic: self.handle_alert(topic_parts[1], payload)
            elif "response" in msg.topic: logger.debug(f"Command response received: {payload.get('response')}")
        except Exception as e:
            logger.error(f"Error processing message from topic '{msg.topic}': {e}", exc_info=True)

    # --- [NEW] Handlers for enhanced awareness ---
    def handle_station_status(self, line_id: str, device_id: str, payload: Dict[str, Any]):
        full_device_id = f"{line_id}_{device_id}"
        self.device_states[full_device_id] = {
            "status": payload.get("status"),
            "buffer": payload.get("buffer", []),
            "output_buffer": payload.get("output_buffer", [])
        }
        # Specifically handle QualityCheck for finisher tasks
        if device_id == "QualityCheck":
            for product_id in payload.get("output_buffer", []):
                self.add_task(product_id, "QualityCheck_output", "Warehouse", line_id)

    def handle_conveyor_status(self, line_id: str, device_id: str, payload: Dict[str, Any]):
        full_device_id = f"{line_id}_{device_id}"
        self.device_states[full_device_id] = {"status": payload.get("status")}
        # Specifically handle Conveyor_CQ for P3 rework tasks
        if device_id == "Conveyor_CQ":
            for product_id in payload.get("upper_buffer", []):
                self.add_task(product_id, "Conveyor_CQ_upper", "StationB", line_id)
            for product_id in payload.get("lower_buffer", []):
                self.add_task(product_id, "Conveyor_CQ_lower", "StationB", line_id)

    def handle_alert(self, line_id: str, payload: Dict[str, Any]):
        device_id = payload.get("device_id")
        full_device_id = f"{line_id}_{device_id}"
        if payload.get("alert_type") == "fault_injected":
            logger.warning(f"🚨【故障告警】产线 {line_id} 设备 {device_id}: {payload.get('symptom')}")
            self.device_states[full_device_id] = {"status": "fault"}
        elif payload.get("alert_type") == "fault_recovered":
            logger.info(f"✅【故障恢复】产线 {line_id} 设备 {device_id}")
            if full_device_id in self.device_states:
                self.device_states[full_device_id]["status"] = "idle" # Assume idle after recovery

    def handle_kpi_status(self, payload: Dict[str, Any]):
        """[NEW] Handle KPI status updates from the factory."""
        self.latest_kpi_data = payload
        logger.debug(f"KPI状态更新: {payload.get('total_products', 0)} 个产品，完成率: {payload.get('order_completion_rate', 0):.1f}%")

    # --- [MODIFIED] Core Logic with Smarter Rules ---
    
    def handle_raw_material_status(self, payload: Dict[str, Any]):
        for product_id in payload.get("buffer", []):
            self.add_task(product_id, "RawMaterial", "StationA")

    def handle_agv_status(self, line_id: str, agv_id: str, payload: Dict[str, Any]):
        full_agv_id = f"{line_id}_{agv_id}"
        new_status = payload.get("status")
        
        self.agv_states[full_agv_id] = {
            "status": new_status,
            "battery_level": payload.get("battery_level", 100.0),
            "current_point": payload.get("current_point"),
            "payload": payload.get("payload", []),
            "line_id": line_id, "agv_id": agv_id,
        }
        
        if new_status == "idle":
            logger.info(f"AGV {full_agv_id} is now idle at {payload.get('current_point')}. Battery: {payload.get('battery_level'):.1f}%.")
            if full_agv_id in self.agv_jobs:
                self.execute_next_step(full_agv_id)
            else:
                # [MODIFIED] Proactive charging check before scheduling
                if not self.check_and_start_proactive_charging(full_agv_id):
                    self.schedule_tasks()

    def add_task(self, product_id: str, from_loc: str, to_loc: str, line_id: Optional[str] = None):
        if product_id in self.tasks_created_for_product: return

        product_type_char = product_id.split('_')[1]
        
        if from_loc == "RawMaterial":
            target_line = next(self.line_cycler)
            self.task_queues[target_line]["feeder"].append(product_id)
            logger.info(f"【新 Feeder 任务】产线 {target_line}: 从 {from_loc} 到 {to_loc}，产品: {product_id}")
        elif from_loc == "QualityCheck_output":
            self.task_queues[line_id]["finisher"].append(product_id)
            logger.info(f"【新 Finisher 任务】产线 {line_id}: 从 {from_loc} 到 {to_loc}，产品: {product_id}")
        elif from_loc.startswith("Conveyor_CQ"):
            self.task_queues[line_id]["rework"].append({"product_id": product_id, "from": from_loc, "to": to_loc})
            logger.info(f"【新 P3 返工任务】产线 {line_id}: 从 {from_loc} 到 {to_loc}，产品: {product_id}")

        self.tasks_created_for_product.add(product_id)
        self.schedule_tasks()

    def check_and_start_proactive_charging(self, agv_id: str) -> bool:
        """[NEW] Implements proactive charging strategy."""
        state = self.agv_states.get(agv_id)
        if state and state["battery_level"] < LOW_BATTERY_THRESHOLD:
            logger.info(f"🔋【主动充电】AGV {agv_id} 电量低 ({state['battery_level']:.1f}%)，前往充电。")
            self.send_charge_command(state["line_id"], state["agv_id"], TARGET_CHARGE_LEVEL)
            return True
        return False

    def schedule_tasks(self):
        """[REWRITTEN] Assigns tasks to any available AGV with pre-dispatch checks."""
        idle_agvs = [agv_id for agv_id, state in self.agv_states.items() 
                     if state['status'] == 'idle' and agv_id not in self.agv_jobs]
        
        if not idle_agvs: return

        # Task priority: Finisher > Rework > Feeder
        task_priority = ["finisher", "rework", "feeder"]
        
        for agv_id in idle_agvs:
            agv_state = self.agv_states[agv_id]
            # Skip AGVs that are low on battery
            if agv_state["battery_level"] < LOW_BATTERY_THRESHOLD:
                continue

            assigned = False
            for task_type in task_priority:
                for line_id in FACTORY_LINES:
                    if self.task_queues[line_id][task_type]:
                        # Pre-dispatch checks
                        from_loc, to_loc = self.get_task_locations(task_type, line_id)
                        if not self.is_destination_ok(line_id, to_loc):
                            logger.warning(f"【调度暂缓】目标 {to_loc} (产线 {line_id}) 不可用，跳过任务。")
                            continue

                        task_products = []
                        if task_type == "rework":
                            rework_job = self.task_queues[line_id][task_type].popleft()
                            task_products.append(rework_job["product_id"])
                            from_loc = rework_job["from"]
                        else: # Feeder and Finisher
                            while len(task_products) < AGV_CAPACITY and self.task_queues[line_id][task_type]:
                                task_products.append(self.task_queues[line_id][task_type].popleft())
                        
                        if task_products:
                            self.agv_jobs[agv_id] = {
                                "products": task_products,
                                "from": from_loc, "to": to_loc,
                                "step": "start",
                                "last_updated": time.time()
                            }
                            logger.info(f"【任务分配-动态】AGV {agv_id} 分配到 {task_type} 任务: 运送 {len(task_products)} 个产品 ({', '.join(task_products)}) 从 {from_loc} 到 {to_loc}")
                            self.execute_next_step(agv_id)
                            assigned = True
                            break
                if assigned: break
            if assigned: continue

    def is_destination_ok(self, line_id: str, to_loc_name: str) -> bool:
        """[NEW] Checks if a destination device is operational and not full."""
        # Find the device ID based on the to_loc_name (e.g., StationA, Warehouse)
        device_id = None
        if to_loc_name == "StationA": device_id = f"{line_id}_StationA"
        elif to_loc_name == "StationB": device_id = f"{line_id}_StationB"
        elif to_loc_name == "Warehouse": device_id = "Warehouse" # Global device
        
        if not device_id or device_id not in self.device_states:
            return True # If we don't have state info, assume it's OK

        state = self.device_states[device_id]
        if state.get("status") == "fault":
            return False
        if len(state.get("buffer", [])) >= AGV_CAPACITY: # Simplified check
            return False
        return True

    def get_task_locations(self, task_type: str, line_id: str) -> Tuple[str, str]:
        """[NEW] Helper to get from/to locations based on task type."""
        if task_type == "feeder": return "RawMaterial", "StationA"
        if task_type == "finisher": return "QualityCheck_output", "Warehouse"
        if task_type == "rework":
            # This is a bit tricky as the specific from_loc is in the task item
            # We'll return a placeholder and the actual from_loc is taken from the job item
            return "Conveyor_CQ", "StationB" 
        return "", ""

    def execute_next_step(self, full_agv_id: str):
        job, agv_state = self.agv_jobs.get(full_agv_id), self.agv_states[full_agv_id]
        if not job: self.schedule_tasks(); return

        line_id, agv_id = agv_state["line_id"], agv_state["agv_id"]
        from_loc, to_loc, products = job["from"], job["to"], job["products"]
        
        # Use .get() with a default for locations not in the main mapping
        pickup_point = LOCATION_MAPPING.get(from_loc, "P_UNKNOWN")
        dropoff_point = LOCATION_MAPPING.get(to_loc, "P_UNKNOWN")

        if pickup_point == "P_UNKNOWN" or dropoff_point == "P_UNKNOWN":
            logger.error(f"无法为任务 {job} 找到路径点。From: {from_loc}, To: {to_loc}")
            # Cleanup failed job
            del self.agv_jobs[full_agv_id]
            return

        step = job["step"]

        # The state machine logic remains largely the same
        if step == "start":
            job["step"] = "moving_to_pickup"
            logger.info(f"  [步骤 1] AGV {full_agv_id}: 前往取货点 {pickup_point} ({from_loc})")
            self.send_move_command(line_id, agv_id, pickup_point)
        elif step == "moving_to_pickup" and agv_state["current_point"] == pickup_point:
            job["step"] = "loading"
            logger.info(f"  [步骤 2] AGV {full_agv_id}: 到达取货点，开始装载 {len(products)} 个产品")
            self.send_load_command(line_id, agv_id, products[0] if from_loc == "RawMaterial" else None)
        elif step == "loading":
            if len(agv_state["payload"]) < len(products):
                next_product_idx = len(agv_state["payload"])
                logger.info(f"  [步骤 2.{next_product_idx+1}] AGV {full_agv_id}: 继续装载第 {next_product_idx+1} 个产品")
                self.send_load_command(line_id, agv_id, products[next_product_idx] if from_loc == "RawMaterial" else None)
            else:
                job["step"] = "moving_to_dropoff"
                logger.info(f"  [步骤 3] AGV {full_agv_id}: 装载完成，前往卸货点 {dropoff_point} ({to_loc})")
                self.send_move_command(line_id, agv_id, dropoff_point)
        elif step == "moving_to_dropoff" and agv_state["current_point"] == dropoff_point:
            job["step"] = "unloading"
            logger.info(f"  [步骤 4] AGV {full_agv_id}: 到达卸货点，开始卸载")
            self.send_unload_command(line_id, agv_id)
        elif step == "unloading":
            if agv_state["payload"]:
                logger.info(f"  [步骤 4.{len(products) - len(agv_state['payload'])}] AGV {full_agv_id}: 继续卸载")
                self.send_unload_command(line_id, agv_id)
            else:
                logger.info(f"【任务完成】AGV {full_agv_id} 完成了运送 {', '.join(products)} 的任务。")
                for pid in products: self.tasks_created_for_product.discard(pid)
                del self.agv_jobs[full_agv_id]
                # After finishing a job, check for charging needs before scheduling next one
                if not self.check_and_start_proactive_charging(full_agv_id):
                    self.schedule_tasks()
        
        if full_agv_id in self.agv_jobs:
            self.agv_jobs[full_agv_id]["last_updated"] = time.time()

    def cleanup_stuck_jobs(self):
        """[MODIFIED] Finds and cleans up stuck jobs, requeuing to the correct dynamic queue."""
        now = time.time()
        stuck_agvs = [agv_id for agv_id, job in self.agv_jobs.items() if now - job.get("last_updated", now) > JOB_TIMEOUT_SECONDS]

        for agv_id in stuck_agvs:
            job = self.agv_jobs[agv_id]
            logger.warning(f"【任务超时】AGV {agv_id} 任务卡住，正在清理。Job: {job}")

            state = self.agv_states.get(agv_id)
            if not state: logger.error(f"无法为 AGV {agv_id} 找到状态，无法重新调度产品。"); continue
            
            line_id = state.get("line_id")
            from_loc = job.get("from")
            task_type = None
            if from_loc == "RawMaterial": task_type = "feeder"
            elif from_loc == "QualityCheck_output": task_type = "finisher"
            elif from_loc.startswith("Conveyor_CQ"): task_type = "rework"

            if task_type and line_id in self.task_queues:
                for product_id in reversed(job.get("products", [])):
                    self.tasks_created_for_product.discard(product_id)
                    if task_type == "rework":
                        self.task_queues[line_id][task_type].appendleft({"product_id": product_id, "from": from_loc, "to": job.get("to")})
                    else:
                        self.task_queues[line_id][task_type].appendleft(product_id)

            del self.agv_jobs[agv_id]
            logger.info(f"【任务清理】AGV {agv_id} 的任务已重置，产品已返回队列。")
    
    # --- Command Sending ---
    def _send_command(self, line_id: str, command: Dict[str, Any]):
        topic = f"{self.topic_root}/command/{line_id}"
        command_id = f"{command['action']}_{command.get('target', 'N/A')}_{int(time.time() * 1000)}"
        payload = json.dumps({"command_id": command_id, **command})
        logger.info(f"发布指令到 '{topic}': {payload}")
        self.client.publish(topic, payload, qos=1)

    def send_move_command(self, line_id: str, agv_id: str, target_point: str):
        self._send_command(line_id, {"action": "move", "target": agv_id, "params": {"target_point": target_point}})

    def send_load_command(self, line_id: str, agv_id: str, product_id: Optional[str]):
        self._send_command(line_id, {"action": "load", "target": agv_id, "params": {"product_id": product_id} if product_id else {}})

    def send_unload_command(self, line_id: str, agv_id: str):
        self._send_command(line_id, {"action": "unload", "target": agv_id, "params": {}})
    
    def send_charge_command(self, line_id: str, agv_id: str, target_level: float):
        self._send_command(line_id, {"action": "charge", "target": agv_id, "params": {"target_level": target_level}})

    def calculate_and_save_kpi(self):
        # This function remains unchanged.
        if not self.latest_kpi_data: logger.warning("没有KPI数据，无法生成报告。"); return
        kpis = self.latest_kpi_data
        logger.info("正在计算最终KPI得分并生成报告...")
        weights = {'production_efficiency': 0.4, 'quality_cost': 0.3, 'agv_efficiency': 0.3}
        efficiency_weights = {'order_completion': 0.4, 'production_cycle': 0.4, 'device_utilization': 0.2}
        quality_cost_weights = {'first_pass_rate': 0.4, 'cost_efficiency': 0.6}
        agv_weights = {'charge_strategy': 0.3, 'energy_efficiency': 0.4, 'utilization': 0.3}
        production_cycle_score = min(100, 100 / max(1, kpis.get('average_production_cycle', 1))) if kpis.get('total_products', 0) > 0 else 0
        baseline_cost_per_product, total_products, total_cost = 25, kpis.get('total_products', 0), kpis.get('total_production_cost', 0)
        cost_efficiency_score = min(100, (baseline_cost_per_product * total_products) / max(1, total_cost) * 100) if total_products > 0 else 0
        efficiency_score = (kpis.get('order_completion_rate', 0) * efficiency_weights['order_completion'] + production_cycle_score * efficiency_weights['production_cycle'] + kpis.get('device_utilization', 0) * efficiency_weights['device_utilization']) * weights['production_efficiency']
        quality_cost_score = (kpis.get('first_pass_rate', 0) * quality_cost_weights['first_pass_rate'] + cost_efficiency_score * quality_cost_weights['cost_efficiency']) * weights['quality_cost']
        agv_energy_efficiency_score = min(100, kpis.get('agv_energy_efficiency', 0) * 1000)
        agv_score = (kpis.get('charge_strategy_efficiency', 0) * agv_weights['charge_strategy'] + agv_energy_efficiency_score * agv_weights['energy_efficiency'] + kpis.get('agv_utilization', 0) * agv_weights['utilization']) * weights['agv_efficiency']
        total_score = efficiency_score + quality_cost_score + agv_score
        
        filename = f"kpi_results_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        report_data = {"总得分": f"{total_score:.2f}", "生产效率得分 (40%)": f"{efficiency_score:.2f}", **{f"  - {k}": f"{v:.2f}" for k, v in {"订单完成率 (%)": kpis.get('order_completion_rate', 0), "生产周期效率得分": production_cycle_score, "设备利用率 (%)": kpis.get('device_utilization', 0)}.items()}, "质量与成本得分 (30%)": f"{quality_cost_score:.2f}", **{f"  - {k}": f"{v:.2f}" for k, v in {"一次通过率 (%)": kpis.get('first_pass_rate', 0), "成本效率得分 (估算)": cost_efficiency_score}.items()}, "AGV效率得分 (30%)": f"{agv_score:.2f}", **{f"  - {k}": f"{v:.2f}" for k, v in {"充电策略效率 (%)": kpis.get('charge_strategy_efficiency', 0), "能效比得分": agv_energy_efficiency_score, "AGV利用率 (%)": kpis.get('agv_utilization', 0)}.items()}, "--- 原始数据 ---": "---", **{k: v for k, v in {"总订单数": kpis.get('total_orders', 0), "已完成订单数": kpis.get('completed_orders', 0)}.items()}, **{k: f"${v:.2f}" for k, v in {"总生产成本": kpis.get('total_production_cost', 0), "物料成本": kpis.get('material_costs', 0), "能源成本": kpis.get('energy_costs', 0), "维修成本": kpis.get('maintenance_costs', 0), "报废成本": kpis.get('scrap_costs', 0)}.items()}}
        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f); writer.writerow(['KPI 指标', '值']); writer.writerows(report_data.items())
            logger.info(f"KPI报告已成功保存到文件: {filename}")
        except IOError as e: logger.error(f"无法写入KPI报告文件 {filename}: {e}")

    def run_forever(self):
        logger.info("增强版任务调度Agent已启动... (按 Ctrl+C 停止)")
        self.client.loop_start()
        try:
            while True:
                self.cleanup_stuck_jobs()
                time.sleep(10)
        except KeyboardInterrupt:
            logger.info("Agent被手动中断，正在关闭...")
        finally:
            self.calculate_and_save_kpi()
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("MQTT已断开连接。")

if __name__ == "__main__":
    scheduler = EnhancedTaskScheduler(MQTT_BROKER_HOST, MQTT_BROKER_PORT, TOPIC_ROOT)
    scheduler.run_forever()