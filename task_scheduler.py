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
logger = logging.getLogger("TaskSchedulerAgent")

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "supos-ce-instance4.supos.app")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
TOPIC_ROOT = os.getenv("TOPIC_ROOT") or os.getenv("USERNAME") or os.getenv("USER") or "marks"

# --- Constants ---
LOCATION_MAPPING = {
    "RawMaterial": "P0",
    "StationA": "P1",
    "QualityCheck_output": "P8",
    "Warehouse": "P9"
}
FACTORY_LINES = ["line1", "line2", "line3"]
AGV_CAPACITY = 2
AGV_ROLES = {
    "AGV_1": "feeder",       # RawMaterial -> StationA
    "AGV_2": "finisher"      # QualityCheck -> Warehouse
}

class TaskScheduler:
    """
    An intelligent agent that schedules factory AGV tasks based on predefined roles
    and optimizes for multi-product transport.
    """
    def __init__(self, broker_host: str, broker_port: int, topic_root: str):
        self.topic_root = topic_root
        self.client_id = f"{topic_root}_task_scheduler_agent_{int(time.time())}"
        
        # --- State Management ---
        self.feeder_tasks: Dict[str, deque] = {line: deque() for line in FACTORY_LINES}
        self.finisher_tasks: Dict[str, deque] = {line: deque() for line in FACTORY_LINES}
        
        self.agv_states: Dict[str, Dict[str, Any]] = {}
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
        """Subscribe to all necessary topics."""
        topics = [
            (f"{self.topic_root}/warehouse/RawMaterial/status", 1),
            (f"{self.topic_root}/+/station/QualityCheck/status", 1),
            (f"{self.topic_root}/+/agv/+/status", 1),
            (f"{self.topic_root}/response/+", 1),
            (f"{self.topic_root}/kpi/status", 1)
        ]
        for topic, qos in topics:
            logger.info(f"Subscribing to topic: {topic}")
            self.client.subscribe(topic, qos)

    def on_message(self, client, userdata, msg):
        """Main message handler, routes messages to specific handlers."""
        try:
            topic = msg.topic
            payload = json.loads(msg.payload.decode('utf-8'))
            
            if "kpi/status" in topic: self.handle_kpi_status(payload)
            elif "RawMaterial/status" in topic: self.handle_raw_material_status(payload)
            elif "QualityCheck/status" in topic: self.handle_quality_check_status(topic.split('/')[1], payload)
            elif "agv" in topic and "status" in topic: self.handle_agv_status(topic.split('/')[1], topic.split('/')[3], payload)
            elif "response" in topic: logger.debug(f"Command response received: {payload.get('response')}")
        except Exception as e:
            logger.error(f"Error processing message from topic '{msg.topic}': {e}", exc_info=True)

    def handle_kpi_status(self, payload: Dict[str, Any]):
        self.latest_kpi_data = payload

    def handle_raw_material_status(self, payload: Dict[str, Any]):
        """Creates feeder tasks for new products in the RawMaterial warehouse."""
        for product_id in payload.get("buffer", []):
            self.add_task(product_id, "RawMaterial", "StationA")

    def handle_quality_check_status(self, line_id: str, payload: Dict[str, Any]):
        """Creates finisher tasks for products in the QualityCheck output buffer."""
        for product_id in payload.get("output_buffer", []):
            self.add_task(product_id, "QualityCheck_output", "Warehouse", line_id)

    def handle_agv_status(self, line_id: str, agv_id: str, payload: Dict[str, Any]):
        """Updates AGV state and triggers scheduling or next job step."""
        full_agv_id = f"{line_id}_{agv_id}"
        old_state = self.agv_states.get(full_agv_id, {})
        new_status = payload.get("status")
        
        self.agv_states[full_agv_id] = {
            "status": new_status,
            "current_point": payload.get("current_point"),
            "payload": payload.get("payload", []),
            "line_id": line_id,
            "agv_id": agv_id,
            "role": AGV_ROLES.get(agv_id)
        }
        
        if (old_state.get("status") != "idle" and new_status == "idle") or (not old_state and new_status == "idle"):
            logger.info(f"AGV {full_agv_id} is now idle at {payload.get('current_point')}. Checking for work.")
            if full_agv_id in self.agv_jobs: self.execute_next_step(full_agv_id)
            else: self.schedule_tasks()

    def add_task(self, product_id: str, from_loc: str, to_loc: str, line_id: Optional[str] = None):
        """Adds a new task to the appropriate role-based queue."""
        if product_id in self.tasks_created_for_product: return
        
        if from_loc == "RawMaterial":
            # Assign feeder tasks to lines in a round-robin fashion
            target_line = next(self.line_cycler)
            self.feeder_tasks[target_line].append(product_id)
            logger.info(f"【新 Feeder 任务】产线 {target_line}: 从 {from_loc} 到 {to_loc}，产品: {product_id}")
        elif from_loc == "QualityCheck_output" and line_id:
            self.finisher_tasks[line_id].append(product_id)
            logger.info(f"【新 Finisher 任务】产线 {line_id}: 从 {from_loc} 到 {to_loc}，产品: {product_id}")

        self.tasks_created_for_product.add(product_id)
        self.schedule_tasks()

    def schedule_tasks(self):
        """Iterates through idle AGVs and assigns them tasks based on their role."""
        idle_agvs = [agv_id for agv_id, state in self.agv_states.items() if state['status'] == 'idle' and agv_id not in self.agv_jobs]
        
        for agv_id in idle_agvs:
            state = self.agv_states[agv_id]
            role = state.get("role")
            line_id = state.get("line_id")
            
            task_products = []
            if role == "feeder" and self.feeder_tasks[line_id]:
                while len(task_products) < AGV_CAPACITY and self.feeder_tasks[line_id]:
                    task_products.append(self.feeder_tasks[line_id].popleft())
                from_loc, to_loc = "RawMaterial", "StationA"
                
            elif role == "finisher" and self.finisher_tasks[line_id]:
                while len(task_products) < AGV_CAPACITY and self.finisher_tasks[line_id]:
                    task_products.append(self.finisher_tasks[line_id].popleft())
                from_loc, to_loc = "QualityCheck_output", "Warehouse"

            if task_products:
                self.agv_jobs[agv_id] = {
                    "products": task_products,
                    "from": from_loc, "to": to_loc,
                    "step": "start"
                }
                logger.info(f"【任务分配】AGV {agv_id} ({role}) 分配到任务: 运送 {len(task_products)} 个产品 ({', '.join(task_products)}) 从 {from_loc} 到 {to_loc}")
                self.execute_next_step(agv_id)

    def execute_next_step(self, full_agv_id: str):
        """Manages the multi-step execution of a job for an AGV."""
        job, agv_state = self.agv_jobs.get(full_agv_id), self.agv_states[full_agv_id]
        if not job: self.schedule_tasks(); return

        line_id, agv_id = agv_state["line_id"], agv_state["agv_id"]
        from_loc, to_loc, products = job["from"], job["to"], job["products"]
        step, pickup_point, dropoff_point = job["step"], LOCATION_MAPPING[from_loc], LOCATION_MAPPING[to_loc]

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
                self.schedule_tasks()
    
    def _send_command(self, line_id: str, command: Dict[str, Any]):
        topic = f"{self.topic_root}/command/{line_id}"
        command_id = f"{command['action']}_{command['target']}_{int(time.time() * 1000)}"
        payload = json.dumps({"command_id": command_id, **command})
        logger.info(f"发布指令到 '{topic}': {payload}")
        self.client.publish(topic, payload, qos=1)

    def send_move_command(self, line_id: str, agv_id: str, target_point: str):
        self._send_command(line_id, {"action": "move", "target": agv_id, "params": {"target_point": target_point}})

    def send_load_command(self, line_id: str, agv_id: str, product_id: Optional[str]):
        self._send_command(line_id, {"action": "load", "target": agv_id, "params": {"product_id": product_id} if product_id else {}})

    def send_unload_command(self, line_id: str, agv_id: str):
        self._send_command(line_id, {"action": "unload", "target": agv_id, "params": {}})

    def calculate_and_save_kpi(self):
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
        logger.info("任务调度Agent已启动... (按 Ctrl+C 停止)")
        try: self.client.loop_forever()
        except KeyboardInterrupt: logger.info("Agent被手动中断，正在关闭...")
        finally: self.calculate_and_save_kpi(); self.client.disconnect(); logger.info("MQTT已断开连接。")

if __name__ == "__main__":
    scheduler = TaskScheduler(MQTT_BROKER_HOST, MQTT_BROKER_PORT, TOPIC_ROOT)
    scheduler.run_forever()