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

# --- 配置日志记录 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("TaskSchedulerAgent")

# --- 从环境中读取MQTT配置 ---
MQTT_BROKER_HOST = "supos-ce-instance4.supos.app"
MQTT_BROKER_PORT = 1883
TOPIC_ROOT = "marks"

# --- 定义任务相关的常量 ---
LOCATION_MAPPING = {
    "RawMaterial": "P0",
    "StationA": "P1",
    "QualityCheck_output": "P8",
    "Warehouse": "P9"
}
FACTORY_LINES = ["line1", "line2", "line3"]

class TaskScheduler:
    """
    一个通过MQTT调度工厂AGV任务的代理，并在关闭时计算和保存KPI。
    """

    def __init__(self, broker_host: str, broker_port: int, topic_root: str):
        self.topic_root = topic_root
        self.client_id = f"{topic_root}_task_scheduler_agent_{int(time.time())}"
        
        # --- 状态管理 ---
        self.task_queue: deque[Tuple[str, Optional[str], str, str]] = deque()
        self.agv_states: Dict[str, Dict[str, Any]] = {}
        self.agv_jobs: Dict[str, Dict[str, Any]] = {}
        self.products_in_qc_buffer: Dict[str, List[str]] = {line: [] for line in FACTORY_LINES}
        self.tasks_created_for_product: set[str] = set()
        self.line_cycler = cycle(FACTORY_LINES)
        
        # --- KPI 数据存储 ---
        self.latest_kpi_data: Optional[Dict[str, Any]] = None

        # --- MQTT客户端设置 ---
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        logger.info(f"正在连接到 MQTT Broker {broker_host}:{broker_port} with client_id: {self.client_id}")
        self.client.connect(broker_host, broker_port, 60)

    def on_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            logger.info("成功连接到 MQTT Broker!")
            self._subscribe_to_topics()
        else:
            logger.error(f"连接失败，返回码: {rc}")

    def _subscribe_to_topics(self):
        """订阅所有需要的话题。"""
        # 修复: 将 'line/+' 改为 '+', 以正确匹配 'line1', 'line2' 等
        topics_to_subscribe = [
            (f"{self.topic_root}/orders/status", 1),
            (f"{self.topic_root}/warehouse/RawMaterial/status", 1),
            (f"{self.topic_root}/+/station/QualityCheck/status", 1),
            (f"{self.topic_root}/+/agv/+/status", 1),
            (f"{self.topic_root}/response/+", 1),
            (f"{self.topic_root}/kpi/status", 1)  # 新增: 订阅KPI话题
        ]
        for topic, qos in topics_to_subscribe:
            logger.info(f"订阅话题: {topic}")
            self.client.subscribe(topic, qos)

    def on_message(self, client, userdata, msg):
        try:
            topic_parts = msg.topic.split('/')
            payload = json.loads(msg.payload.decode('utf-8'))
            
            if "kpi/status" in msg.topic:
                self.handle_kpi_status(payload)
            elif "RawMaterial/status" in msg.topic:
                self.handle_raw_material_status(payload)
            elif "QualityCheck/status" in msg.topic:
                line_id = topic_parts[1]
                self.handle_quality_check_status(line_id, payload)
            elif "agv" in msg.topic and "status" in msg.topic:
                line_id = topic_parts[1]
                agv_id = topic_parts[3]
                self.handle_agv_status(line_id, agv_id, payload)
            elif "response" in msg.topic:
                self.handle_command_response(payload)
        except json.JSONDecodeError:
            logger.warning(f"无法解析来自话题 '{msg.topic}' 的JSON数据")
        except Exception as e:
            logger.error(f"处理消息时发生错误 (话题: {msg.topic}): {e}", exc_info=True)

    def handle_kpi_status(self, payload: Dict[str, Any]):
        """存储最新的KPI数据。"""
        logger.debug(f"接收到KPI更新: {payload}")
        self.latest_kpi_data = payload

    def handle_raw_material_status(self, payload: Dict[str, Any]):
        products = payload.get("buffer", [])
        if not products: return
        for product_id in products:
            if product_id not in self.tasks_created_for_product:
                line_id = next(self.line_cycler)
                self.add_task(line_id, product_id, "RawMaterial", "StationA")

    def handle_quality_check_status(self, line_id: str, payload: Dict[str, Any]):
        products_in_output = payload.get("output_buffer", [])
        known_products = set(self.products_in_qc_buffer.get(line_id, []))
        for product_id in products_in_output:
            if product_id not in known_products:
                self.products_in_qc_buffer[line_id].append(product_id)
                self.add_task(line_id, product_id, "QualityCheck_output", "Warehouse")
        self.products_in_qc_buffer[line_id] = [p for p in self.products_in_qc_buffer[line_id] if p in products_in_output]

    def handle_agv_status(self, line_id: str, agv_id: str, payload: Dict[str, Any]):
        full_agv_id = f"{line_id}_{agv_id}"
        old_status = self.agv_states.get(full_agv_id, {}).get("status")
        new_status = payload.get("status")
        self.agv_states[full_agv_id] = {"status": new_status, "current_point": payload.get("current_point"), "payload": payload.get("payload", []), "line_id": line_id, "agv_id": agv_id}
        logger.debug(f"AGV状态更新: {full_agv_id} - {self.agv_states[full_agv_id]}")
        if (old_status != "idle" and new_status == "idle") or (old_status is None and new_status == "idle"):
            logger.info(f"AGV {full_agv_id} 变为空闲，位于 {payload.get('current_point')}. 检查任务流程。")
            if full_agv_id in self.agv_jobs: self.execute_next_step(full_agv_id)
            else: self.schedule_tasks()

    def handle_command_response(self, payload: Dict[str, Any]):
        logger.debug(f"收到指令反馈: {payload.get('response')} (ID: {payload.get('command_id')})")

    def add_task(self, line_id: str, product_id: Optional[str], from_loc: str, to_loc: str):
        if product_id and product_id in self.tasks_created_for_product:
            logger.debug(f"产品 {product_id} 的任务已存在，跳过。")
            return
        task = (line_id, product_id, from_loc, to_loc)
        self.task_queue.append(task)
        if product_id: self.tasks_created_for_product.add(product_id)
        logger.info(f"【新任务】已添加任务到队列: 从 {from_loc} 到 {to_loc}，产品: {product_id or 'ANY'}, 产线: {line_id}")
        self.schedule_tasks()

    def schedule_tasks(self):
        if not self.task_queue:
            logger.debug("任务队列为空，无需调度。")
            return
        target_line_id = self.task_queue[0][0]
        idle_agvs_for_line = [agv_id for agv_id, state in self.agv_states.items() if state['status'] == 'idle' and state['line_id'] == target_line_id and agv_id not in self.agv_jobs]
        if not idle_agvs_for_line:
            logger.info(f"产线 {target_line_id} 没有空闲的AGV来执行任务。等待中...")
            return
        agv_to_use = idle_agvs_for_line[0]
        task_to_do = self.task_queue.popleft()
        line_id, product_id, from_loc, to_loc = task_to_do
        self.agv_jobs[agv_to_use] = {"task": task_to_do, "step": "start"}
        logger.info(f"【任务分配】将任务 '{product_id or 'ANY'}: {from_loc} -> {to_loc}' 分配给AGV {agv_to_use}")
        self.execute_next_step(agv_to_use)

    def execute_next_step(self, full_agv_id: str):
        job = self.agv_jobs.get(full_agv_id)
        if not job:
            logger.warning(f"AGV {full_agv_id} 没有进行中的任务，重新调度。")
            self.schedule_tasks()
            return
        agv_state = self.agv_states[full_agv_id]
        line_id, product_id, from_loc, to_loc = job["task"]
        step, pickup_point, dropoff_point = job["step"], LOCATION_MAPPING[from_loc], LOCATION_MAPPING[to_loc]
        if step == "start":
            logger.info(f"  [步骤 1/4] AGV {full_agv_id}: 前往取货点 {pickup_point} ({from_loc})")
            job["step"] = "moving_to_pickup"
            self.send_move_command(line_id, agv_state["agv_id"], pickup_point)
        elif step == "moving_to_pickup" and agv_state["current_point"] == pickup_point:
            logger.info(f"  [步骤 2/4] AGV {full_agv_id}: 到达取货点，开始装载")
            job["step"] = "loading"
            self.send_load_command(line_id, agv_state["agv_id"], product_id if from_loc == "RawMaterial" else None)
        elif step == "loading" and agv_state["payload"]:
            logger.info(f"  [步骤 3/4] AGV {full_agv_id}: 装载成功，前往卸货点 {dropoff_point} ({to_loc})")
            job["step"] = "moving_to_dropoff"
            self.send_move_command(line_id, agv_state["agv_id"], dropoff_point)
        elif step == "moving_to_dropoff" and agv_state["current_point"] == dropoff_point:
            logger.info(f"  [步骤 4/4] AGV {full_agv_id}: 到达卸货点，开始卸载")
            job["step"] = "unloading"
            self.send_unload_command(line_id, agv_state["agv_id"])
        elif step == "unloading" and not agv_state["payload"]:
            logger.info(f"【任务完成】AGV {full_agv_id} 完成了任务: '{product_id or 'ANY'}: {from_loc} -> {to_loc}'")
            if product_id and product_id in self.tasks_created_for_product: self.tasks_created_for_product.remove(product_id)
            del self.agv_jobs[full_agv_id]
            self.schedule_tasks()
        else:
            logger.debug(f"AGV {full_agv_id} 正在等待步骤 '{step}' 完成。当前状态: {agv_state['status']}, 位置: {agv_state['current_point']}")

    def _send_command(self, line_id: str, command: Dict[str, Any]):
        topic = f"{self.topic_root}/command/{line_id}"
        command_id = f"{command['action']}_{command['target']}_{int(time.time() * 1000)}"
        command_with_id = {"command_id": command_id, **command}
        payload = json.dumps(command_with_id)
        logger.info(f"发布指令到 '{topic}': {payload}")
        self.client.publish(topic, payload, qos=1)

    def send_move_command(self, line_id: str, agv_id: str, target_point: str):
        self._send_command(line_id, {"action": "move", "target": agv_id, "params": {"target_point": target_point}})

    def send_load_command(self, line_id: str, agv_id: str, product_id: Optional[str]):
        params = {"product_id": product_id} if product_id else {}
        self._send_command(line_id, {"action": "load", "target": agv_id, "params": params})

    def send_unload_command(self, line_id: str, agv_id: str):
        self._send_command(line_id, {"action": "unload", "target": agv_id, "params": {}})

    def calculate_and_save_kpi(self):
        """在脚本关闭时计算最终得分并保存到CSV文件。"""
        if not self.latest_kpi_data:
            logger.warning("没有收到任何KPI数据，无法生成报告。")
            return

        kpis = self.latest_kpi_data
        logger.info("正在计算最终KPI得分并生成报告...")

        # --- 计分权重 (源于项目文档) ---
        weights = {'production_efficiency': 0.40, 'quality_cost': 0.30, 'agv_efficiency': 0.30}
        efficiency_weights = {'order_completion': 0.40, 'production_cycle': 0.40, 'device_utilization': 0.20}
        quality_cost_weights = {'first_pass_rate': 0.40, 'cost_efficiency': 0.60}
        agv_weights = {'charge_strategy': 0.30, 'energy_efficiency': 0.40, 'utilization': 0.30}

        # --- 计分逻辑 (复刻自 kpi_calculator.py) ---
        # 生产周期评分
        production_cycle_score = min(100, 100 / max(1, kpis.get('average_production_cycle', 1))) if kpis.get('total_products', 0) > 0 else 0
        
        # 成本效率评分 (简化版：由于agent没有基准成本，我们假设成本越低分越高，设置一个任意的基准)
        baseline_cost_per_product = 25 
        total_products = kpis.get('total_products', 0)
        total_cost = kpis.get('total_production_cost', 0)
        cost_efficiency_score = min(100, (baseline_cost_per_product * total_products) / max(1, total_cost) * 100) if total_products > 0 else 0

        # 各大项得分
        efficiency_score = (kpis.get('order_completion_rate', 0) * efficiency_weights['order_completion'] + production_cycle_score * efficiency_weights['production_cycle'] + kpis.get('device_utilization', 0) * efficiency_weights['device_utilization']) * weights['production_efficiency']
        quality_cost_score = (kpis.get('first_pass_rate', 0) * quality_cost_weights['first_pass_rate'] + cost_efficiency_score * quality_cost_weights['cost_efficiency']) * weights['quality_cost']
        # 能效比评分 (假设 0.1 tasks/sec 为100分)
        agv_energy_efficiency_score = min(100, kpis.get('agv_energy_efficiency', 0) * 1000)
        agv_score = (kpis.get('charge_strategy_efficiency', 0) * agv_weights['charge_strategy'] + agv_energy_efficiency_score * agv_weights['energy_efficiency'] + kpis.get('agv_utilization', 0) * agv_weights['utilization']) * weights['agv_efficiency']
        total_score = efficiency_score + quality_cost_score + agv_score

        # --- 写入CSV ---
        timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"kpi_results_{timestamp_str}.csv"
        
        report_data = {
            "总得分": f"{total_score:.2f}",
            "生产效率得分 (40%)": f"{efficiency_score:.2f}",
            "  - 订单完成率 (%)": f"{kpis.get('order_completion_rate', 0):.2f}",
            "  - 生产周期效率得分": f"{production_cycle_score:.2f}",
            "  - 设备利用率 (%)": f"{kpis.get('device_utilization', 0):.2f}",
            "质量与成本得分 (30%)": f"{quality_cost_score:.2f}",
            "  - 一次通过率 (%)": f"{kpis.get('first_pass_rate', 0):.2f}",
            "  - 成本效率得分 (估算)": f"{cost_efficiency_score:.2f}",
            "AGV效率得分 (30%)": f"{agv_score:.2f}",
            "  - 充电策略效率 (%)": f"{kpis.get('charge_strategy_efficiency', 0):.2f}",
            "  - 能效比得分": f"{agv_energy_efficiency_score:.2f}",
            "  - AGV利用率 (%)": f"{kpis.get('agv_utilization', 0):.2f}",
            "--- 原始数据 ---": "---",
            "总订单数": kpis.get('total_orders', 0),
            "已完成订单数": kpis.get('completed_orders', 0),
            "总生产成本": f"${kpis.get('total_production_cost', 0):.2f}",
            "物料成本": f"${kpis.get('material_costs', 0):.2f}",
            "能源成本": f"${kpis.get('energy_costs', 0):.2f}",
            "维修成本": f"${kpis.get('maintenance_costs', 0):.2f}",
            "报废成本": f"${kpis.get('scrap_costs', 0):.2f}",
        }

        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['KPI 指标', '值'])
                for key, value in report_data.items():
                    writer.writerow([key, value])
            logger.info(f"KPI报告已成功保存到文件: {filename}")
        except IOError as e:
            logger.error(f"无法写入KPI报告文件 {filename}: {e}")

    def run_forever(self):
        logger.info("任务调度Agent已启动，正在监听MQTT消息... (按 Ctrl+C 停止)")
        try:
            self.client.loop_forever()
        except KeyboardInterrupt:
            logger.info("Agent被手动中断，正在关闭...")
        finally:
            self.calculate_and_save_kpi()
            self.client.disconnect()
            logger.info("MQTT已断开连接。")


if __name__ == "__main__":
    scheduler = TaskScheduler(MQTT_BROKER_HOST, MQTT_BROKER_PORT, TOPIC_ROOT)
    scheduler.run_forever()