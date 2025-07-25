import os
import paho.mqtt.client as mqtt
import json
import logging
import time
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
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "supos-ce-instance4.supos.app")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
TOPIC_ROOT = os.getenv("TOPIC_ROOT") or os.getenv("USERNAME") or os.getenv("USER") or "marks"

# --- 定义任务相关的常量 ---
LOCATION_MAPPING = {
    "RawMaterial": "P0",
    "StationA": "P1",
    "QualityCheck_output": "P8",
    "Warehouse": "P9"
}
# 定义工厂中的产线列表
FACTORY_LINES = ["line1", "line2", "line3"]

class TaskScheduler:
    """
    一个通过MQTT调度工厂AGV任务的代理。
    """

    def __init__(self, broker_host: str, broker_port: int, topic_root: str):
        self.topic_root = topic_root
        self.client_id = f"{topic_root}_task_scheduler_agent_{int(time.time())}"
        
        # --- 状态管理 ---
        # 任务队列，存储待执行的任务元组 (line_id, product_id, from_loc, to_loc)
        self.task_queue: deque[Tuple[str, Optional[str], str, str]] = deque()
        
        # AGV状态: {'line1_AGV_1': {'status': 'idle', 'current_point': 'P10', 'payload': []}}
        self.agv_states: Dict[str, Dict[str, Any]] = {}

        # 正在执行的任务: {'line1_AGV_1': {'task': ..., 'step': 'moving_to_pickup'}}
        self.agv_jobs: Dict[str, Dict[str, Any]] = {}
        
        # 防止重复添加任务
        self.products_in_qc_buffer: Dict[str, List[str]] = {line: [] for line in FACTORY_LINES}
        self.tasks_created_for_product: set[str] = set()

        # 用于在新任务间轮流分配产线
        self.line_cycler = cycle(FACTORY_LINES)

        # --- MQTT客户端设置 ---
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        logger.info(f"正在连接到 MQTT Broker {broker_host}:{broker_port} with client_id: {self.client_id}")
        self.client.connect(broker_host, broker_port, 60)

    def on_connect(self, client, userdata, flags, rc, properties):
        """连接成功后的回调函数，用于订阅话题。"""
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
            (f"{self.topic_root}/response/+", 1)
        ]
        for topic, qos in topics_to_subscribe:
            logger.info(f"订阅话题: {topic}")
            self.client.subscribe(topic, qos)

    def on_message(self, client, userdata, msg):
        """处理所有订阅到的消息。"""
        try:
            topic_parts = msg.topic.split('/')
            payload = json.loads(msg.payload.decode('utf-8'))
            
            # --- 根据话题分发消息 ---
            if "RawMaterial/status" in msg.topic:
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

    def handle_raw_material_status(self, payload: Dict[str, Any]):
        """处理原料仓库的状态更新，创建从仓库到工站A的任务。"""
        products = payload.get("buffer", [])
        if not products:
            return
        
        for product_id in products:
            if product_id not in self.tasks_created_for_product:
                # 优化: 轮流为不同产线分配任务
                line_id = next(self.line_cycler)
                self.add_task(line_id, product_id, "RawMaterial", "StationA")

    def handle_quality_check_status(self, line_id: str, payload: Dict[str, Any]):
        """处理质检站的状态更新，创建从质检站到成品仓的任务。"""
        products_in_output = payload.get("output_buffer", [])
        
        # 获取当前已知的在buffer中的产品，用于检测新产品
        known_products = set(self.products_in_qc_buffer.get(line_id, []))
        
        for product_id in products_in_output:
            if product_id not in known_products:
                self.products_in_qc_buffer[line_id].append(product_id)
                self.add_task(line_id, product_id, "QualityCheck_output", "Warehouse")
        
        # 清理已经不在buffer中的产品记录
        self.products_in_qc_buffer[line_id] = [p for p in self.products_in_qc_buffer[line_id] if p in products_in_output]


    def handle_agv_status(self, line_id: str, agv_id: str, payload: Dict[str, Any]):
        """更新AGV状态并触发任务调度。"""
        full_agv_id = f"{line_id}_{agv_id}"
        old_status = self.agv_states.get(full_agv_id, {}).get("status")
        new_status = payload.get("status")
        
        self.agv_states[full_agv_id] = {
            "status": new_status,
            "current_point": payload.get("current_point"),
            "payload": payload.get("payload", []),
            "line_id": line_id,
            "agv_id": agv_id
        }
        
        # 调试日志: 打印所有收到的AGV状态
        logger.debug(f"AGV状态更新: {full_agv_id} - {self.agv_states[full_agv_id]}")

        # 当AGV完成任务变为空闲时，触发调度
        if old_status != "idle" and new_status == "idle":
            logger.info(f"AGV {full_agv_id} 变为空闲，位于 {payload.get('current_point')}. 检查任务流程。")
            if full_agv_id in self.agv_jobs:
                self.execute_next_step(full_agv_id)
            else:
                self.schedule_tasks()
        # 第一次收到AGV状态时也触发一次调度
        elif old_status is None and new_status == "idle":
             logger.info(f"首次接收到AGV {full_agv_id} 状态为空闲。触发任务调度。")
             self.schedule_tasks()


    def handle_command_response(self, payload: Dict[str, Any]):
        """处理来自仿真环境的指令反馈。"""
        logger.debug(f"收到指令反馈: {payload.get('response')} (ID: {payload.get('command_id')})")

    def add_task(self, line_id: str, product_id: Optional[str], from_loc: str, to_loc: str):
        """向队列中添加一个新任务。"""
        if product_id and product_id in self.tasks_created_for_product:
            logger.debug(f"产品 {product_id} 的任务已存在，跳过。")
            return

        task = (line_id, product_id, from_loc, to_loc)
        self.task_queue.append(task)
        if product_id:
            self.tasks_created_for_product.add(product_id)
        logger.info(f"【新任务】已添加任务到队列: 从 {from_loc} 到 {to_loc}，产品: {product_id or 'ANY'}, 产线: {line_id}")
        self.schedule_tasks()

    def schedule_tasks(self):
        """寻找空闲的AGV并分配任务。"""
        if not self.task_queue:
            logger.debug("任务队列为空，无需调度。")
            return

        # 寻找与任务产线匹配的空闲AGV
        task_to_do = self.task_queue[0] # 先查看第一个任务
        target_line_id = task_to_do[0]

        idle_agvs_for_line = [
            agv_id for agv_id, state in self.agv_states.items() 
            if state['status'] == 'idle' and state['line_id'] == target_line_id and agv_id not in self.agv_jobs
        ]

        if not idle_agvs_for_line:
            logger.info(f"产线 {target_line_id} 没有空闲的AGV来执行任务。等待中...")
            return
            
        # 分配任务
        agv_to_use = idle_agvs_for_line[0]
        task_to_do = self.task_queue.popleft()
        
        line_id, product_id, from_loc, to_loc = task_to_do
        
        self.agv_jobs[agv_to_use] = {
            "task": task_to_do,
            "step": "start"
        }
        
        logger.info(f"【任务分配】将任务 '{product_id or 'ANY'}: {from_loc} -> {to_loc}' 分配给AGV {agv_to_use}")
        self.execute_next_step(agv_to_use)

    def execute_next_step(self, full_agv_id: str):
        """根据AGV当前任务状态，执行下一步操作。"""
        job = self.agv_jobs.get(full_agv_id)
        if not job:
            logger.warning(f"AGV {full_agv_id} 没有进行中的任务，重新调度。")
            self.schedule_tasks()
            return
            
        agv_state = self.agv_states[full_agv_id]
        line_id, product_id, from_loc, to_loc = job["task"]
        step = job["step"]

        pickup_point = LOCATION_MAPPING[from_loc]
        dropoff_point = LOCATION_MAPPING[to_loc]
        
        # 任务流程状态机
        if step == "start":
            logger.info(f"  [步骤 1/4] AGV {full_agv_id}: 前往取货点 {pickup_point} ({from_loc})")
            self.agv_jobs[full_agv_id]["step"] = "moving_to_pickup"
            self.send_move_command(line_id, agv_state["agv_id"], pickup_point)

        elif step == "moving_to_pickup" and agv_state["current_point"] == pickup_point:
            logger.info(f"  [步骤 2/4] AGV {full_agv_id}: 到达取货点，开始装载")
            self.agv_jobs[full_agv_id]["step"] = "loading"
            self.send_load_command(line_id, agv_state["agv_id"], product_id if from_loc == "RawMaterial" else None)

        elif step == "loading" and agv_state["payload"]:
            logger.info(f"  [步骤 3/4] AGV {full_agv_id}: 装载成功，前往卸货点 {dropoff_point} ({to_loc})")
            self.agv_jobs[full_agv_id]["step"] = "moving_to_dropoff"
            self.send_move_command(line_id, agv_state["agv_id"], dropoff_point)

        elif step == "moving_to_dropoff" and agv_state["current_point"] == dropoff_point:
            logger.info(f"  [步骤 4/4] AGV {full_agv_id}: 到达卸货点，开始卸载")
            self.agv_jobs[full_agv_id]["step"] = "unloading"
            self.send_unload_command(line_id, agv_state["agv_id"])

        elif step == "unloading" and not agv_state["payload"]:
            logger.info(f"【任务完成】AGV {full_agv_id} 完成了任务: '{product_id or 'ANY'}: {from_loc} -> {to_loc}'")
            if product_id and product_id in self.tasks_created_for_product:
                 self.tasks_created_for_product.remove(product_id) 
            del self.agv_jobs[full_agv_id]
            self.schedule_tasks()
        else:
             logger.debug(f"AGV {full_agv_id} 正在等待步骤 '{step}' 完成。当前状态: {agv_state['status']}, 位置: {agv_state['current_point']}")

    def _send_command(self, line_id: str, command: Dict[str, Any]):
        """发送指令到指定产线的指令话题。"""
        topic = f"{self.topic_root}/command/{line_id}"
        command_id = f"{command['action']}_{command['target']}_{int(time.time() * 1000)}"
        command_with_id = {"command_id": command_id, **command}
        payload = json.dumps(command_with_id)
        logger.info(f"发布指令到 '{topic}': {payload}")
        self.client.publish(topic, payload, qos=1)

    def send_move_command(self, line_id: str, agv_id: str, target_point: str):
        """发送移动指令。"""
        command = {
            "action": "move",
            "target": agv_id,
            "params": {"target_point": target_point}
        }
        self._send_command(line_id, command)

    def send_load_command(self, line_id: str, agv_id: str, product_id: Optional[str]):
        """发送装载指令。"""
        params = {}
        if product_id:
            params["product_id"] = product_id

        command = {
            "action": "load",
            "target": agv_id,
            "params": params
        }
        self._send_command(line_id, command)

    def send_unload_command(self, line_id: str, agv_id: str):
        """发送卸载指令。"""
        command = {
            "action": "unload",
            "target": agv_id,
            "params": {}
        }
        self._send_command(line_id, command)

    def run_forever(self):
        """保持脚本运行并处理MQTT消息。"""
        logger.info("任务调度Agent已启动，正在监听MQTT消息...")
        try:
            self.client.loop_forever()
        except KeyboardInterrupt:
            logger.info("Agent被手动中断，正在关闭...")
        finally:
            self.client.disconnect()
            logger.info("MQTT已断开连接。")


if __name__ == "__main__":
    scheduler = TaskScheduler(MQTT_BROKER_HOST, MQTT_BROKER_PORT, TOPIC_ROOT)
    scheduler.run_forever()