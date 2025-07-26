import os
import paho.mqtt.client as mqtt
import json
import logging
import time
import csv
import uuid
import threading
from datetime import datetime
from collections import deque, defaultdict
from typing import Dict, Any, Optional, List, Tuple
from openai import OpenAI

# --- 全局配置 (Global Configuration) ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("MultiAgentScheduler")

# --- MQTT 和工厂常量 ---
MQTT_BROKER_HOST = "supos-ce-instance4.supos.app"
MQTT_BROKER_PORT = 1883
TOPIC_ROOT = "marks"
FACTORY_LINES = ["line1", "line2", "line3"]
AGV_IDS_PER_LINE = ["AGV_1", "AGV_2"]
MOONSHOT_API_KEY = "sk-WJOuxjufh0vXgnanIToEMHZQxzppEuuTu4fLzWRYwmtnvvWZ"


# --- 智能体行为常量 ---
LOW_BATTERY_THRESHOLD = 35.0
TARGET_CHARGE_LEVEL = 90.0
BIDDING_WINDOW_SECONDS = 2.0 # 投标窗口时间

# --- [新增] LLM 辅助模块 ---
class LLMHelper:
    """封装对 Kimi LLM API 的调用"""
    def __init__(self):
        logger.info(f"KIMI API KEY: {MOONSHOT_API_KEY}")
        try:
            self.client = OpenAI(
                api_key=MOONSHOT_API_KEY,
                base_url="https://api.moonshot.cn/v1",
            )
            logger.info("Kimi LLM 客户端初始化成功。")
        except Exception as e:
            self.client = None
            logger.error(f"Kimi LLM 客户端初始化失败: {e}. LLM功能将不可用。")

    def ask_kimi(self, prompt: str, system_prompt: str) -> str:
        if not self.client:
            return "ERROR_LLM_UNAVAILABLE"
        try:
            completion = self.client.chat.completions.create(
                model="kimi-k2-0711-preview",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
            )
            response = completion.choices[0].message.content
            # 发布LLM日志
            log_topic = f"{TOPIC_ROOT}/agents/llm_logs"
            log_payload = json.dumps({"prompt": prompt, "response": response})
            # (这里假设有一个全局的mqtt_client实例可以用来发布)
            if 'mqtt_client' in globals():
                mqtt_client.publish(log_topic, log_payload)
            return response
        except Exception as e:
            logger.error(f"Kimi API 调用失败: {e}")
            return "ERROR_LLM_CALL"

# --- 基础 Agent 类 ---
class BaseAgent:
    """所有智能体的基类，处理通用功能如MQTT连接和状态发布。"""
    def __init__(self, agent_id: str, agent_type: str, topic_root: str):
        self.agent_id = agent_id
        self.agent_type = agent_type
        self.topic_root = topic_root
        self.client_id = f"{topic_root}_{agent_id}_{int(time.time())}"
        
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = lambda c, u, rc, p: logger.warning(f"Agent {self.agent_id} disconnected with code {rc}")
        
        self.callbacks = {}

    def connect(self):
        logger.info(f"[{self.agent_id}] 正在连接到 MQTT Broker...")
        self.client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)

    def on_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            logger.info(f"[{self.agent_id}] 已成功连接到 MQTT Broker。")
            for topic, callback in self.callbacks.items():
                client.subscribe(topic, qos=1)
                logger.info(f"[{self.agent_id}] 订阅主题: {topic}")
        else:
            logger.error(f"[{self.agent_id}] 连接失败，返回码: {rc}")

    def on_message(self, client, userdata, msg):
        try:
            for topic_filter, callback in self.callbacks.items():
                if mqtt.topic_matches_sub(topic_filter, msg.topic):
                    callback(msg.topic, json.loads(msg.payload.decode('utf-8')))
                    return
        except Exception as e:
            logger.error(f"[{self.agent_id}] 处理消息时出错 (topic: {msg.topic}): {e}", exc_info=True)

    def publish(self, topic: str, payload: dict):
        self.client.publish(topic, json.dumps(payload), qos=1)

    def subscribe(self, topic: str, callback):
        self.callbacks[topic] = callback
        if self.client.is_connected():
            self.client.subscribe(topic, qos=1)
            logger.info(f"[{self.agent_id}] 订阅主题: {topic}")

    def run(self):
        self.connect()
        self.client.loop_forever()

# --- 资源 Agent ---
class ResourceAgent(BaseAgent):
    """监控资源点（如仓库、质检站），并在需要时发布运输任务。"""
    def __init__(self, agent_id: str, line_id: str, device_name: str, topic_root: str):
        super().__init__(agent_id, "Resource", topic_root)
        self.line_id = line_id
        self.device_name = device_name
        self.published_tasks = set()
        
        # 订阅自己的状态
        status_topic_map = {
            "RawMaterial": f"{self.topic_root}/warehouse/RawMaterial/status",
            "QualityCheck": f"{self.topic_root}/{self.line_id}/station/QualityCheck/status",
            "Conveyor_CQ": f"{self.topic_root}/{self.line_id}/conveyor/Conveyor_CQ/status"
        }
        self.subscribe(status_topic_map[self.device_name], self.handle_status_update)

    def handle_status_update(self, topic: str, payload: dict):
        if self.device_name == "RawMaterial":
            products = payload.get("buffer", [])
            for prod_id in products:
                if prod_id not in self.published_tasks:
                    self._create_and_publish_task("feeder", [prod_id], "RawMaterial", "StationA")
                    self.published_tasks.add(prod_id)
        elif self.device_name == "QualityCheck":
            products = payload.get("output_buffer", [])
            for prod_id in products:
                if prod_id not in self.published_tasks:
                    self._create_and_publish_task("finisher", [prod_id], "QualityCheck_output", "Warehouse")
                    self.published_tasks.add(prod_id)
        elif self.device_name == "Conveyor_CQ":
            rework_products = payload.get("upper_buffer", []) + payload.get("lower_buffer", [])
            for prod_id in rework_products:
                if prod_id not in self.published_tasks:
                    self._create_and_publish_task("rework", [prod_id], "Conveyor_CQ", "StationB")
                    self.published_tasks.add(prod_id)
    
    def _create_and_publish_task(self, task_type: str, products: List[str], from_loc: str, to_loc: str):
        task_id = f"task_{self.device_name.lower()}_{uuid.uuid4().hex[:8]}"
        task_announcement = {
            "task_id": task_id,
            "source_agent": self.agent_id,
            "line_id": self.line_id,
            "task_type": task_type,
            "products": products,
            "from_loc": from_loc,
            "to_loc": to_loc,
            "priority": "high" if task_type == "finisher" else "normal",
            "creation_time": time.time()
        }
        logger.info(f"【任务发布】[{self.agent_id}] 发布新任务: {task_id} (类型: {task_type})")
        self.publish(f"{self.topic_root}/tasks/new", task_announcement)

# --- 协调 Agent ---
class CoordinatorAgent(BaseAgent):
    """任务看板和拍卖师，负责任务的分配。"""
    def __init__(self, topic_root: str):
        super().__init__("coordinator", "Coordinator", topic_root)
        self.open_tasks = {}
        self.bids = defaultdict(list)
        self.lock = threading.Lock()
        
        self.subscribe(f"{self.topic_root}/tasks/new", self.handle_new_task)
        self.subscribe(f"{self.topic_root}/tasks/bids", self.handle_new_bid)

    def handle_new_task(self, topic: str, payload: dict):
        with self.lock:
            task_id = payload["task_id"]
            if task_id not in self.open_tasks:
                payload["bidding_deadline"] = time.time() + BIDDING_WINDOW_SECONDS
                self.open_tasks[task_id] = payload
                logger.info(f"【任务看板】收到新任务 {task_id}，开始招标。")

    def handle_new_bid(self, topic: str, payload: dict):
        with self.lock:
            task_id = payload["task_id"]
            if task_id in self.open_tasks:
                self.bids[task_id].append(payload)
                logger.info(f"【任务看板】收到 AGV {payload['agv_id']} 对任务 {task_id} 的投标，报价: {payload['bid_score']:.2f}")

    def auction_tasks(self):
        while True:
            time.sleep(0.5)
            with self.lock:
                tasks_to_assign = []
                for task_id, task in self.open_tasks.items():
                    if time.time() > task["bidding_deadline"]:
                        tasks_to_assign.append(task_id)

                for task_id in tasks_to_assign:
                    if task_id in self.bids and self.bids[task_id]:
                        best_bid = min(self.bids[task_id], key=lambda x: x['bid_score'])
                        assignment = {
                            "task_id": task_id,
                            "assigned_agv_id": best_bid['agv_id'],
                            "assignment_time": time.time(),
                            **self.open_tasks[task_id] # Include original task details
                        }
                        self.publish(f"{self.topic_root}/tasks/assignments", assignment)
                        logger.info(f"【任务分配】任务 {task_id} 分配给 AGV {best_bid['agv_id']} (最优报价: {best_bid['bid_score']:.2f})")
                    else:
                        # 重新招标
                        self.open_tasks[task_id]["bidding_deadline"] = time.time() + BIDDING_WINDOW_SECONDS
                        logger.info(f"【任务看板】任务 {task_id} 无人投标，重新招标。")

                    # 清理已处理的任务
                    if task_id in self.open_tasks: del self.open_tasks[task_id]
                    if task_id in self.bids: del self.bids[task_id]

    def run(self):
        auction_thread = threading.Thread(target=self.auction_tasks, daemon=True)
        auction_thread.start()
        super().run()

# --- 执行 Agent (AGV) ---
class AGVAgent(BaseAgent):
    """代表单个AGV，负责投标、执行任务和自我管理。"""
    def __init__(self, line_id: str, agv_id_suffix: str, topic_root: str, llm: LLMHelper):
        agent_id = f"{line_id}_{agv_id_suffix}"
        super().__init__(agent_id, "AGV", topic_root)
        self.llm = llm
        self.state = "initializing" # initializing, idle, bidding, working, charging
        self.current_task = None
        self.agv_sim_state = {}
        self.bidding_task_id = None
        self.bidding_timeout = 0

        self.subscribe(f"{self.topic_root}/{line_id}/agv/{agv_id_suffix}/status", self.handle_status_update)
        self.subscribe(f"{self.topic_root}/tasks/assignments", self.handle_assignment)
        self.subscribe(f"{self.topic_root}/tasks/new", self.handle_new_task_announcement)

    def handle_status_update(self, topic: str, payload: dict):
        self.agv_sim_state = payload
        # 如果正在充电，检查是否完成
        if self.state == "charging" and payload.get("battery_level", 0) >= TARGET_CHARGE_LEVEL:
            logger.info(f"🔋[{self.agent_id}] 充电完成。")
            self.state = "idle"

    def handle_new_task_announcement(self, topic: str, payload: dict):
        if self.state == "idle":
            task_id = payload['task_id']
            # LLM 辅助决策是否投标
            decision = self.llm_decide_to_bid(payload)
            if "YES" in decision:
                # 简单本地计算投标分数（可替换为更复杂的LLM调用）
                bid_score = self.calculate_bid_score(payload)
                if bid_score != float('inf'):
                    bid = {"task_id": task_id, "agv_id": self.agent_id, "bid_score": bid_score}
                    self.publish(f"{self.topic_root}/tasks/bids", bid)
                    self.state = "bidding"
                    self.bidding_task_id = task_id
                    self.bidding_timeout = time.time() + 5 # 5秒竞标超时
                    logger.info(f"[{self.agent_id}] 对任务 {task_id} 投标，进入 BIDDING 状态。")
            else:
                logger.info(f"[{self.agent_id}] LLM 决策不对任务 {task_id} 投标: {decision}")

    def handle_assignment(self, topic: str, payload: dict):
        if self.state == "bidding" and payload["assigned_agv_id"] == self.agent_id:
            logger.info(f"🎉 [{self.agent_id}] 赢得任务 {payload['task_id']}！")
            self.state = "working"
            self.current_task = payload
            self.bidding_task_id = None
            self.execute_task_step()

    def run(self):
        # 启动MQTT循环
        mqtt_thread = threading.Thread(target=super().run, daemon=True)
        mqtt_thread.start()
        
        # Agent主逻辑循环
        while True:
            if self.state == "idle":
                # 主动充电决策
                if self.agv_sim_state.get("battery_level", 100) < LOW_BATTERY_THRESHOLD:
                    logger.info(f"🔋[{self.agent_id}] 电量低，主动进入充电状态。")
                    self.state = "charging"
                    self.send_charge_command()
            
            elif self.state == "bidding":
                if time.time() > self.bidding_timeout:
                    logger.warning(f"[{self.agent_id}] 投标任务 {self.bidding_task_id} 超时，返回 IDLE 状态。")
                    self.state = "idle"
                    self.bidding_task_id = None

            time.sleep(1)
            self.publish_status()

    def publish_status(self):
        status_payload = {
            "agent_id": self.agent_id,
            "agent_type": self.agent_type,
            "timestamp": time.time(),
            "status": self.state,
            "data": self.agv_sim_state
        }
        self.publish(f"{self.topic_root}/agents/{self.agent_id}/status", status_payload)

    def calculate_bid_score(self, task: dict) -> float:
        # 简化版：仅基于电量否决，未来可加入路径计算
        # 注意: 真实的路径时间需要查询 path_timing.py 或通过API获取
        if self.agv_sim_state.get("battery_level", 0) < 20.0:
            return float('inf') # 电量过低，不投标
        return random.uniform(10, 100) # 返回一个随机分数模拟

    def llm_decide_to_bid(self, task: dict) -> str:
        system_prompt = (
            "You are an AGV's decision-making AI. Based on the AGV's state and a new task, "
            "decide if it's a good idea to bid for it. "
            "Consider battery level and task type. Rework and Finisher tasks are high priority. "
            "Respond ONLY with 'YES' or 'NO' followed by a brief reason (e.g., 'NO, battery too low')."
        )
        prompt = (
            f"AGV State:\n- ID: {self.agent_id}\n- Battery: {self.agv_sim_state.get('battery_level', 100):.1f}%\n\n"
            f"New Task:\n- Type: {task['task_type']}\n- From: {task['from_loc']} To: {task['to_loc']}\n\n"
            "Should I bid for this task?"
        )
        return self.llm.ask_kimi(prompt, system_prompt)

    def execute_task_step(self):
        """执行当前任务的逻辑状态机。"""
        if not self.current_task: return
        
        # 这是一个简化的实现，直接按顺序发送指令。
        # 完整的实现需要监听 response 和 status 来确认每一步是否成功。
        line_id, agv_id_suffix = self.agent_id.split('_', 1)
        
        logger.info(f"[{self.agent_id}] 开始执行任务 {self.current_task['task_id']}")
        # 1. 前往取货点
        pickup_point = LOCATION_MAPPING.get(self.current_task['from_loc'])
        self.send_move_command(line_id, agv_id_suffix, pickup_point)
        # ... (此处省略了等待每一步完成的复杂逻辑，实际项目中需要实现)
        
        # 假设任务瞬间完成 (仅为演示)
        logger.info(f"[{self.agent_id}] 任务 {self.current_task['task_id']} 已完成。")
        self.state = "idle"
        self.current_task = None


    def send_move_command(self, line_id: str, agv_id: str, target_point: str):
        self._send_command(line_id, {"action": "move", "target": agv_id, "params": {"target_point": target_point}})
    
    def send_charge_command(self):
        line_id, agv_id_suffix = self.agent_id.split('_', 1)
        self._send_command(line_id, {"action": "charge", "target": agv_id_suffix, "params": {"target_level": TARGET_CHARGE_LEVEL}})

    def _send_command(self, line_id: str, command: Dict[str, Any]):
        topic = f"{self.topic_root}/command/{line_id}"
        command_id = f"{command['action']}_{command.get('target', 'N/A')}_{int(time.time() * 1000)}"
        payload = json.dumps({"command_id": command_id, **command})
        logger.debug(f"[{self.agent_id}] 发送指令到 '{topic}': {payload}")
        self.client.publish(topic, payload, qos=1)


# --- 主程序入口 ---
if __name__ == "__main__":
    logger.info("启动多智能体调度系统...")
    
    # 0. 初始化LLM助手
    llm_helper = LLMHelper()
    
    # 1. 实例化所有Agent
    agents = []
    
    # 1.1 协调Agent
    coordinator = CoordinatorAgent(TOPIC_ROOT)
    agents.append(coordinator)
    
    # 1.2 资源Agent 和 执行Agent (AGV)
    resource_agents_config = [
        {"agent_id": "global_RawMaterial", "line_id": "global", "device_name": "RawMaterial"},
    ]
    for line in FACTORY_LINES:
        resource_agents_config.append({"agent_id": f"{line}_QualityCheck", "line_id": line, "device_name": "QualityCheck"})
        resource_agents_config.append({"agent_id": f"{line}_Conveyor_CQ", "line_id": line, "device_name": "Conveyor_CQ"})
        for agv_id in AGV_IDS_PER_LINE:
            agents.append(AGVAgent(line, agv_id, TOPIC_ROOT, llm_helper))

    for config in resource_agents_config:
        agents.append(ResourceAgent(**config, topic_root=TOPIC_ROOT))

    # 2. 在独立线程中启动每个Agent
    threads = []
    for agent in agents:
        thread = threading.Thread(target=agent.run, daemon=True)
        threads.append(thread)
        thread.start()
        time.sleep(0.1) # 错开连接时间

    logger.info(f"已成功启动 {len(agents)} 个智能体。系统运行中... (按 Ctrl+C 停止)")

    # 3. 保持主线程运行
    try:
        while True:
            time.sleep(60) # 主线程可以定期执行一些监控任务
            logger.info("系统心跳：所有智能体正在运行。")
    except KeyboardInterrupt:
        logger.info("收到关闭信号，正在停止所有智能体...")
        for agent in agents:
            agent.client.disconnect()
            agent.client.loop_stop()
        logger.info("所有智能体已安全关闭。")