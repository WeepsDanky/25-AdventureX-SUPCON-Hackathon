import os
import paho.mqtt.client as mqtt
import json
import logging
import time
import uuid
import threading
from datetime import datetime
from collections import deque, defaultdict
from typing import Dict, Any, Optional, List, Tuple
from openai import OpenAI
import random
# [新增] 导入路径时间计算函数和csv模块
from config.path_timing import get_travel_time
import csv
# [新增] 导入 cycle 用于轮询
from itertools import cycle

# --- 全局配置 (Global Configuration) ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("MultiAgentScheduler")

# --- MQTT 和工厂常量 ---
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "supos-ce-instance4.supos.app")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
TOPIC_ROOT = os.getenv("TOPIC_ROOT") or os.getenv("USERNAME") or os.getenv("USER") or "marks"
FACTORY_LINES = ["line1", "line2", "line3"]
AGV_IDS_PER_LINE = ["AGV_1", "AGV_2"]
MOONSHOT_API_KEY = os.getenv("MOONSHOT_API_KEY")

# --- 智能体行为常量 ---
LOCATION_MAPPING = {
    "RawMaterial": "P0", "StationA": "P1", "StationB": "P3",
    "Conveyor_CQ": "P6", "Conveyor_CQ_upper": "P6", "Conveyor_CQ_lower": "P6",
    "QualityCheck_output": "P8", "Warehouse": "P9", "Charging": "P10"
}
LOW_BATTERY_THRESHOLD = 35.0
TARGET_CHARGE_LEVEL = 90.0
BIDDING_WINDOW_SECONDS = 2.0

# --- [已修复] LLM 辅助模块 (增加了健壮性) ---
class LLMHelper:
    def __init__(self):
        if not MOONSHOT_API_KEY:
            logger.warning("MOONSHOT_API_KEY 环境变量未设置。LLM功能将不可用。")
            self.client = None
            return
        
        try:
            self.client = OpenAI(api_key=MOONSHOT_API_KEY, base_url="https://api.moonshot.cn/v1")
            self.client.models.list() # Test connection
            logger.info("Kimi LLM 客户端初始化成功。")
        except Exception as e:
            self.client = None
            logger.error(f"Kimi LLM 客户端初始化失败: {e}. LLM功能将不可用。")

    def ask_kimi(self, prompt: str, system_prompt: str) -> str:
        if not self.client:
            return "ERROR_LLM_UNAVAILABLE"
        try:
            completion = self.client.chat.completions.create(
                model="moonshot-v1-8k",
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": prompt}],
                temperature=0.3,
            )
            return completion.choices[0].message.content
        except Exception as e:
            logger.error(f"Kimi API 调用失败: {e}")
            return "ERROR_LLM_CALL"

# --- [已修复] 基础 Agent 类 ---
class BaseAgent:
    def __init__(self, agent_id: str, agent_type: str, topic_root: str):
        self.agent_id = agent_id
        self.agent_type = agent_type
        self.topic_root = topic_root
        self.client_id = f"{topic_root}_{agent_id.replace('_','-')}_{int(time.time())}"
        
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = lambda c, u, f, rc, p: logger.warning(f"Agent {self.agent_id} disconnected with code {rc}")
        
        self.callbacks = {}

    def connect(self):
        logger.info(f"[{self.agent_id}] 正在连接到 MQTT Broker...")
        self.client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)

    def on_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            logger.info(f"[{self.agent_id}] 已成功连接到 MQTT Broker。")
            for topic, callback in self.callbacks.items():
                client.subscribe(topic, qos=1)
                logger.debug(f"[{self.agent_id}] 订阅主题: {topic}")
        else:
            logger.error(f"[{self.agent_id}] 连接失败，返回码: {rc}")

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode('utf-8'))
            for topic_filter, callback in self.callbacks.items():
                if mqtt.topic_matches_sub(topic_filter, msg.topic):
                    callback(msg.topic, payload)
                    return
        except Exception as e:
            logger.error(f"[{self.agent_id}] 处理消息时出错 (topic: {msg.topic}): {e}", exc_info=True)

    def publish(self, topic: str, payload: dict):
        self.client.publish(topic, json.dumps(payload), qos=1)

    def subscribe(self, topic: str, callback):
        self.callbacks[topic] = callback
        if self.client.is_connected():
            self.client.subscribe(topic, qos=1)
            logger.debug(f"[{self.agent_id}] 订阅主题: {topic}")

    def run(self):
        self.connect()
        self.client.loop_forever()

# --- 资源 Agent (修改了任务分派逻辑) ---
class ResourceAgent(BaseAgent):
    def __init__(self, agent_id: str, line_id: str, device_name: str, topic_root: str):
        super().__init__(agent_id, "Resource", topic_root)
        self.line_id = line_id
        self.device_name = device_name
        self.published_tasks = set()
        # [修改] 为 RawMaterial Agent 添加产线轮询器
        if self.device_name == "RawMaterial":
            self.line_cycler = cycle(FACTORY_LINES)
        
        topic_map = {
            "RawMaterial": f"{self.topic_root}/warehouse/RawMaterial/status",
            "QualityCheck": f"{self.topic_root}/{self.line_id}/station/QualityCheck/status",
            "Conveyor_CQ": f"{self.topic_root}/{self.line_id}/conveyor/Conveyor_CQ/status"
        }
        self.subscribe(topic_map[self.device_name], self.handle_status_update)

    def handle_status_update(self, topic: str, payload: dict):
        products = []
        if self.device_name == "RawMaterial":
            products = [p for p in payload.get("buffer", []) if p not in self.published_tasks]
            if products:
                # [修改] 使用轮询策略为每个产品分配产线
                target_line = next(self.line_cycler)
                self._create_and_publish_task("feeder", products, "RawMaterial", "StationA", target_line)
        elif self.device_name == "QualityCheck":
            products = [p for p in payload.get("output_buffer", []) if p not in self.published_tasks]
            if products: self._create_and_publish_task("finisher", products, "QualityCheck_output", "Warehouse", self.line_id)
        elif self.device_name == "Conveyor_CQ":
            products = [p for p in payload.get("upper_buffer", []) + payload.get("lower_buffer", []) if p not in self.published_tasks]
            if products: self._create_and_publish_task("rework", products, "Conveyor_CQ", "StationB", self.line_id)
        
        for prod_id in products: self.published_tasks.add(prod_id)

    # [修改] 增加 target_line 参数，并修改发布主题
    def _create_and_publish_task(self, task_type: str, products: List[str], from_loc: str, to_loc: str, target_line: str):
        task_id = f"task_{self.device_name.lower()}_{uuid.uuid4().hex[:8]}"
        task = {
            "task_id": task_id, "source_agent": self.agent_id, "line_id": target_line, # [修改] line_id 现在是具体产线
            "task_type": task_type, "products": products, "from_loc": from_loc,
            "to_loc": to_loc, "priority": "high" if task_type == "finisher" else "normal",
            "creation_time": time.time()
        }
        # [修改] 发布到特定产线的主题
        publish_topic = f"{self.topic_root}/{target_line}/tasks/new"
        logger.info(f"【任务分派】[{self.agent_id}] 向产线 {target_line} 发布新任务: {task_id} (产品: {products})")
        self.publish(publish_topic, task)

# --- 协调 Agent (现在订阅特定产线的任务) ---
class CoordinatorAgent(BaseAgent):
    def __init__(self, topic_root: str):
        super().__init__("coordinator", "Coordinator", topic_root)
        self.open_tasks = {}
        self.bids = defaultdict(list)
        self.lock = threading.Lock()
        self.latest_kpi_data: Optional[Dict[str, Any]] = None
        
        # [修改] 订阅所有产线的新任务主题
        self.subscribe(f"{self.topic_root}/+/tasks/new", self.handle_new_task)
        self.subscribe(f"{self.topic_root}/tasks/bids", self.handle_new_bid)
        self.subscribe(f"{self.topic_root}/kpi/status", self.handle_kpi_update)

    def handle_kpi_update(self, topic: str, payload: dict):
        self.latest_kpi_data = payload
        logger.debug(f"[{self.agent_id}] 收到KPI更新数据。")

    def handle_new_task(self, topic: str, payload: dict):
        with self.lock:
            task_id = payload["task_id"]
            if task_id not in self.open_tasks:
                payload["bidding_deadline"] = time.time() + BIDDING_WINDOW_SECONDS
                self.open_tasks[task_id] = payload
                logger.info(f"【任务看板】收到新任务 {task_id} (产线: {payload['line_id']})，开始招标。")

    def handle_new_bid(self, topic: str, payload: dict):
        with self.lock:
            task_id = payload["task_id"]
            if task_id in self.open_tasks:
                self.bids[task_id].append(payload)
                logger.info(f"【投标】[{payload['agv_id']}] 对任务 {task_id} 投标，报价: {payload['bid_score']:.2f}")

    def auction_tasks(self):
        while True:
            time.sleep(0.5)
            with self.lock:
                tasks_to_assign = [task_id for task_id, task in self.open_tasks.items() if time.time() > task["bidding_deadline"]]

                for task_id in tasks_to_assign:
                    if task_id in self.bids and self.bids[task_id]:
                        best_bid = min(self.bids[task_id], key=lambda x: x['bid_score'])
                        assignment = {"task_id": task_id, "assigned_agv_id": best_bid['agv_id'], **self.open_tasks[task_id]}
                        self.publish(f"{self.topic_root}/tasks/assignments", assignment)
                        logger.info(f"【任务分配】任务 {task_id} 分配给 AGV {best_bid['agv_id']} (最优报价: {best_bid['bid_score']:.2f})")
                    else:
                        self.open_tasks[task_id]["bidding_deadline"] = time.time() + BIDDING_WINDOW_SECONDS
                        logger.info(f"【任务看板】任务 {task_id} 无人投标，重新招标。")
                    
                    if task_id in self.open_tasks: del self.open_tasks[task_id]
                    if task_id in self.bids: del self.bids[task_id]

    def calculate_and_save_kpi(self):
        if not self.latest_kpi_data: 
            logger.warning("没有KPI数据，无法生成报告。")
            return
        kpis = self.latest_kpi_data
        logger.info("正在计算最终KPI得分并生成报告...")
        
        weights = {'production_efficiency': 0.4, 'quality_cost': 0.3, 'agv_efficiency': 0.3}
        efficiency_weights = {'order_completion': 0.4, 'production_cycle': 0.4, 'device_utilization': 0.2}
        quality_cost_weights = {'first_pass_rate': 0.4, 'cost_efficiency': 0.6}
        agv_weights = {'charge_strategy': 0.3, 'energy_efficiency': 0.4, 'utilization': 0.3}
        
        production_cycle_score = min(100, 100 / max(1, kpis.get('average_production_cycle', 1))) if kpis.get('total_products', 0) > 0 else 0
        baseline_cost_per_product, total_products, total_cost = 25, kpis.get('total_products', 0), kpis.get('total_production_cost', 0)
        cost_efficiency_score = min(100, (baseline_cost_per_product * total_products) / max(1, total_cost) * 100) if total_products > 0 else 0
        agv_energy_efficiency_score = min(100, kpis.get('agv_energy_efficiency', 0) * 1000)

        efficiency_score = (kpis.get('order_completion_rate', 0) * efficiency_weights['order_completion'] + production_cycle_score * efficiency_weights['production_cycle'] + kpis.get('device_utilization', 0) * efficiency_weights['device_utilization']) * weights['production_efficiency']
        quality_cost_score = (kpis.get('first_pass_rate', 0) * quality_cost_weights['first_pass_rate'] + cost_efficiency_score * quality_cost_weights['cost_efficiency']) * weights['quality_cost']
        agv_score = (kpis.get('charge_strategy_efficiency', 0) * agv_weights['charge_strategy'] + agv_energy_efficiency_score * agv_weights['energy_efficiency'] + kpis.get('agv_utilization', 0) * agv_weights['utilization']) * weights['agv_efficiency']
        total_score = efficiency_score + quality_cost_score + agv_score
        
        filename = f"kpi_results_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
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
            "报废成本": f"${kpis.get('scrap_costs', 0):.2f}"
        }
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(['KPI 指标', '值'])
                writer.writerows(report_data.items())
            logger.info(f"KPI报告已成功保存到文件: {filename}")
        except IOError as e:
            logger.error(f"无法写入KPI报告文件 {filename}: {e}")

    def run(self):
        threading.Thread(target=self.auction_tasks, daemon=True).start()
        super().run()

# --- [已修复] 执行 Agent (AGV) ---
class AGVAgent(BaseAgent):
    AGV_SPEED_MPS = 2.0
    AGV_BATTERY_CONSUMPTION_PER_METER = 0.1
    AGV_BATTERY_CONSUMPTION_PER_ACTION = 0.5
    BID_SCORE_TRAVEL_TIME_WEIGHT = 1.0
    BID_SCORE_ENERGY_WEIGHT = 0.5

    def __init__(self, line_id: str, agv_id_suffix: str, topic_root: str, llm: LLMHelper):
        agent_id = f"{line_id}_{agv_id_suffix}"
        super().__init__(agent_id, "AGV", topic_root)
        self.line_id = line_id # [新增] 保存自身产线ID
        self.llm = llm
        self.state = "initializing"
        self.task_step = None
        self.current_task = None
        self.agv_sim_state = {}
        self.bidding_timeout = 0
        
        self.subscribe(f"{self.topic_root}/{line_id}/agv/{agv_id_suffix}/status", self.handle_status_update)
        self.subscribe(f"{self.topic_root}/tasks/assignments", self.handle_assignment)
        # [修改] AGV只订阅自己产线的任务
        self.subscribe(f"{self.topic_root}/{self.line_id}/tasks/new", self.handle_new_task_announcement)

    def handle_status_update(self, topic: str, payload: dict):
        self.agv_sim_state = payload
        
        if self.state == "initializing" and payload.get("status") == "idle":
            self.state = "idle"
            logger.info(f"[{self.agent_id}] 初始化完成，进入 IDLE 状态。")
        
        if self.state == "working" and payload.get("status") == "idle":
            self.execute_task_step()
        
        if self.state == "charging" and payload.get("battery_level", 0) >= TARGET_CHARGE_LEVEL:
            logger.info(f"🔋[{self.agent_id}] 充电完成。")
            self.state = "idle"

    def handle_new_task_announcement(self, topic: str, payload: dict):
        if self.state == "idle":
            decision = self.llm_decide_to_bid(payload)
            should_bid = "YES" in decision or "ERROR_LLM" in decision

            if should_bid:
                bid_score = self.calculate_bid_score(payload)
                if bid_score != float('inf'):
                    self.state = "bidding"
                    self.bidding_timeout = time.time() + 5
                    self.publish(f"{self.topic_root}/tasks/bids", {"task_id": payload['task_id'], "agv_id": self.agent_id, "bid_score": bid_score})
                    logger.info(f"[{self.agent_id}] 对任务 {payload['task_id']} 投标，进入 BIDDING 状态。")
            else:
                logger.info(f"[{self.agent_id}] LLM 决策不对任务 {payload['task_id']} 投标: {decision}")

    def handle_assignment(self, topic: str, payload: dict):
        # 增加产线匹配检查，确保不会意外收到其他产线的分配
        if payload.get("line_id") != self.line_id:
            return
        if self.state == "bidding" and payload["assigned_agv_id"] == self.agent_id:
            logger.info(f"🎉 [{self.agent_id}] 赢得任务 {payload['task_id']}！")
            self.state = "working"
            self.current_task = payload
            self.task_step = "start"
            self.execute_task_step()

    def run(self):
        threading.Thread(target=super().run, daemon=True).start()
        while True:
            if self.state == "idle":
                if self.agv_sim_state.get("battery_level", 100) < LOW_BATTERY_THRESHOLD:
                    logger.info(f"🔋[{self.agent_id}] 电量低，主动进入充电状态。")
                    self.state = "charging"
                    self.send_charge_command()
            elif self.state == "bidding" and time.time() > self.bidding_timeout:
                logger.warning(f"[{self.agent_id}] 投标超时，返回 IDLE 状态。")
                self.state = "idle"
            
            time.sleep(1)
            self.publish_status()

    def publish_status(self):
        self.publish(f"{self.topic_root}/agents/{self.agent_id}/status", {
            "agent_id": self.agent_id, "agent_type": self.agent_type, "timestamp": time.time(),
            "status": self.state, "data": self.agv_sim_state
        })

    def calculate_bid_score(self, task: dict) -> float:
        current_point = self.agv_sim_state.get("current_point")
        battery_level = self.agv_sim_state.get("battery_level", 0)
        
        if not current_point or battery_level == 0:
            return float('inf')

        try:
            pickup_point = LOCATION_MAPPING[task['from_loc']]
            dropoff_point = LOCATION_MAPPING[task['to_loc']]
            charge_point = LOCATION_MAPPING['Charging']
        except KeyError:
            logger.error(f"[{self.agent_id}] 任务 {task['task_id']} 包含无效的位置信息。")
            return float('inf')

        time_to_pickup = get_travel_time(current_point, pickup_point)
        time_to_dropoff = get_travel_time(pickup_point, dropoff_point)
        
        if time_to_pickup < 0 or time_to_dropoff < 0:
            logger.warning(f"[{self.agent_id}] 无法计算任务 {task['task_id']} 的路径。({current_point}->{pickup_point}->{dropoff_point})")
            return float('inf')
            
        total_task_time = time_to_pickup + time_to_dropoff
        
        total_task_distance = total_task_time * self.AGV_SPEED_MPS
        estimated_consumption = (total_task_distance * self.AGV_BATTERY_CONSUMPTION_PER_METER) + (2 * self.AGV_BATTERY_CONSUMPTION_PER_ACTION)

        time_to_charge = get_travel_time(dropoff_point, charge_point)
        if time_to_charge < 0: time_to_charge = 30.0
        
        reserve_battery = (time_to_charge * self.AGV_SPEED_MPS * self.AGV_BATTERY_CONSUMPTION_PER_METER)
        
        if battery_level < estimated_consumption + reserve_battery + 5.0:
            logger.info(f"[{self.agent_id}] 因电量不足而放弃任务 {task['task_id']}。需要: {estimated_consumption + reserve_battery + 5.0:.1f}%, 现有: {battery_level:.1f}%")
            return float('inf')

        time_cost = total_task_time * self.BID_SCORE_TRAVEL_TIME_WEIGHT
        energy_cost = estimated_consumption * self.BID_SCORE_ENERGY_WEIGHT
        
        final_score = time_cost + energy_cost
        
        logger.debug(f"[{self.agent_id}] 为任务 {task['task_id']} 计算报价: 分数={final_score:.2f} (时间成本={time_cost:.2f}, 能源成本={energy_cost:.2f})")
        return final_score

    def llm_decide_to_bid(self, task: dict) -> str:
        # [修改] 简化System Prompt，因为任务已经是定向的
        system_prompt = (
            "You are a decision-making AI for an AGV. Your goal is to be efficient. "
            "You have been assigned a task on your production line. Decide if you should accept it. "
            "Key factors: proximity (how close you are) and capability (battery, status). "
            "Respond ONLY with 'YES' or 'NO' and a brief, concise reason."
        )

        prompt = (
            f"--- My Current State ---\n"
            f"Agent ID: {self.agent_id}\n"
            f"Status: {self.state}\n"
            f"Current Location: {self.agv_sim_state.get('current_point', 'Unknown')}\n"
            f"Battery Level: {self.agv_sim_state.get('battery_level', 0):.1f}%\n\n"
            f"--- New Task Details (for my line) ---\n"
            f"{json.dumps(task, indent=2)}\n\n"
            f"--- Decision ---\n"
            f"Should I bid on this task?"
        )
        
        return self.llm.ask_kimi(prompt, system_prompt)

    def execute_task_step(self):
        if not self.current_task: return

        line_id, agv_id_suffix = self.agent_id.split('_', 1)
        task = self.current_task
        agv_sim_state = self.agv_sim_state
        
        pickup_point = LOCATION_MAPPING.get(task['from_loc'])
        dropoff_point = LOCATION_MAPPING.get(task['to_loc'])

        if self.task_step == "start":
            self.task_step = "moving_to_pickup"
            logger.info(f"  [步骤 1] AGV {self.agent_id}: 前往取货点 {pickup_point} ({task['from_loc']})")
            self.send_move_command(line_id, agv_id_suffix, pickup_point)

        elif self.task_step == "moving_to_pickup" and agv_sim_state["current_point"] == pickup_point:
            self.task_step = "loading"
            logger.info(f"  [步骤 2] AGV {self.agent_id}: 到达取货点，开始装载")
            self.send_load_command(line_id, agv_id_suffix, task['products'][0] if task['from_loc'] == "RawMaterial" else None)

        elif self.task_step == "loading" and len(agv_sim_state["payload"]) > 0:
            self.task_step = "moving_to_dropoff"
            logger.info(f"  [步骤 3] AGV {self.agent_id}: 装载完成，前往卸货点 {dropoff_point} ({task['to_loc']})")
            self.send_move_command(line_id, agv_id_suffix, dropoff_point)

        elif self.task_step == "moving_to_dropoff" and agv_sim_state["current_point"] == dropoff_point:
            self.task_step = "unloading"
            logger.info(f"  [步骤 4] AGV {self.agent_id}: 到达卸货点，开始卸载")
            self.send_unload_command(line_id, agv_id_suffix)

        elif self.task_step == "unloading" and len(agv_sim_state["payload"]) == 0:
            logger.info(f"【任务完成】AGV {self.agent_id} 完成了任务 {task['task_id']}。")
            self.publish(f"{self.topic_root}/tasks/updates", {"task_id": task['task_id'], "agv_id": self.agent_id, "status": "completed"})
            self.state = "idle"
            self.current_task = None
            self.task_step = None

    def send_move_command(self, line_id: str, agv_id: str, target_point: str):
        self._send_command(line_id, {"action": "move", "target": agv_id, "params": {"target_point": target_point}})
    def send_load_command(self, line_id: str, agv_id: str, product_id: Optional[str]):
        self._send_command(line_id, {"action": "load", "target": agv_id, "params": {"product_id": product_id} if product_id else {}})
    def send_unload_command(self, line_id: str, agv_id: str):
        self._send_command(line_id, {"action": "unload", "target": agv_id, "params": {}})
    def send_charge_command(self):
        line_id, agv_id_suffix = self.agent_id.split('_', 1)
        self._send_command(line_id, {"action": "charge", "target": agv_id_suffix, "params": {"target_level": TARGET_CHARGE_LEVEL}})
    def _send_command(self, line_id: str, command: Dict[str, Any]):
        topic = f"{self.topic_root}/command/{line_id}"
        command_id = f"cmd_{self.agent_id}_{int(time.time() * 1000)}"
        self.publish(topic, {"command_id": command_id, **command})

# --- 主程序入口 ---
if __name__ == "__main__":
    logger.info("启动多智能体调度系统...")
    
    llm_helper = LLMHelper()
    agents = []
    
    coordinator = CoordinatorAgent(TOPIC_ROOT)
    agents.append(coordinator)
    
    # [修改] RawMaterial Agent 的 line_id 设置为 'global' 仅用于标识，实际任务会分派
    resource_agents_config = [{"agent_id": "global_RawMaterial", "line_id": "global", "device_name": "RawMaterial"}]
    for line in FACTORY_LINES:
        resource_agents_config.append({"agent_id": f"{line}_QualityCheck", "line_id": line, "device_name": "QualityCheck"})
        resource_agents_config.append({"agent_id": f"{line}_Conveyor_CQ", "line_id": line, "device_name": "Conveyor_CQ"})
        for agv_id in AGV_IDS_PER_LINE:
            agents.append(AGVAgent(line, agv_id, TOPIC_ROOT, llm_helper))
    
    for config in resource_agents_config:
        agents.append(ResourceAgent(**config, topic_root=TOPIC_ROOT))

    threads = [threading.Thread(target=agent.run, daemon=True) for agent in agents]
    for thread in threads:
        thread.start()
        time.sleep(0.1)

    logger.info(f"已成功启动 {len(agents)} 个智能体。系统运行中... (按 Ctrl+C 停止)")

    try:
        while True: time.sleep(60)
    except KeyboardInterrupt:
        logger.info("收到关闭信号，正在停止所有智能体...")
        logger.info("正在生成最终KPI报告...")
        coordinator.calculate_and_save_kpi()
        
        for agent in agents:
            if agent.client.is_connected():
                agent.client.disconnect()
        logger.info("所有智能体已安全关闭。")