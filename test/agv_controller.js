const mqtt = require('mqtt');
    
const options = {
  clean: true, // 设置为 false 以便离线时接收消息，如果服务器支持
  connectTimeout: 4000, 
  clientId: 'agv_controller_client_' + Math.random().toString(16).substr(2, 8), // 使用一个唯一的客户端ID
  rejectUnauthorized: false, // 如果使用wss且证书有问题，可以设置为true
};

// 确认连接URL和端口与您的MQTTBroker.json中的"ws"或"wss"配置一致
// 如果您在Unity端使用 "wss": true，那么这里也应该使用 'wss://' 和 8084 端口
// 对于 Python 模拟器，通常是 1883 (tcp) 或 8083 (ws)
const connectUrl ='ws://supos-ce-instance4.supos.app:8083/mqtt';

const client = mqtt.connect(connectUrl, options);

// --- 重要的配置：与模拟器保持一致的 TOPIC_ROOT ---
const ROOT_TOPIC_HEAD = 'marks'; // <-- 请根据您的模拟器实际运行时的配置进行修改

client.on('connect', function () {
  console.log('Connected to MQTT broker.');

  // 1. 订阅 AGV 的状态主题，以便观察移动结果
  const agvStatusTopic = `${ROOT_TOPIC_HEAD}/line1/agv/AGV_1/status`;
  client.subscribe(agvStatusTopic, function (err) {
    if (!err) {
      console.log(`Subscribed to AGV_1 status: ${agvStatusTopic}`);
      // 可以在这里发送第一个命令，确保订阅成功后才操作
      // moveAGV('AGV_1', 'P1', 'line1'); // 初始命令，移动到 P1
    } else {
      console.error('Subscription error:', err);
    }
  });

  // 2. 订阅系统响应主题，查看命令执行结果
  const systemResponseTopic = `${ROOT_TOPIC_HEAD}/response/line1`; // 替换为您的生产线ID
  client.subscribe(systemResponseTopic, function (err) {
    if (!err) {
      console.log(`Subscribed to system responses: ${systemResponseTopic}`);
    } else {
      console.error('System response subscription error:', err);
    }
  });

  // 3. 延迟发送命令，确保所有订阅都已完成
  setTimeout(() => {
    // 示例：移动 AGV_1 到 P8
    moveAGV('AGV_1', 'P8', 'line1'); 
  }, 2000); // 延迟2秒发送命令，给订阅留出时间
});

client.on('message', function (topic, message) {
  // 打印所有接收到的消息
  console.log(`Received message on topic: ${topic}`);
  try {
    const msgPayload = JSON.parse(message.toString());
    console.log('  Payload:', JSON.stringify(msgPayload, null, 2));
    
    // 如果是 AGV 状态更新，可以进一步解析
    if (topic.includes('/agv/') && topic.endsWith('/status')) {
      console.log(`  AGV current point: ${msgPayload.current_point}, status: ${msgPayload.status}`);
      // 您可以根据 AGV 的状态和位置，在这里编写后续的命令逻辑
      // 例如，如果 AGV 到达 P8，就发送装载命令
      if (msgPayload.current_point === 'P8' && msgPayload.status === 'idle') {
        console.log('AGV_1 reached P8 and is idle. Sending load command...');
        loadAGV('AGV_1', 'line1');
      } else if (msgPayload.current_point === 'P9' && msgPayload.status === 'idle') {
        console.log('AGV_1 reached P9 and is idle. Sending unload command...');
        unloadAGV('AGV_1', 'line1');
      }
    }

  } catch (e) {
    console.error('  Error parsing message:', e);
    console.log('  Raw message:', message.toString());
  }
});

// --- 发送 AGV 移动命令的函数 ---
function moveAGV(agvId, targetPoint, lineId) {
  const commandTopic = `${ROOT_TOPIC_HEAD}/command/${lineId}`;
  const commandPayload = {
    command_id: `move_${agvId}_${targetPoint}_${Date.now()}`, // 唯一的命令ID
    action: 'move',
    target: agvId,
    params: {
      target_point: targetPoint
    }
  };
  client.publish(commandTopic, JSON.stringify(commandPayload));
  console.log(`Sent MOVE command for ${agvId} to ${targetPoint} on ${lineId}: ${JSON.stringify(commandPayload)}`);
}

// --- 发送 AGV 装载命令的函数 ---
function loadAGV(agvId, lineId) {
  const commandTopic = `${ROOT_TOPIC_HEAD}/command/${lineId}`;
  const commandPayload = {
    command_id: `load_${agvId}_${Date.now()}`,
    action: 'load',
    target: agvId, // 目标 AGV ID
    params: {} // 参数为空，模拟器会根据AGV当前位置自动判断从哪个设备装载
  };
  client.publish(commandTopic, JSON.stringify(commandPayload));
  console.log(`Sent LOAD command for ${agvId} on ${lineId}: ${JSON.stringify(commandPayload)}`);
}

// --- 发送 AGV 卸载命令的函数 ---
function unloadAGV(agvId, lineId) {
  const commandTopic = `${ROOT_TOPIC_HEAD}/command/${lineId}`;
  const commandPayload = {
    command_id: `unload_${agvId}_${Date.now()}`,
    action: 'unload',
    target: agvId, // 目标 AGV ID
    params: {} // 参数为空，模拟器会根据AGV当前位置自动判断卸载到哪个设备
  };
  client.publish(commandTopic, JSON.stringify(commandPayload));
  console.log(`Sent UNLOAD command for ${agvId} on ${lineId}: ${JSON.stringify(commandPayload)}`);
}