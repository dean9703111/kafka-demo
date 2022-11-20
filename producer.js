const { Kafka, CompressionTypes, logLevel } = require('kafkajs')
const kafka = new Kafka({
  logLevel: logLevel.ERROR,
  brokers: [`localhost:9092`],
  clientId: 'example-producer',
})

const topic = 'topic-test'
const numPartitions = 2;
const crateTopic = async (topic) => {
  const admin = kafka.admin();
  await admin.connect();
  const topics = await admin.listTopics();
  if (!topics.includes(topic)) {
    await admin.createTopics({
      topics: [{
        topic: topic,
        numPartitions: numPartitions
      }],
    })
    const fetchTopicOffsets = await admin.fetchTopicOffsets('topic-test')
    console.log(`Crate topic:${topic} successful!`)
    console.log(fetchTopicOffsets)
  }
  await admin.disconnect();
}

const producer = kafka.producer()
const getRandomNumber = () => Math.round(Math.random(10) * 1000)
const createMessage = (num, partition = 0) => ({
  key: `key-${num}`,
  value: `value-${num}-${new Date().toISOString()}`,
  partition: partition
})

const sendMessage = () => {
  return producer
    .send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: Array(createMessage(getRandomNumber()))
      // 用來測試 kafka 多個 Partition 與 Consumer 的關係
      // messages: Array(createMessage(getRandomNumber(), 0), createMessage(getRandomNumber(), 1))
    })
    .then(console.log)
    .catch(e => console.error(`[example/producer] ${e.message}`, e))
}

const run = async () => {
  await crateTopic(topic);
  await producer.connect()
  setInterval(sendMessage, 2000)
}

run().catch(e => console.error(`[example/producer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']
// 收到無法處理錯誤時的處理
errorTypes.forEach(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})
// 收到中斷訊號時的處理
signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})