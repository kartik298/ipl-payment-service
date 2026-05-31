const { Kafka, logLevel } = require('kafkajs');
const winston = require('winston');

const logger = winston.createLogger({
  format: winston.format.combine(winston.format.timestamp(), winston.format.json()),
  transports: [new winston.transports.Console()],
});

const kafka = new Kafka({
  clientId: 'payment-service',
  brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(','),
  logLevel: logLevel.WARN,
  retry: { retries: 10, initialRetryTime: 1000 },
});

const producer = kafka.producer({ allowAutoTopicCreation: true });

async function connectProducer() {
  await producer.connect();
  logger.info({ msg: 'Kafka producer connected (payment-service)' });
}

async function publishEvent(topic, key, value) {
  await producer.send({
    topic,
    messages: [{ key: String(key), value: JSON.stringify(value) }],
  });
}

module.exports = { connectProducer, publishEvent };
