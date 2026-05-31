require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const helmet = require('helmet');
const promClient = require('prom-client');
const winston = require('winston');
const paymentRouter = require('./routes/payment');
const { connectProducer } = require('./kafka');

const app  = express();
const PORT = process.env.PORT || 3005;

const logger = winston.createLogger({
  format: winston.format.combine(winston.format.timestamp(), winston.format.json()),
  transports: [new winston.transports.Console()],
});

const register = new promClient.Registry();
promClient.collectDefaultMetrics({ register });
const httpReqs = new promClient.Counter({
  name: 'http_requests_total', help: 'HTTP requests',
  labelNames: ['method', 'status'], registers: [register],
});

// Raw body for webhook signature verification
app.use('/payment/webhook', express.raw({ type: '*/*' }), (req, res, next) => {
  try { req.body = JSON.parse(req.body); } catch {}
  next();
});
app.use(helmet()); app.use(cors()); app.use(express.json());
app.use((req, res, next) => { res.on('finish', () => httpReqs.inc({ method: req.method, status: res.statusCode })); next(); });

app.use('/payment', paymentRouter);
app.get('/health', (req, res) => res.json({ status: 'ok', service: 'payment-service' }));
app.get('/metrics', async (req, res) => { res.set('Content-Type', register.contentType); res.end(await register.metrics()); });

async function connectWithRetry(uri, retries = 10, delay = 5000) {
  for (let i = 0; i < retries; i++) {
    try { await mongoose.connect(uri); logger.info({ msg: 'MongoDB connected' }); return; }
    catch (err) { logger.warn({ msg: `Retry ${i + 1}/${retries}`, err: err.message }); await new Promise(r => setTimeout(r, delay)); }
  }
  throw new Error('MongoDB connection failed');
}

async function connectKafkaWithRetry(retries = 10, delay = 5000) {
  for (let i = 0; i < retries; i++) {
    try { await connectProducer(); return; }
    catch (err) { logger.warn({ msg: `Kafka retry ${i + 1}/${retries}`, err: err.message }); await new Promise(r => setTimeout(r, delay)); }
  }
  logger.error({ msg: 'Kafka unavailable — proceeding without it' });
}

connectWithRetry(process.env.MONGO_URI || 'mongodb://localhost:27017/ipl_db')
  .then(() => connectKafkaWithRetry())
  .then(() => app.listen(PORT, () => logger.info({ msg: 'Payment service started', port: PORT })))
  .catch(err => { logger.error({ msg: 'Startup failed', err: err.message }); process.exit(1); });
