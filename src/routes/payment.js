const router  = require('express').Router();
const crypto  = require('crypto');
const { v4: uuidv4 } = require('uuid');
const axios   = require('axios');
const Payment = require('../models/Payment');
const { publishEvent } = require('../kafka');
const winston = require('winston');

const logger = winston.createLogger({
  format: winston.format.combine(winston.format.timestamp(), winston.format.json()),
  transports: [new winston.transports.Console()],
});

const BOOKING_SERVICE_URL = process.env.BOOKING_SERVICE_URL || 'http://booking-service:3004';
const TICKET_SERVICE_URL  = process.env.TICKET_SERVICE_URL  || 'http://ticket-service:3006';

// After payment resolves, confirm booking + generate ticket via direct HTTP
// This runs whether or not Kafka is available
async function postPaymentSuccess({ bookingId, orderId, paymentId, amount, userId, userEmail, seats, matchDetails }) {
  try {
    await axios.put(`${BOOKING_SERVICE_URL}/bookings/${bookingId}/confirm`,
      { paymentId: paymentId.toString(), orderId }, { timeout: 8000 });
    logger.info({ msg: 'Booking confirmed via HTTP', bookingId });
  } catch (err) {
    logger.error({ msg: 'Failed to confirm booking via HTTP', bookingId, err: err.message });
  }

  try {
    await axios.post(`${TICKET_SERVICE_URL}/tickets/generate`, {
      bookingId, userId, matchId: matchDetails?._id,
      seats, amount, matchDetails, userEmail,
    }, { timeout: 15000 });
    logger.info({ msg: 'Ticket generated via HTTP', bookingId });
  } catch (err) {
    logger.error({ msg: 'Failed to generate ticket via HTTP', bookingId, err: err.message });
  }
}

async function postPaymentFailure({ bookingId }) {
  try {
    // Mark booking cancelled directly
    await axios.put(`${BOOKING_SERVICE_URL}/bookings/${bookingId}/cancel`, {}, { timeout: 8000 });
  } catch {}
}

// POST /payment/initiate
router.post('/initiate', async (req, res) => {
  try {
    const { bookingId, amount, userId, userEmail, idempotencyKey, seats, matchDetails } = req.body;
    const uid   = userId    || req.headers['x-user-id'];
    const email = userEmail || req.headers['x-user-email'];

    if (idempotencyKey) {
      const existing = await Payment.findOne({ idempotencyKey });
      if (existing) return res.json({ success: true, data: { orderId: existing.orderId, status: existing.status } });
    }

    const orderId = `IPL_ORDER_${uuidv4().replace(/-/g, '').toUpperCase().slice(0, 16)}`;

    const payment = await Payment.create({
      bookingId, orderId, amount, userId: uid, userEmail: email,
      status: 'processing',
      idempotencyKey: idempotencyKey || orderId,
    });

    const roll          = parseInt(crypto.randomBytes(1).toString('hex'), 16) % 100;
    const willSucceed   = roll < 95;
    const processingDelay = 3000 + (roll % 5) * 1000;

    setTimeout(async () => {
      try {
        if (willSucceed) {
          await Payment.findByIdAndUpdate(payment._id, {
            status: 'completed',
            gatewayResponse: { code: 'PAYMENT_SUCCESS', message: 'Payment processed successfully' },
          });

          // Publish to Kafka (if available) — non-fatal
          try {
            await publishEvent('payment.completed', bookingId, {
              bookingId, orderId, amount, userId: uid, userEmail: email,
              paymentId: payment._id, seats, matchDetails,
            });
          } catch {}

          // Always call services directly (works without Kafka)
          await postPaymentSuccess({
            bookingId, orderId, paymentId: payment._id,
            amount, userId: uid, userEmail: email, seats, matchDetails,
          });

        } else {
          await Payment.findByIdAndUpdate(payment._id, {
            status: 'failed',
            gatewayResponse: { code: 'PAYMENT_FAILED', message: 'Payment declined by bank' },
          });

          try {
            await publishEvent('payment.failed', bookingId, {
              bookingId, orderId, reason: 'Payment declined by bank',
            });
          } catch {}

          await postPaymentFailure({ bookingId });
        }
      } catch (err) {
        logger.error({ msg: 'Post-payment processing error', err: err.message });
      }
    }, processingDelay);

    res.json({
      success: true,
      data: {
        orderId, status: 'processing',
        message: 'Payment is being processed. Please wait…',
        estimatedSeconds: Math.ceil(processingDelay / 1000),
      },
    });
  } catch (err) {
    if (err.code === 11000) {
      const p = await Payment.findOne({ idempotencyKey: req.body.idempotencyKey });
      return res.json({ success: true, data: { orderId: p.orderId, status: p.status } });
    }
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST /payment/webhook — Razorpay-style HMAC-verified callback
router.post('/webhook', async (req, res) => {
  try {
    const signature = req.headers['x-razorpay-signature'];
    const secret    = process.env.PAYMENT_WEBHOOK_SECRET || 'webhook-secret';
    const rawBody   = JSON.stringify(req.body);
    const expected  = crypto.createHmac('sha256', secret).update(rawBody).digest('hex');

    if (signature && signature !== expected) {
      return res.status(400).json({ success: false, error: 'Invalid signature' });
    }

    const { event, payload } = req.body;
    const entity = payload?.payment?.entity || {};
    const payment = await Payment.findOne({ orderId: entity.order_id });
    if (!payment) return res.status(404).json({ success: false, error: 'Payment not found' });

    if (event === 'payment.captured') {
      await Payment.findByIdAndUpdate(payment._id, { status: 'completed', gatewayResponse: entity });
      try { await publishEvent('payment.completed', payment.bookingId.toString(), { bookingId: payment.bookingId, orderId: payment.orderId, amount: payment.amount }); } catch {}
      await postPaymentSuccess({ bookingId: payment.bookingId, orderId: payment.orderId, paymentId: payment._id, amount: payment.amount });
    } else if (event === 'payment.failed') {
      await Payment.findByIdAndUpdate(payment._id, { status: 'failed', gatewayResponse: entity });
      try { await publishEvent('payment.failed', payment.bookingId.toString(), { bookingId: payment.bookingId }); } catch {}
      await postPaymentFailure({ bookingId: payment.bookingId });
    }
    res.json({ success: true });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// GET /payment/status/:orderId
router.get('/status/:orderId', async (req, res) => {
  try {
    const payment = await Payment.findOne({ orderId: req.params.orderId });
    if (!payment) return res.status(404).json({ success: false, error: 'Payment not found' });
    res.json({ success: true, data: payment });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

module.exports = router;
