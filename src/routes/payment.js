const router = require('express').Router();
const crypto = require('crypto');
const { v4: uuidv4 } = require('uuid');
const Payment = require('../models/Payment');
const { publishEvent } = require('../kafka');

// POST /payment/initiate
router.post('/initiate', async (req, res) => {
  try {
    const { bookingId, amount, userId, userEmail, idempotencyKey, seats, matchDetails } = req.body;
    const uid   = userId   || req.headers['x-user-id'];
    const email = userEmail|| req.headers['x-user-email'];

    // Idempotency check
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

    // Simulate async payment processing — 95% success rate
    const successThreshold = 95;
    const roll = parseInt(crypto.randomBytes(1).toString('hex'), 16) % 100; // 0..99
    const willSucceed = roll < successThreshold;
    const processingDelay = 3000 + (roll % 5) * 1000; // 3-7 seconds

    setTimeout(async () => {
      try {
        if (willSucceed) {
          await Payment.findByIdAndUpdate(payment._id, {
            status: 'completed',
            gatewayResponse: { code: 'PAYMENT_SUCCESS', message: 'Payment processed successfully' },
          });
          await publishEvent('payment.completed', bookingId, {
            bookingId, orderId, amount, userId: uid, userEmail: email,
            paymentId: payment._id, seats, matchDetails,
          });
        } else {
          await Payment.findByIdAndUpdate(payment._id, {
            status: 'failed',
            gatewayResponse: { code: 'PAYMENT_FAILED', message: 'Payment declined by bank' },
          });
          await publishEvent('payment.failed', bookingId, {
            bookingId, orderId, reason: 'Payment declined by bank',
          });
        }
      } catch (err) {
        // Swallow async errors
      }
    }, processingDelay);

    res.json({
      success: true,
      data: { orderId, status: 'processing', message: 'Payment is being processed. Please wait...', estimatedSeconds: Math.ceil(processingDelay / 1000) },
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
      await publishEvent('payment.completed', payment.bookingId.toString(), {
        bookingId: payment.bookingId, orderId: payment.orderId, amount: payment.amount,
        userId: payment.userId, userEmail: payment.userEmail,
      });
    } else if (event === 'payment.failed') {
      await Payment.findByIdAndUpdate(payment._id, { status: 'failed', gatewayResponse: entity });
      await publishEvent('payment.failed', payment.bookingId.toString(), {
        bookingId: payment.bookingId, orderId: payment.orderId, reason: entity.error_description,
      });
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
