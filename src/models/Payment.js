const mongoose = require('mongoose');

const paymentSchema = new mongoose.Schema({
  bookingId:      { type: mongoose.Schema.Types.ObjectId, required: true, unique: true, index: true },
  orderId:        { type: String, required: true, unique: true },
  amount:         { type: Number, required: true },
  currency:       { type: String, default: 'INR' },
  status:         { type: String, enum: ['created', 'processing', 'completed', 'failed', 'refunded'], default: 'created', index: true },
  paymentMethod:  { type: String, default: 'mock_razorpay' },
  userId:         { type: String },
  userEmail:      { type: String },
  gatewayResponse:{ type: mongoose.Schema.Types.Mixed },
  idempotencyKey: { type: String, unique: true, sparse: true },
}, { timestamps: true });

module.exports = mongoose.model('Payment', paymentSchema);
