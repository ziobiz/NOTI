/**
 * Tazapay webhook helpers —
 * 1) Tazapay 원본 JSON webhook → 서명 검증 → ICOPAY …/TAZAPAY → Comp-Id 가맹 릴레이
 * 2) ICOPAY 통합 JSON(event=pg.payment.status) → 재포워드 금지, 가맹 릴레이만
 * 가맹 본문: ICOPAY 통합 필드 (성공/실패·orderNo·금액). PG 브랜드명 최소화.
 */
'use strict';

const crypto = require('crypto');

function nested(obj, dotted) {
  if (!obj || typeof obj !== 'object' || !dotted) return undefined;
  const parts = String(dotted).split('.');
  let cur = obj;
  for (const p of parts) {
    if (cur == null || typeof cur !== 'object') return undefined;
    cur = cur[p];
  }
  return cur;
}

function pick(obj, keys) {
  for (const k of keys) {
    const v = k.includes('.') ? nested(obj, k) : obj && obj[k];
    if (v != null && String(v).trim() !== '') return v;
  }
  return undefined;
}

function headerGetIgnoreCase(headers, name) {
  if (!headers || typeof headers !== 'object') return '';
  const want = String(name || '').toLowerCase();
  for (const [k, v] of Object.entries(headers)) {
    if (String(k).toLowerCase() === want) {
      return v == null ? '' : String(v).trim();
    }
  }
  return '';
}

function cleanCompId(compId) {
  const id = String(compId || '').trim();
  if (!id) return '';
  const hash = id.indexOf('#');
  return hash > 0 ? id.slice(0, hash) : id;
}

/** ICOPAY 아웃바운드 통합 통보 — ICOPAY 로 재포워드 금지 */
function looksLikeIcopayUnifiedNotify(body) {
  const b = body && typeof body === 'object' ? body : {};
  const ev = String(pick(b, ['event', 'Event']) || '')
    .trim()
    .toLowerCase();
  if (ev === 'pg.payment.status' || ev === 'payment.status') return true;
  const st = String(pick(b, ['status', 'Status']) || '').trim();
  const paySt = String(pick(b, ['paymentStatus', 'PaymentStatus']) || '')
    .trim()
    .toUpperCase();
  const unifiedStatus =
    st === '10' ||
    st === '99' ||
    paySt === 'PAID' ||
    paySt === 'FAILED' ||
    paySt === 'FAIL';
  const hasIcoKeys =
    unifiedStatus &&
    !!pick(b, ['trnId', 'pgTxnId', 'orderNo', 'OrderNo', 'compId', 'CompId', 'Comp-Id']);
  const hasTazaEvt = !!pick(b, ['type']) && String(pick(b, ['object']) || '').toLowerCase() === 'event';
  return hasIcoKeys && !hasTazaEvt;
}

/**
 * Tazapay Webhook Authentication
 * signature = Base64(HMAC-SHA256(secret, event_id + raw_payload + created_at))
 * Header: signature (또는 webhook-signature)
 */
function verifyTazapayWebhookSignature(rawBody, headers, secret) {
  const sec = String(secret || '').trim();
  if (!sec) return { ok: false, reason: 'webhook_secret_missing' };
  const raw = rawBody != null ? String(rawBody) : '';
  let parsed = null;
  try {
    parsed = JSON.parse(raw || '{}');
  } catch (_) {
    return { ok: false, reason: 'invalid_json' };
  }
  const eventId = String(pick(parsed, ['id']) || '').trim();
  const createdAt = String(pick(parsed, ['created_at', 'createdAt']) || '').trim();
  if (!eventId || !createdAt) return { ok: false, reason: 'missing_event_id_or_created_at' };
  const hdr =
    headerGetIgnoreCase(headers, 'signature') ||
    headerGetIgnoreCase(headers, 'webhook-signature') ||
    headerGetIgnoreCase(headers, 'x-tazapay-signature') ||
    '';
  if (!hdr) return { ok: false, reason: 'missing_signature_header' };
  const expected = crypto.createHmac('sha256', sec).update(eventId + raw + createdAt, 'utf8').digest('base64');
  const a = Buffer.from(String(hdr).trim());
  const b = Buffer.from(expected);
  if (a.length !== b.length) return { ok: false, reason: 'signature_mismatch' };
  try {
    if (!crypto.timingSafeEqual(a, b)) return { ok: false, reason: 'signature_mismatch' };
  } catch (_) {
    return { ok: false, reason: 'signature_mismatch' };
  }
  return { ok: true, eventId, createdAt };
}

const SUCCESS_TYPES = new Set([
  'checkout.paid',
  'payin.succeeded',
  'payment_attempt.succeeded',
]);
const FAIL_TYPES = new Set([
  'payment_attempt.failed',
  'payin.cancelled',
  'payment_attempt.reversed',
  'checkout.expired',
]);
const REFUND_TYPES = new Set(['refund.created', 'refund.succeeded', 'payment_attempt.reversed']);

function tazapayEventType(body) {
  return String(pick(body, ['type', 'event', 'Event']) || '')
    .trim()
    .toLowerCase();
}

function tazapayIsExplicitFailure(body) {
  const b = body && typeof body === 'object' ? body : {};
  if (looksLikeIcopayUnifiedNotify(b)) {
    const st = String(pick(b, ['status', 'Status']) || '').trim();
    const paySt = String(pick(b, ['paymentStatus', 'PaymentStatus']) || '')
      .trim()
      .toUpperCase();
    if (st === '99' || paySt === 'FAILED' || paySt === 'FAIL') return true;
    return false;
  }
  const typ = tazapayEventType(b);
  if (FAIL_TYPES.has(typ) || REFUND_TYPES.has(typ)) return true;
  const data = b.data && typeof b.data === 'object' ? b.data : {};
  const st = String(pick(data, ['status', 'payment_status', 'paymentStatus']) || '')
    .trim()
    .toLowerCase();
  return /fail|cancel|expired|revers/.test(st);
}

function tazapayIsPaid(body) {
  const b = body && typeof body === 'object' ? body : {};
  if (tazapayIsExplicitFailure(b)) return false;
  if (looksLikeIcopayUnifiedNotify(b)) {
    const st = String(pick(b, ['status', 'Status']) || '').trim();
    const paySt = String(pick(b, ['paymentStatus', 'PaymentStatus']) || '')
      .trim()
      .toUpperCase();
    return st === '10' || paySt === 'PAID' || paySt === 'SUCCEEDED' || paySt === 'SUCCESS';
  }
  const typ = tazapayEventType(b);
  if (SUCCESS_TYPES.has(typ)) return true;
  const data = b.data && typeof b.data === 'object' ? b.data : {};
  const paySt = String(pick(data, ['payment_status', 'paymentStatus', 'status']) || '')
    .trim()
    .toLowerCase();
  return paySt === 'paid' || paySt === 'succeeded' || paySt === 'success';
}

function tazapayShouldNotifyMerchant(body) {
  const b = body && typeof body === 'object' ? body : {};
  if (looksLikeIcopayUnifiedNotify(b)) return true;
  const typ = tazapayEventType(b);
  if (SUCCESS_TYPES.has(typ) || FAIL_TYPES.has(typ) || REFUND_TYPES.has(typ)) return true;
  /* 미매칭·기타 이벤트도 로그는 남기되 릴레이는 스킵 가능 — 호출측에서 append 후 skip */
  return false;
}

function zeroDecimalCurrency(cur) {
  return /^(JPY|KRW|VND|CLP|ISK|UGX)$/i.test(String(cur || '').trim());
}

/** Tazapay amount 는 보통 minor unit. 표시·ICOPAY 용 major 문자열 */
function formatTazapayAmount(amountRaw, currency) {
  if (amountRaw == null || String(amountRaw).trim() === '') return '';
  const n = Number(amountRaw);
  if (!Number.isFinite(n)) return String(amountRaw).trim();
  if (zeroDecimalCurrency(currency)) return String(Math.round(n));
  /* 정수처럼 보이면 minor → major */
  if (Number.isInteger(n) && Math.abs(n) >= 1) {
    return (n / 100).toFixed(2);
  }
  return String(n);
}

function pickTazapayOrderNo(b, merchantOrderNo) {
  const data = b.data && typeof b.data === 'object' ? b.data : {};
  const meta = (data.metadata && typeof data.metadata === 'object' ? data.metadata : null) ||
    (b.metadata && typeof b.metadata === 'object' ? b.metadata : null) ||
    {};
  return String(
    merchantOrderNo ||
      pick(b, ['orderNo', 'OrderNo', 'order_id', 'orderId']) ||
      pick(meta, ['orderNo', 'OrderNo', 'order_id', 'merchant_order_no', 'merchantOrderNo']) ||
      pick(data, ['reference_id', 'referenceId', 'customer_reference', 'invoice_number']) ||
      '',
  ).trim();
}

function pickTazapayTxnId(b) {
  const data = b.data && typeof b.data === 'object' ? b.data : {};
  return String(
    pick(b, ['trnId', 'pgTxnId', 'TransactionId', 'transactionId']) ||
      pick(data, ['id', 'payin', 'latest_payment_attempt', 'reference_id']) ||
      pick(b, ['id']) ||
      '',
  ).trim();
}

/**
 * 가맹 릴레이·노티 로그용 본문 (ICOPAY 통합 필드).
 */
function mapTazapayToMerchantNotifyBody(evtBody, merchantOrderNo, extra) {
  const b = evtBody && typeof evtBody === 'object' ? evtBody : {};
  if (looksLikeIcopayUnifiedNotify(b)) {
    const paid = tazapayIsPaid(b);
    const orderNo = String(pick(b, ['orderNo', 'OrderNo']) || merchantOrderNo || '').trim();
    const amount = String(pick(b, ['amount', 'Amount']) || '').trim();
    const currency = String(pick(b, ['currency', 'Currency']) || 'USD');
    const txId = String(
      (extra && extra.trnId) || pick(b, ['trnId', 'pgTxnId', 'TransactionId']) || '',
    ).trim();
    const out = {
      event: 'pg.payment.status',
      orderNo,
      OrderNo: orderNo,
      orderid: orderNo,
      orderID: orderNo,
      status: paid ? '10' : '99',
      paymentStatus: paid ? 'PAID' : 'FAILED',
      PaymentStatus: paid ? '0' : '1',
      amount: amount || undefined,
      Amount: amount || undefined,
      currency,
      Currency: currency,
      ts: Date.now(),
    };
    if (txId) {
      out.trnId = txId;
      out.pgTxnId = txId;
      out.TransactionId = txId;
      out.transactionId = txId;
    }
    const outcomeReasonCode = String(pick(b, ['outcomeReasonCode']) || '').trim();
    const outcomeReason = String(pick(b, ['outcomeReason']) || '').trim();
    if (outcomeReasonCode) out.outcomeReasonCode = outcomeReasonCode;
    if (outcomeReason) out.outcomeReason = outcomeReason;
    const comp = cleanCompId(
      (extra && extra.compId) || pick(b, ['compId', 'CompId', 'Comp-Id', 'merchantId']),
    );
    if (comp) {
      out.compId = comp;
      out.CompId = comp;
      out['Comp-Id'] = comp;
      out.merchantId = comp;
    }
    return out;
  }

  const data = b.data && typeof b.data === 'object' ? b.data : {};
  const typ = tazapayEventType(b);
  const currency = String(
    pick(data, ['invoice_currency', 'charge_currency', 'currency', 'Currency']) ||
      pick(b, ['currency']) ||
      'USD',
  );
  const amountRaw = pick(data, ['amount_paid', 'amount', 'Amount']) || pick(b, ['amount']);
  const amount = formatTazapayAmount(amountRaw, currency);
  const paid = tazapayIsPaid(b);
  const orderNo = pickTazapayOrderNo(b, merchantOrderNo);
  const extraTrn = extra && extra.trnId != null ? String(extra.trnId).trim() : '';
  const txId = extraTrn || pickTazapayTxnId(b);
  const email = String(
    pick(data, ['customer_details.email', 'customer_email', 'email']) ||
      (data.customer_details && data.customer_details.email) ||
      '',
  ).trim();
  const paidAt = String(pick(data, ['created_at', 'paid_at']) || pick(b, ['created_at']) || '').trim();

  const out = {
    event: 'pg.payment.status',
    orderNo: orderNo || '',
    OrderNo: orderNo || '',
    orderid: orderNo || '',
    orderID: orderNo || '',
    status: paid ? '10' : '99',
    paymentStatus: paid ? 'PAID' : 'FAILED',
    PaymentStatus: paid ? '0' : '1',
    amount: amount || undefined,
    Amount: amount || undefined,
    currency,
    Currency: currency,
    ts: Date.now(),
    tazapayEventType: typ || undefined,
  };
  if (txId) {
    out.trnId = txId;
    out.pgTxnId = txId;
    out.TransactionId = txId;
    out.transactionId = txId;
  }
  if (paidAt) {
    out.paidAt = paidAt;
    out.PaymentDate = paidAt;
    out.paymentDate = paidAt;
  }
  if (email) {
    out.email = email;
    out.CustomerEmail = email;
  }
  if (REFUND_TYPES.has(typ)) {
    out.outcomeReasonCode = 'TAZAPAY_REVERSED';
    out.outcomeReason = String(pick(data, ['status_description']) || typ);
  } else if (!paid) {
    out.outcomeReasonCode = 'TAZAPAY_FAIL';
    out.outcomeReason = String(pick(data, ['status_description', 'payment_status_description']) || typ || 'failed');
  }
  const meta = data.metadata && typeof data.metadata === 'object' ? data.metadata : {};
  const comp = cleanCompId(
    (extra && extra.compId) ||
      pick(b, ['compId', 'CompId', 'Comp-Id', 'merchantId']) ||
      pick(meta, ['compId', 'CompId', 'merchantId']),
  );
  if (comp) {
    out.compId = comp;
    out.CompId = comp;
    out['Comp-Id'] = comp;
    out.merchantId = comp;
  }
  Object.keys(out).forEach((k) => {
    if (out[k] === undefined || out[k] === '') {
      if (k === 'orderNo' || k === 'status' || k === 'paymentStatus' || k === 'event') return;
      delete out[k];
    }
  });
  return out;
}

module.exports = {
  pick,
  headerGetIgnoreCase,
  cleanCompId,
  looksLikeIcopayUnifiedNotify,
  verifyTazapayWebhookSignature,
  tazapayEventType,
  tazapayIsExplicitFailure,
  tazapayIsPaid,
  tazapayShouldNotifyMerchant,
  mapTazapayToMerchantNotifyBody,
  pickTazapayOrderNo,
  pickTazapayTxnId,
  formatTazapayAmount,
  SUCCESS_TYPES,
  FAIL_TYPES,
  REFUND_TYPES,
};
