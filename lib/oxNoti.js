/**
 * ox (OxPay Financial) webhook helpers — same merchant notify schema as JPAY/EP.
 */
function normalizeOxMethod(raw) {
  return String(raw || '')
    .trim()
    .toLowerCase();
}

/** check: no merchant notify; pay / payment.* / succeeded: notify */
function oxShouldNotifyMerchant(method) {
  const m = normalizeOxMethod(method);
  if (!m || m === 'check') return false;
  if (m === 'pay') return true;
  if (m.startsWith('payment.')) return true;
  if (m === 'succeeded' || m === 'success' || m === 'failed' || m === 'fail') return true;
  return false;
}

function oxCallbackStatusCode(body) {
  if (!body || typeof body !== 'object') return null;
  const raw = body.status != null ? body.status : body.Status;
  if (raw == null || raw === '') return null;
  const n = Number(String(raw).trim());
  return Number.isFinite(n) ? n : null;
}

function looksLikeOxCallbackBody(body) {
  if (!body || typeof body !== 'object') return false;
  const pg = String(body.pgKind || body.van || '')
    .toLowerCase()
    .trim();
  if (pg === 'ox' || pg === 'oxpay') return true;
  if (body.icopay_source != null && String(body.icopay_source).trim() !== '') {
    const src = String(body.icopay_source).toLowerCase();
    if (src.includes('ox')) return true;
  }
  const st = String(body.status || body.paymentStatus || body.state || '')
    .trim()
    .toLowerCase();
  if (st === 'succeeded' || st === 'success' || st === 'failed' || st === 'fail' || st === 'paid') {
    return true;
  }
  const code = oxCallbackStatusCode(body);
  if (code == null) return false;
  return (
    code === 204 ||
    code === 205 ||
    code === 206 ||
    code === 270 ||
    code === 401 ||
    code === 474 ||
    code === 475
  );
}

function oxIsSuccessCallbackStatus(body) {
  if (!body || typeof body !== 'object') return false;
  const code = oxCallbackStatusCode(body);
  if (code === 205) return true;
  const st = String(body.status || body.paymentStatus || body.state || '')
    .trim()
    .toLowerCase();
  if (st === 'succeeded' || st === 'success' || st === 'paid' || st === '00') return true;
  const msg = String(body.status_message || body.statusMessage || body.message || '')
    .trim()
    .toLowerCase();
  if (msg === 'payment success' || msg === 'payment can process' || msg === 'success') return true;
  return false;
}

function oxIsFailureCallbackStatus(body) {
  if (!body || typeof body !== 'object') return false;
  if (oxIsSuccessCallbackStatus(body)) return false;
  const code = oxCallbackStatusCode(body);
  if (code === 204 || code === 401 || code === 474 || code === 475) return true;
  const st = String(body.status || body.paymentStatus || body.state || '')
    .trim()
    .toLowerCase();
  if (st === 'failed' || st === 'fail' || st === 'rejected' || st === 'declined') return true;
  const msg = String(body.status_message || body.statusMessage || body.message || '')
    .trim()
    .toLowerCase();
  if (/reject|fail|wrong hash|wrong order|not found|error|declin/i.test(msg)) return true;
  return false;
}

function oxIsFailureMethod(method, body) {
  const m = normalizeOxMethod(method);
  if (/reject|fail|cancel|error|declin/i.test(m)) return true;
  if (looksLikeOxCallbackBody(body)) {
    if (oxIsSuccessCallbackStatus(body)) return false;
    if (oxIsFailureCallbackStatus(body)) return true;
  }
  const st = String((body && (body.status || body.paymentStatus || body.state)) || '')
    .trim()
    .toLowerCase();
  if (/^\d+$/.test(st)) return false;
  if (/reject|fail|cancel|error|declin|unsuccess/i.test(st)) return true;
  return false;
}

/**
 * Map ox / ICOPAY mirror fields → JPAY-compatible merchant notify schema
 * (returncode / orderid / transaction_id / amount) — same as EP/JPAY merchants.
 */
function mapOxToMerchantNotifyBody(oxBody, method) {
  const b = oxBody && typeof oxBody === 'object' ? oxBody : {};
  const order = String(
    b.order || b.orderNo || b.OrderNo || b.orderid || b.merchantOrderId || b.reference || '',
  ).trim();
  const txId = String(
    b.id ||
      b.transaction_id ||
      b.transactionId ||
      b.TransactionId ||
      b.paymentId ||
      b.PaymentId ||
      '',
  ).trim();
  const amount = b.amount != null ? b.amount : b.Amount;
  const currency = b.currency != null ? b.currency : b.Currency;
  const fail = oxIsFailureMethod(method, b);
  const returncode = fail ? '01' : '00';
  const out = {
    orderid: order,
    orderID: order,
    OrderNo: order,
    orderNo: order,
    transaction_id: txId,
    TransactionId: txId,
    returncode,
    amount,
    Amount: amount,
    currency,
    Currency: currency,
    paymentStatus: fail ? 'Failed' : 'Succeeded',
    PaymentStatus: fail ? '1' : '0',
    chillPaymentStatus: fail ? 'Failed' : 'Paid',
    outcome: fail ? 'reject' : 'success',
    oxReturn: fail ? 'reject' : 'success',
    pgKind: 'ox',
    van: 'ox',
    method: String(method || '').trim(),
    timestamp: b.timestamp != null ? b.timestamp : undefined,
  };
  if (b.status != null && b.status !== '') out.status = b.status;
  if (b.status_message != null && b.status_message !== '') out.status_message = b.status_message;
  if (b.icopay_source != null && b.icopay_source !== '') out.icopay_source = b.icopay_source;
  if (b.compId != null && b.compId !== '') out.compId = b.compId;
  if (b.CompId != null && b.CompId !== '') out.CompId = b.CompId;
  if (b['Comp-Id'] != null && b['Comp-Id'] !== '') {
    out['Comp-Id'] = b['Comp-Id'];
    if (!out.compId) out.compId = b['Comp-Id'];
    if (!out.CompId) out.CompId = b['Comp-Id'];
  }
  if (b.merchantId != null && b.merchantId !== '') out.merchantId = b.merchantId;
  if (b.MID != null && b.MID !== '' && !out.compId) out.compId = b.MID;
  const cid = String(
    b.CustomerId ||
      b.customerId ||
      b.payEmailAddress ||
      b.pay_email_address ||
      b.email ||
      b.Email ||
      '',
  ).trim();
  if (cid) {
    out.CustomerId = cid;
    out.customerId = cid;
  }
  const cname = String(b.CustomerName || b.customerName || b.customerNm || b.customer || '').trim();
  if (cname) out.CustomerName = cname;
  return out;
}

function headerGetIgnoreCase(headers, name) {
  if (!headers || typeof headers !== 'object') return '';
  const want = String(name).toLowerCase();
  for (const [k, v] of Object.entries(headers)) {
    if (String(k).toLowerCase() === want) {
      if (Array.isArray(v)) return String(v[0] || '').trim();
      return String(v == null ? '' : v).trim();
    }
  }
  return '';
}

module.exports = {
  normalizeOxMethod,
  oxShouldNotifyMerchant,
  oxIsFailureMethod,
  oxCallbackStatusCode,
  looksLikeOxCallbackBody,
  oxIsSuccessCallbackStatus,
  oxIsFailureCallbackStatus,
  mapOxToMerchantNotifyBody,
  headerGetIgnoreCase,
};
