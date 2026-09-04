/**
 * ElementPay (EP) webhook helpers — mapping & method policy.
 * Wired from server.js (MERCHANTS / relay live there).
 */

function normalizeElementPayMethod(raw) {
  return String(raw || '')
    .trim()
    .toLowerCase();
}

/** check: no merchant notify; pay / payment.*: notify when settlement-related */
function elementPayShouldNotifyMerchant(method) {
  const m = normalizeElementPayMethod(method);
  if (!m || m === 'check') return false;
  if (m === 'pay') return true;
  if (m.startsWith('payment.')) return true;
  return false;
}

/** ElementPay callback/mirror numeric status (205=success, 204=fail, …). */
function elementPayCallbackStatusCode(body) {
  if (!body || typeof body !== 'object') return null;
  const raw = body.status != null ? body.status : body.Status;
  if (raw == null || raw === '') return null;
  const n = Number(String(raw).trim());
  return Number.isFinite(n) ? n : null;
}

/**
 * EP / ICOPAY mirror payload shape (not ChillPay PaymentStatus 0–25).
 * status 204/205/206/270/401/474/475 or icopay_source / pgKind=elementpay.
 */
function looksLikeElementPayCallbackBody(body) {
  if (!body || typeof body !== 'object') return false;
  const pg = String(body.pgKind || body.van || '')
    .toLowerCase()
    .trim();
  if (pg === 'elementpay' || pg === 'ep') return true;
  if (body.icopay_source != null && String(body.icopay_source).trim() !== '') return true;
  const code = elementPayCallbackStatusCode(body);
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

/** EP 205 Payment success (and success message). */
function elementPayIsSuccessCallbackStatus(body) {
  if (!body || typeof body !== 'object') return false;
  const code = elementPayCallbackStatusCode(body);
  if (code === 205) return true;
  const msg = String(body.status_message || body.statusMessage || body.message || '')
    .trim()
    .toLowerCase();
  if (msg === 'payment success' || msg === 'payment can process') return true;
  return false;
}

/** EP 204 reject / auth-order errors. */
function elementPayIsFailureCallbackStatus(body) {
  if (!body || typeof body !== 'object') return false;
  if (elementPayIsSuccessCallbackStatus(body)) return false;
  const code = elementPayCallbackStatusCode(body);
  if (code === 204 || code === 401 || code === 474 || code === 475) return true;
  const msg = String(body.status_message || body.statusMessage || body.message || '')
    .trim()
    .toLowerCase();
  if (/reject|fail|wrong hash|wrong order|not found|error/i.test(msg)) return true;
  return false;
}

function elementPayIsFailureMethod(method, body) {
  const m = normalizeElementPayMethod(method);
  if (/reject|fail|cancel|error|declin/i.test(m)) return true;
  if (looksLikeElementPayCallbackBody(body)) {
    if (elementPayIsSuccessCallbackStatus(body)) return false;
    if (elementPayIsFailureCallbackStatus(body)) return true;
  }
  const st = String((body && (body.status || body.paymentStatus || body.state)) || '')
    .trim()
    .toLowerCase();
  // Do not treat numeric EP codes (205/204) as text failure tokens
  if (/^\d+$/.test(st)) return false;
  if (/reject|fail|cancel|error|declin|unsuccess/i.test(st)) return true;
  return false;
}

/**
 * Map EP form fields → JPAY-compatible merchant notify schema
 * (merchants already parse returncode / orderid / transaction_id / amount).
 */
function mapElementPayToMerchantNotifyBody(epBody, method) {
  const b = epBody && typeof epBody === 'object' ? epBody : {};
  const order = String(b.order || b.orderNo || b.OrderNo || b.orderid || '').trim();
  const txId = String(b.id || b.transaction_id || b.transactionId || b.TransactionId || '').trim();
  const amount = b.amount != null ? b.amount : b.Amount;
  const currency = b.currency != null ? b.currency : b.Currency;
  const fail = elementPayIsFailureMethod(method, b);
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
    /* pay-result.html: 0/1 과 Paid/Failed 모두 인식. ChillPay 숫자(10)와 혼동 방지차 Succeeded도 유지 */
    PaymentStatus: fail ? '1' : '0',
    chillPaymentStatus: fail ? 'Failed' : 'Paid',
    outcome: fail ? 'reject' : 'success',
    elementpayReturn: fail ? 'reject' : 'success',
    pgKind: 'elementpay',
    van: 'elementpay',
    method: String(method || '').trim(),
    timestamp: b.timestamp != null ? b.timestamp : undefined,
  };
  // Keep EP/ICOPAY status codes for admin display (205=success, 204=fail)
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
  if (b.hash != null) out.ep_hash = b.hash;
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
  normalizeElementPayMethod,
  elementPayShouldNotifyMerchant,
  elementPayIsFailureMethod,
  elementPayCallbackStatusCode,
  looksLikeElementPayCallbackBody,
  elementPayIsSuccessCallbackStatus,
  elementPayIsFailureCallbackStatus,
  mapElementPayToMerchantNotifyBody,
  headerGetIgnoreCase,
};
