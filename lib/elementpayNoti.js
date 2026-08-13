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

function elementPayIsFailureMethod(method, body) {
  const m = normalizeElementPayMethod(method);
  if (/reject|fail|cancel|error|declin/i.test(m)) return true;
  const st = String((body && (body.status || body.paymentStatus || body.state)) || '')
    .trim()
    .toLowerCase();
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
    PaymentStatus: fail ? '1' : '0',
    pgKind: 'elementpay',
    van: 'elementpay',
    method: String(method || '').trim(),
    timestamp: b.timestamp != null ? b.timestamp : undefined,
  };
  // Keep a few original keys for debugging (optional, non-breaking)
  if (b.hash != null) out.ep_hash = b.hash;
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
  mapElementPayToMerchantNotifyBody,
  headerGetIgnoreCase,
};
