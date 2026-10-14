/**
 * CRYPTO (ICOPAY custody) notify helpers — JPAY/EP/ox 와 동일 가맹 통보 스키마.
 * 소스: ICOPAY 입금 확정 → POST /noti/crypto (외부 카드 PG 없음).
 */
'use strict';

function normalizeCryptoMethod(raw) {
  return String(raw || '')
    .trim()
    .toLowerCase();
}

/** pay / confirm / confirmed / succeeded → 가맹 통보 */
function cryptoShouldNotifyMerchant(method) {
  const m = normalizeCryptoMethod(method);
  if (!m) return true; // default: ICOPAY confirm mirror
  if (m === 'check' || m === 'pending' || m === 'awaiting') return false;
  if (m === 'pay' || m === 'confirm' || m === 'confirmed' || m === 'deposit') return true;
  if (m === 'succeeded' || m === 'success' || m === 'paid') return true;
  if (m === 'failed' || m === 'fail' || m === 'expired') return true;
  return true;
}

function cryptoIsFailureMethod(method, body) {
  const m = normalizeCryptoMethod(method);
  if (/fail|reject|cancel|expir|error/i.test(m)) return true;
  const st = String(
    (body && (body.status || body.paymentStatus || body.state || body.depositStatus)) || '',
  )
    .trim()
    .toLowerCase();
  if (/fail|reject|cancel|expir|error/i.test(st)) return true;
  if (st === '99' || st === '01') return true;
  return false;
}

/**
 * Map ICOPAY crypto confirm payload → JPAY-compatible merchant notify schema
 * (returncode / orderid / transaction_id / amount).
 */
function mapCryptoToMerchantNotifyBody(cryptoBody, method) {
  const b = cryptoBody && typeof cryptoBody === 'object' ? cryptoBody : {};
  const order = String(
    b.order || b.orderNo || b.OrderNo || b.orderid || b.orderID || b.merchantOrderId || '',
  ).trim();
  const txId = String(
    b.id ||
      b.transaction_id ||
      b.transactionId ||
      b.TransactionId ||
      b.depositTxHash ||
      b.txHash ||
      b.payTrnId ||
      '',
  ).trim();
  const amount = b.amount != null ? b.amount : b.Amount;
  const currency =
    b.currency != null
      ? b.currency
      : b.Currency != null
        ? b.Currency
        : b.assetCode != null
          ? b.assetCode
          : b.quoteCurrency;
  const fail = cryptoIsFailureMethod(method, b);
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
    cryptoReturn: fail ? 'reject' : 'success',
    pgKind: 'crypto',
    van: 'crypto',
    method: String(method || 'confirm').trim(),
    timestamp: b.timestamp != null ? b.timestamp : undefined,
  };
  if (b.status != null && b.status !== '') out.status = b.status;
  if (b.status_message != null && b.status_message !== '') out.status_message = b.status_message;
  if (b.icopay_source != null && b.icopay_source !== '') out.icopay_source = b.icopay_source;
  if (b.assetCode != null && b.assetCode !== '') out.assetCode = b.assetCode;
  if (b.networkCode != null && b.networkCode !== '') out.networkCode = b.networkCode;
  if (b.depositTxHash != null && b.depositTxHash !== '') out.depositTxHash = b.depositTxHash;
  if (b.quoteCurrency != null && b.quoteCurrency !== '') out.quoteCurrency = b.quoteCurrency;
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
      b.buyerEmail ||
      b.payEmailAddress ||
      b.email ||
      b.Email ||
      '',
  ).trim();
  if (cid) {
    out.CustomerId = cid;
    out.customerId = cid;
  }
  const cname = String(
    b.CustomerName || b.customerName || b.buyerName || b.customerNm || b.customer || '',
  ).trim();
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
  normalizeCryptoMethod,
  cryptoShouldNotifyMerchant,
  cryptoIsFailureMethod,
  mapCryptoToMerchantNotifyBody,
  headerGetIgnoreCase,
};
