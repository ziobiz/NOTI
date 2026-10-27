/**
 * ILK (ILKPAY) Back Noti helpers — JPAY/EP/ox 와 동일 가맹 통보 스키마.
 * 소스: ILK → NOTI /noti/ilk → ICOPAY …/ILK → (Comp-Id) 가맹 Callback 릴레이.
 * QR(PayPay)도 카드와 같은 릴레이 경로·스키마를 씁니다.
 */
'use strict';

function nested(obj, dotted) {
  if (!obj || typeof obj !== 'object' || !dotted) return undefined;
  if (Object.prototype.hasOwnProperty.call(obj, dotted) && obj[dotted] != null && obj[dotted] !== '') {
    return obj[dotted];
  }
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

function normalizeIlkMethod(raw) {
  return String(raw || '')
    .trim()
    .toLowerCase();
}

/** Back Noti 는 항상 가맹 통보 후보(성공·실패 모두). check 없음. */
function ilkShouldNotifyMerchant(method, body) {
  const m = normalizeIlkMethod(method);
  if (m === 'check' || m === 'pending') return false;
  if (body && typeof body === 'object') {
    const st = String(pick(body, ['status', 'Status']) || '')
      .trim()
      .toUpperCase();
    if (st) return true;
  }
  return true;
}

function ilkIsFailure(body) {
  const st = String(pick(body, ['status', 'Status']) || '')
    .trim()
    .toUpperCase();
  if (!st) return false;
  if (st === 'SUCCESS' || st === '00' || st === 'PAID') return false;
  return true;
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

/**
 * ILK Back Noti JSON → JPAY-compatible merchant notify (returncode / orderid / transaction_id).
 */
function mapIlkToMerchantNotifyBody(ilkBody, method) {
  const b = ilkBody && typeof ilkBody === 'object' ? ilkBody : {};
  const order = String(
    pick(b, [
      'clientReferenceInformation.code',
      'order',
      'orderNo',
      'orderid',
      'OrderNo',
    ]) || '',
  ).trim();
  const txId = String(pick(b, ['id', 'transaction_id', 'transactionId', 'TransactionId']) || '').trim();
  const amount = pick(b, [
    'orderInformation.amountDetails.totalAmount',
    'amount',
    'Amount',
    'totalAmount',
  ]);
  const currency = pick(b, [
    'orderInformation.amountDetails.currency',
    'currency',
    'Currency',
  ]);
  const brand = String(
    pick(b, ['paymentInformation.brand', 'brand', 'cardBrand']) || '',
  )
    .trim()
    .toUpperCase();
  const payMethod = String(
    pick(b, ['processingInformation.paymentMethod', 'paymentMethod', 'payMethod']) || '',
  )
    .trim()
    .toUpperCase();
  const isQr = payMethod === 'QR' || brand === 'PAYPAY' || brand === 'QR';
  const fail = ilkIsFailure(b);
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
    pgKind: 'ilk',
    van: 'ILK',
    method: String(method || 'back_noti').trim() || 'back_noti',
    paymentChannel: isQr ? 'QR' : 'FIAT',
    paymentMethod: isQr ? 'QR' : payMethod || 'CARD',
  };
  if (brand) out.brand = brand;
  if (b.status != null && b.status !== '') out.status = b.status;
  const statusMsg = pick(b, ['statusMessage', 'status_message', 'message']);
  if (statusMsg != null && statusMsg !== '') out.status_message = statusMsg;
  const cid = String(pick(b, ['compId', 'CompId', 'Comp-Id', 'merchantId']) || '').trim();
  if (cid) {
    out.compId = cid;
    out.CompId = cid;
    out['Comp-Id'] = cid;
    out.merchantId = cid;
  }
  return out;
}

module.exports = {
  nested,
  pick,
  normalizeIlkMethod,
  ilkShouldNotifyMerchant,
  ilkIsFailure,
  headerGetIgnoreCase,
  mapIlkToMerchantNotifyBody,
};
