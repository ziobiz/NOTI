/**
 * QBANG(큐뱅) NOTI helpers — ICOPAY PL 승인 미러 → 가맹 통보 스키마.
 */
'use strict';

function normalizeQbangMethod(raw) {
  return String(raw || '')
    .trim()
    .toLowerCase();
}

function qbangShouldNotifyMerchant(method) {
  const m = normalizeQbangMethod(method);
  if (!m) return true;
  if (m === 'check' || m === 'pending' || m === 'poll') return false;
  return true;
}

function qbangIsFailureMethod(method, body) {
  const m = normalizeQbangMethod(method);
  if (/fail|reject|cancel|expir|error/i.test(m)) return true;
  const st = String(
    (body && (body.status || body.paymentStatus || body.state)) || '',
  )
    .trim()
    .toLowerCase();
  if (/fail|reject|cancel|expir|error/i.test(st)) return true;
  if (st === '99' || st === '01' || st === 'failed') return true;
  return false;
}

function mapQbangToMerchantNotifyBody(qbangBody, method) {
  const b = qbangBody && typeof qbangBody === 'object' ? qbangBody : {};
  const order = String(
    b.order || b.orderNo || b.OrderNo || b.orderid || b.orderID || '',
  ).trim();
  const txId = String(
    b.transactionNo ||
      b.transaction_id ||
      b.transactionId ||
      b.TransactionId ||
      b.approvalNo ||
      b.id ||
      '',
  ).trim();
  const amount = b.amount != null ? b.amount : b.Amount;
  const currency = b.currency != null ? b.currency : b.Currency;
  const fail = qbangIsFailureMethod(method, b);
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
    pgKind: 'qbang',
    van: 'QBANG',
    method: String(method || 'confirm').trim(),
    timestamp: b.timestamp != null ? b.timestamp : undefined,
  };
  if (b.approvalNo != null && b.approvalNo !== '') out.approvalNo = b.approvalNo;
  if (b.compId != null && b.compId !== '') out.compId = b.compId;
  if (b.CompId != null && b.CompId !== '') out.CompId = b.CompId;
  if (b['Comp-Id'] != null && b['Comp-Id'] !== '') {
    out['Comp-Id'] = b['Comp-Id'];
    if (!out.compId) out.compId = b['Comp-Id'];
  }
  if (b.merchantId != null && b.merchantId !== '') out.merchantId = b.merchantId;
  const cid = String(b.CustomerId || b.customerId || b.buyerEmail || b.email || '').trim();
  if (cid) {
    out.CustomerId = cid;
    out.customerId = cid;
  }
  const cname = String(b.CustomerName || b.customerName || b.buyerName || b.customerNm || '').trim();
  if (cname) {
    out.CustomerName = cname;
    out.customerName = cname;
  }
  return out;
}

module.exports = {
  qbangShouldNotifyMerchant,
  qbangIsFailureMethod,
  mapQbangToMerchantNotifyBody,
};
