/**
 * KSNET webhook helpers (OVERSEAS · USD ONLY) —
 * ICOPAY KSNET approve → POST /noti/ksnet → ICOPAY …/pg-notify/{token}/KSNET → Comp-Id 가맹 릴레이.
 * (OMSPAY 패턴 복제. 해외결제 USD 한정 — 국내 KRW KSNET 아님)
 *
 * 수신 JSON (ICOPAY → NOTI): authyn, trno, amt, authno, ordno, resultcd, Comp-Id, orderNo, currency(USD).
 * 서명·검증은 ICOPAY KSNET 콜백 서비스 담당.
 *
 * 브라우저 Result(return URL)는 ElementPay/ICOPAY 가맹점과 동일 필드
 * (orderid / returncode / paymentStatus / Comp-Id …)를 맞춘다.
 */
'use strict';

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

/** KSNET authyn: O/Y/1 = 승인 */
function ksnetIsApproved(b) {
  const authyn = String(pick(b, ['authyn', 'Authyn', 'AUTHYN']) || '')
    .trim()
    .toUpperCase();
  if (authyn === 'O' || authyn === 'Y' || authyn === '1' || authyn === 'TRUE') return true;
  const rc = String(pick(b, ['resultcd', 'resultCd', 'ResultCd', 'RESULTCD']) || '').trim();
  if (rc === '0000' || rc === '00') return true;
  const st = String(pick(b, ['status', 'Status', 'paymentStatus']) || '')
    .trim()
    .toLowerCase();
  return /paid|success|succeed|complete|approved/.test(st);
}

/** KSNET 노티 — 가맹 릴레이 대상 (ICOPAY가 서명·매칭 판단하므로 기본 true) */
function ksnetShouldNotifyMerchant(body) {
  const b = body && typeof body === 'object' ? body : {};
  if (pick(b, ['trno', 'Trno', 'ordno', 'Ordno', 'orderNo', 'order'])) return true;
  return true;
}

function mapKsnetToMerchantNotifyBody(evtBody) {
  const b = evtBody && typeof evtBody === 'object' ? evtBody : {};
  const trno = String(pick(b, ['trno', 'Trno', 'TRNO']) || '').trim();
  const merchantOrder = String(
    pick(b, ['orderNo', 'OrderNo', 'ordno', 'Ordno', 'ORDNO', 'order', 'orderid', 'orderID']) || '',
  ).trim();
  const orderid = merchantOrder || trno;
  const amountRaw = pick(b, ['amt', 'Amt', 'AMT', 'amount', 'Amount']);
  let amount = '';
  if (amountRaw != null && String(amountRaw).trim() !== '') {
    const n = Number(amountRaw);
    amount = Number.isFinite(n) ? String(n) : String(amountRaw).trim();
  }
  const paid = ksnetIsApproved(b);
  const authno = String(pick(b, ['authno', 'Authno', 'AUTHNO']) || '').trim();
  const resultcd = String(pick(b, ['resultcd', 'resultCd', 'ResultCd', 'RESULTCD']) || '').trim();

  return {
    returncode: paid ? '00' : '99',
    orderid,
    orderID: orderid,
    OrderNo: orderid,
    orderNo: orderid,
    transaction_id: trno,
    trno,
    authno,
    resultcd,
    amount,
    currency: String(pick(b, ['currency', 'Currency']) || 'USD'),
    paymentStatus: paid ? 'Succeeded' : 'Fail',
    PaymentStatus: paid ? '0' : '1',
    chillPaymentStatus: paid ? 'Paid' : 'Fail',
    outcome: paid ? 'success' : 'fail',
    pgKind: 'ksnet',
    pgkind: 'KSNET',
    van: 'KSNET',
    method: paid ? 'waiting' : 'fail',
  };
}

module.exports = {
  pick,
  headerGetIgnoreCase,
  ksnetIsApproved,
  ksnetShouldNotifyMerchant,
  mapKsnetToMerchantNotifyBody,
};
