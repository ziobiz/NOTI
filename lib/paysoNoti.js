/**
 * Payso (Pay Solutions) webhook helpers —
 * postback → ICOPAY …/PAYSO → Comp-Id 가맹 릴레이.
 * 가맹 통보 본문은 ICOPAY 통합 API 필드만 (PG명 미포함).
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

function cleanCompId(compId) {
  const id = String(compId || '').trim();
  if (!id) return '';
  const hash = id.indexOf('#');
  return hash > 0 ? id.slice(0, hash) : id;
}

/** Default/form postback 또는 동적 GET — 가맹 릴레이 대상 */
function paysoShouldNotifyMerchant(body) {
  const b = body && typeof body === 'object' ? body : {};
  if (pick(b, ['refno', 'refNo', 'referenceNo', 'transactionId', 'transactionid'])) return true;
  if (pick(b, ['total', 'amount', 'Amount'])) return true;
  return true;
}

/**
 * @param {object} evtBody Payso postback body (form or query)
 * @param {string} [merchantOrderNo] X-Icopay-Order-No
 * @param {object} [extra] { compId, trnId }
 */
function mapPaysoToMerchantNotifyBody(evtBody, merchantOrderNo, extra) {
  const b = evtBody && typeof evtBody === 'object' ? evtBody : {};
  const refno = String(pick(b, ['refno', 'refNo', 'referenceNo']) || '').trim();
  const txnId = String(pick(b, ['transactionId', 'transactionid', 'transaction_id']) || '').trim();
  const fromHdr = String(merchantOrderNo || '').trim();
  const merchantOrder = fromHdr || refno || '';
  const amountRaw = pick(b, ['total', 'amount', 'Amount']);
  let amount = '';
  if (amountRaw != null && String(amountRaw).trim() !== '') {
    const n = Number(amountRaw);
    amount = Number.isFinite(n) ? String(n) : String(amountRaw).trim();
  }
  /* Payso 기본 postback는 성공 통보 위주 — 실패 Result는 status/fail 힌트 */
  const st = String(pick(b, ['status', 'Status', 'paymentStatus', 'result']) || '')
    .trim()
    .toLowerCase();
  const fail =
    st === 'fail' ||
    st === 'failed' ||
    st === 'cancel' ||
    st === 'cancelled' ||
    st === 'void' ||
    String(pick(b, ['returncode', 'PaymentStatus']) || '') === '99' ||
    String(pick(b, ['PaymentStatus']) || '') === '1';
  const paid = !fail;
  const pgTxnId = txnId || refno;
  const out = {
    event: 'pg.payment.status',
    orderNo: merchantOrder || '',
    status: paid ? '10' : '99',
    paymentStatus: paid ? 'PAID' : 'FAILED',
    amount: amount || undefined,
    currency: String(pick(b, ['currency', 'Currency', 'currencyCode']) || 'THB'),
    pgTxnId: pgTxnId || undefined,
    ts: Date.now(),
  };
  const comp = cleanCompId(extra && extra.compId);
  if (comp) out.compId = comp;
  const trnId = extra && extra.trnId != null ? String(extra.trnId).trim() : '';
  if (trnId) out.trnId = trnId;
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
  paysoShouldNotifyMerchant,
  mapPaysoToMerchantNotifyBody,
};
