/**
 * KSNET (해외카드 USD 결제창) webhook helpers —
 * ICOPAY recv_post 미러 JSON → …/KSNET → Comp-Id 가맹 릴레이.
 * 가맹 통보 본문은 통합 API 필드만 (PG명 미포함).
 * amt 는 USD×1000 스케일 → 가맹 amount 는 /1000.
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

/** KSNET amt: USD × 1000 → 표시/가맹용 USD 문자열 */
function ksnetAmtToUsd(amtRaw) {
  if (amtRaw == null || String(amtRaw).trim() === '') return '';
  const n = Number(amtRaw);
  if (!Number.isFinite(n)) return String(amtRaw).trim();
  const usd = n / 1000;
  if (!Number.isFinite(usd)) return String(amtRaw).trim();
  // 불필요한 trailing zero 정리
  const s = usd.toFixed(3).replace(/\.?0+$/, '');
  return s || '0';
}

function ksnetIsSuccess(body) {
  const b = body && typeof body === 'object' ? body : {};
  if (b.success === true || String(b.success || '').toLowerCase() === 'true') return true;
  if (b.success === false || String(b.success || '').toLowerCase() === 'false') return false;
  const authyn = String(pick(b, ['authyn', 'authYn', 'AUTHYN']) || '')
    .trim()
    .toUpperCase();
  if (authyn === 'O') return true;
  if (authyn === 'X') return false;
  const resultcd = String(pick(b, ['resultcd', 'resultCd', 'RESULTCD', 'result']) || '').trim();
  if (resultcd === '00' || resultcd === '0000') return true;
  const result = String(pick(b, ['result', 'Result']) || '')
    .trim()
    .toLowerCase();
  if (result === 'success' || result === 'ok' || result === 'paid') return true;
  if (result === 'fail' || result === 'failed' || result === 'error') return false;
  // 승인번호 있고 authyn 없으면 보수적으로 성공 취급하지 않음 — authyn 우선
  return authyn === 'O';
}

/** 가맹 릴레이 대상 여부 (미러 body 기준 — 기본적으로 시도) */
function ksnetShouldNotifyMerchant(body) {
  const b = body && typeof body === 'object' ? body : {};
  if (pick(b, ['trno', 'TrNo', 'TRNO', 'authno', 'AuthNo'])) return true;
  if (pick(b, ['ordno', 'orderNo', 'order', 'OrdNo'])) return true;
  if (pick(b, ['amt', 'amount', 'Amount'])) return true;
  return true;
}

/**
 * @param {object} evtBody KSNET/ICOPAY mirror JSON
 * @param {string} [merchantOrderNo] X-Icopay-Order-No
 * @param {object} [extra] { compId, trnId }
 */
function mapKsnetToMerchantNotifyBody(evtBody, merchantOrderNo, extra) {
  const b = evtBody && typeof evtBody === 'object' ? evtBody : {};
  const trno = String(pick(b, ['trno', 'TrNo', 'TRNO', 'transactionId']) || '').trim();
  const authno = String(pick(b, ['authno', 'AuthNo', 'AUTHNO']) || '').trim();
  const ordno = String(pick(b, ['ordno', 'OrdNo', 'ORDNO', 'orderNo', 'order']) || '').trim();
  const fromHdr = String(merchantOrderNo || '').trim();
  const merchantOrder = fromHdr || ordno || '';
  const amount = ksnetAmtToUsd(pick(b, ['amt', 'Amt', 'AMT', 'amount', 'Amount']));
  const paid = ksnetIsSuccess(b);
  const pgTxnId = trno || authno || '';
  const out = {
    event: 'pg.payment.status',
    orderNo: merchantOrder || '',
    status: paid ? '10' : '99',
    paymentStatus: paid ? 'PAID' : 'FAILED',
    amount: amount || undefined,
    currency: String(pick(b, ['currency', 'Currency', 'currencyCode']) || 'USD'),
    pgTxnId: pgTxnId || undefined,
    authNo: authno || undefined,
    resultCd: String(pick(b, ['resultcd', 'resultCd', 'RESULTCD']) || '').trim() || undefined,
    msg1: String(pick(b, ['msg1', 'Msg1']) || '').trim() || undefined,
    msg2: String(pick(b, ['msg2', 'Msg2']) || '').trim() || undefined,
    trddt: String(pick(b, ['trddt', 'TrdDt', 'TRDDT']) || '').trim() || undefined,
    trdtm: String(pick(b, ['trdtm', 'TrdTm', 'TRDTM']) || '').trim() || undefined,
    ts: Date.now(),
  };
  const comp = cleanCompId(extra && extra.compId);
  if (comp) out.compId = comp;
  const trnId = extra && extra.trnId != null ? String(extra.trnId).trim() : '';
  if (trnId) out.trnId = trnId;
  else if (trno) out.trnId = trno;
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
  ksnetAmtToUsd,
  ksnetIsSuccess,
  ksnetShouldNotifyMerchant,
  mapKsnetToMerchantNotifyBody,
};
