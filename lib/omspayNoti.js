/**
 * OMSPAY (Waylar OMS) webhook helpers —
 * order-paid → ICOPAY …/OMSPAY → Comp-Id 가맹 릴레이.
 * 서명(x-waylar-signature) 검증은 ICOPAY OmspayCallbackService 담당.
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

/** Waylar order-paid 등 — 가맹 릴레이 대상 */
function omspayShouldNotifyMerchant(body) {
  const b = body && typeof body === 'object' ? body : {};
  const so = String(pick(b, ['so', 'So']) || '').trim();
  if (so.startsWith('SO-') || so.startsWith('so-')) return true;
  if (pick(b, ['paid_at', 'paidAt'])) return true;
  const st = String(pick(b, ['status', 'Status', 'state']) || '')
    .trim()
    .toLowerCase();
  if (st === 'paid' || st === 'success' || st === 'succeeded' || st === 'completed') return true;
  // 빈 이벤트 타입이면 기본적으로 릴레이 시도 (ICOPAY가 서명·매칭 판단)
  return true;
}

function mapOmspayToMerchantNotifyBody(evtBody) {
  const b = evtBody && typeof evtBody === 'object' ? evtBody : {};
  const so = String(pick(b, ['so', 'So']) || '').trim();
  const order = String(pick(b, ['note', 'orderNo', 'order', 'orderid', 'ref1']) || '').trim();
  const amountRaw = pick(b, ['amount', 'Amount', 'paid_amount']);
  let amount = '';
  if (amountRaw != null && String(amountRaw).trim() !== '') {
    const n = Number(amountRaw);
    amount = Number.isFinite(n) ? String(n) : String(amountRaw).trim();
  }
  const paid =
    !!pick(b, ['paid_at', 'paidAt']) ||
    String(pick(b, ['status', 'Status']) || '')
      .toLowerCase()
      .match(/paid|success|succeed|complete/) != null;
  return {
    returncode: paid ? '00' : '99',
    orderid: order || so,
    transaction_id: so || String(pick(b, ['ref1', 'id']) || '').trim(),
    amount,
    currency: String(pick(b, ['currency', 'Currency']) || 'THB'),
    payer_name: String(pick(b, ['payer_name', 'payerName']) || '').trim(),
    pgkind: 'OMSPAY',
    van: 'OMSPAY',
  };
}

module.exports = {
  pick,
  headerGetIgnoreCase,
  omspayShouldNotifyMerchant,
  mapOmspayToMerchantNotifyBody,
};
