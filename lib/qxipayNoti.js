/**
 * QXIPAY gateway webhook helpers — charge.succeeded → ICOPAY …/QXIPAY → Comp-Id 가맹 릴레이.
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

function qxipayShouldNotifyMerchant(eventType, body) {
  const t = String(eventType || pick(body, ['type']) || '')
    .trim()
    .toLowerCase();
  if (!t) return true;
  if (t === 'charge.succeeded' || t === 'charge.failed' || t === 'charge.expired') return true;
  return t.startsWith('charge.');
}

function mapQxipayToMerchantNotifyBody(evtBody) {
  const b = evtBody && typeof evtBody === 'object' ? evtBody : {};
  const charge = (b.data && b.data.object) || b.data || b;
  const order = String(pick(charge, ['reference', 'orderNo', 'order']) || '').trim();
  const chargeId = String(pick(charge, ['id']) || '').trim();
  const status = String(pick(charge, ['status']) || pick(b, ['type']) || '')
    .trim()
    .toLowerCase();
  const paid =
    status === 'succeeded' ||
    status === 'paid' ||
    String(pick(b, ['type']) || '')
      .toLowerCase()
      .includes('succeeded');
  const amountMinor = Number(pick(charge, ['amount']) || 0);
  const amountMajor = Number.isFinite(amountMinor) ? (amountMinor / 100).toFixed(2) : '';
  return {
    returncode: paid ? '00' : '99',
    orderid: order,
    transaction_id: chargeId,
    amount: amountMajor,
    currency: String(pick(charge, ['currency']) || 'THB'),
    pgkind: 'QXIPAY',
    van: 'QXIPAY',
  };
}

module.exports = {
  pick,
  headerGetIgnoreCase,
  qxipayShouldNotifyMerchant,
  mapQxipayToMerchantNotifyBody,
};
