/**
 * OMSPAY (Waylar OMS) webhook helpers — order-paid → ICOPAY …/OMSPAY → Comp-Id merchant relay.
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

function omspayShouldNotifyMerchant(body) {
  if (!body || typeof body !== 'object') return true;
  if (body.is_paid === true || body.is_paid === 'true' || body.is_paid === 1) return true;
  const ev = String(pick(body, ['event', 'type', 'event_type']) || '')
    .trim()
    .toLowerCase();
  if (!ev) return true;
  return ev.includes('order-paid') || ev.includes('paid') || ev.includes('payment');
}

function mapOmspayToMerchantNotifyBody(evtBody) {
  const b = evtBody && typeof evtBody === 'object' ? evtBody : {};
  /* note = ICOPAY orderNo; do not use Beam charge id (ref1) as orderid */
  const order = String(pick(b, ['note', 'orderNo', 'order_no']) || '').trim();
  const so = String(pick(b, ['so', 'code']) || '').trim();
  const paid =
    b.is_paid === true ||
    b.is_paid === 'true' ||
    b.is_paid === 1 ||
    (b.paid_at != null && String(b.paid_at).trim() !== '') ||
    String(pick(b, ['event', 'type']) || '')
      .toLowerCase()
      .includes('paid');
  const amount = pick(b, ['amount', 'total', 'paid_amount']);
  return {
    returncode: paid ? '00' : '99',
    orderid: order || so,
    transaction_id: so,
    amount: amount != null ? String(amount) : '',
    currency: String(pick(b, ['currency', 'ccy']) || ''),
    pgkind: 'OMSPAY',
    van: 'OMSPAY',
    payment_ref: String(pick(b, ['ref1', 'payment_id']) || '').trim(),
  };
}

module.exports = {
  pick,
  headerGetIgnoreCase,
  omspayShouldNotifyMerchant,
  mapOmspayToMerchantNotifyBody,
};
