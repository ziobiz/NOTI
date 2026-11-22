'use strict';
const fs = require('fs');
const path = require('path');
const root = process.argv[2] || path.join(__dirname, '..', '..');
const payso = require(path.join(root, 'lib', 'paysoNoti'));
const logPath = path.join(root, 'data', 'pg-noti.log');

function ymd(iso, tz) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: tz,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(new Date(iso));
}
function inR(d) {
  return !!(d && d >= '2026-11-22' && d <= '2026-11-23');
}
const probe = /HEALTHCHECK|DEDUP-TEST|VERIFY001|VERIFYFAIL|SMOKE/i;
const lines = fs.readFileSync(logPath, 'utf8').trim().split(/\n+/).filter(Boolean);
const byKey = new Map();
for (const line of lines) {
  let o;
  try {
    o = JSON.parse(line);
  } catch {
    continue;
  }
  const channel = payso.looksLikePaysoChannelNotify(o.body || {}, o.rawBody);
  const pg = String(o.pgProvider || '').toLowerCase();
  const rk = String(o.routeKey || '');
  if (!(pg === 'payso' || rk.startsWith('payso') || channel)) continue;
  if (String(o.kind || '') === 'result') continue;
  const b = payso.enrichPaysoBodyForDisplay(o.body || {}, o.rawBody);
  if (probe.test([b.TransactionId, b.orderNo, String(o.rawBody || '').slice(0, 80)].join(' '))) continue;
  const payY = String(b.paidAt || b.PaymentDate || '').slice(0, 10);
  const orderY = payso.paysoFsOrderCalendarYmd(b.orderNo || '');
  const orc = String(b.outcomeReasonCode || '').toUpperCase();
  const pending = orc === 'PAYSO_PENDING_EXPIRED' || orc.startsWith('PAYSO_PENDING');
  const recJp = ymd(o.receivedAtIso || '', 'Asia/Tokyo');
  const recTh = ymd(o.receivedAtIso || '', 'Asia/Bangkok');
  let ok = false;
  if (inR(payY) || inR(orderY)) ok = true;
  else if (payY || orderY) ok = false;
  else if (pending) ok = false;
  else if (inR(recJp) || inR(recTh)) ok = true;
  if (!ok) continue;
  const paid = payso.paysoIsPaid(b);
  const truePaid = paid && !!(b.paidAt || b.PaymentDate);
  const key = /^(OM|FS)/i.test(b.orderNo || '')
    ? 'ord:' + b.orderNo
    : 'tx:' + String(b.TransactionId || b.trnId || '').toLowerCase();
  const prev = byKey.get(key);
  const rec = {
    paid: truePaid || paid,
    truePaid,
    mid: o.merchantId,
    order: b.orderNo,
    tx: b.TransactionId || b.trnId,
    amt: b.amount,
    st: b.paymentStatus,
    day: payY || orderY || recJp,
  };
  if (!prev || (rec.truePaid && !prev.truePaid) || (rec.paid && !prev.paid)) byKey.set(key, rec);
}
const arr = [...byKey.values()].sort((a, b) => String(b.day + b.order).localeCompare(String(a.day + a.order)));
console.log('FINAL', arr.length, 'paid', arr.filter((x) => x.paid).length, 'fail', arr.filter((x) => !x.paid).length);
arr.forEach((r, i) =>
  console.log(String(i + 1).padStart(2), r.paid ? 'OK' : 'NG', r.day, r.mid, r.order, r.tx, r.amt, r.st),
);
