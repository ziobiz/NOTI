'use strict';
const fs = require('fs');
const path = require('path');
const root = process.argv[2] || path.join(__dirname, '..', '..');
const payso = require(path.join(root, 'lib', 'paysoNoti'));
const logPath = path.join(root, 'data', 'pg-noti.log');

function ymd(iso, tz) {
  try {
    return new Intl.DateTimeFormat('en-CA', {
      timeZone: tz,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).format(new Date(iso));
  } catch {
    return '';
  }
}

const lines = fs.readFileSync(logPath, 'utf8').trim().split(/\n+/).filter(Boolean);
const rows = [];
for (const line of lines) {
  let o;
  try {
    o = JSON.parse(line);
  } catch {
    continue;
  }
  const pg = String(o.pgProvider || '').toLowerCase();
  const rk = String(o.routeKey || '');
  const isOms = pg === 'omspay' || rk.startsWith('omspay');
  if (!isOms) continue;
  if (String(o.kind || '') === 'result') continue;
  const b = o.body || {};
  let rb = {};
  const raw = String(o.rawBody || '');
  if (raw.trim().startsWith('{')) {
    try {
      rb = JSON.parse(raw);
    } catch (_) {}
  }
  const iso = o.receivedAtIso || '';
  const jp = ymd(iso, 'Asia/Tokyo');
  const th = ymd(iso, 'Asia/Bangkok');
  if (!(jp === '2026-11-23' || th === '2026-11-23')) continue;
  const channel = payso.looksLikePaysoChannelNotify(b, o.rawBody);
  rows.push({
    jp,
    th,
    mid: o.merchantId || rb.compId || b.compId || '',
    order: rb.orderNo || b.orderNo || '',
    tx: rb.trnId || b.trnId || b.TransactionId || '',
    amt: rb.amount || b.amount || '',
    st: rb.paymentStatus || b.paymentStatus || '',
    orc: rb.outcomeReasonCode || b.outcomeReasonCode || '',
    ch: rb.notifyChannel || b.notifyChannel || '',
    route: rk,
    pg,
    channelPayso: channel,
    iso,
  });
}
rows.sort((a, b) => String(b.iso).localeCompare(String(a.iso)));
console.log('today omspay-labeled', rows.length, 'paysoChannel', rows.filter((r) => r.channelPayso).length);
rows.forEach((r, i) =>
  console.log(
    String(i + 1).padStart(2),
    r.jp,
    r.mid,
    r.order,
    r.tx,
    r.amt,
    r.st,
    r.orc || '-',
    'paysoCh=' + r.channelPayso,
    r.route,
  ),
);
