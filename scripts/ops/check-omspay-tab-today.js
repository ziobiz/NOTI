'use strict';
const fs = require('fs');
const path = require('path');
const root = process.argv[2] || path.join(__dirname, '..', '..');
const payso = require(path.join(root, 'lib', 'paysoNoti'));

function getNotiLogPgAcquirer(log) {
  if (payso.looksLikePaysoChannelNotify(log && log.body, log && log.rawBody)) return 'payso';
  const explicit = log && log.pgProvider != null ? String(log.pgProvider).toLowerCase().trim() : '';
  if (explicit === 'omspay' || explicit === 'oms' || explicit === 'waylar') return 'omspay';
  if (explicit === 'payso') return 'payso';
  const rk = log && log.routeKey != null ? String(log.routeKey) : '';
  if (rk.startsWith('omspay/')) return 'omspay';
  if (rk.startsWith('payso/')) return 'payso';
  return 'other';
}

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

const lines = fs.readFileSync(path.join(root, 'data', 'pg-noti.log'), 'utf8').trim().split(/\n+/).filter(Boolean);
const omsTab = [];
const diskOmsButPayso = [];
for (const line of lines) {
  let o;
  try {
    o = JSON.parse(line);
  } catch {
    continue;
  }
  if (String(o.kind || '') === 'result') continue;
  const iso = o.receivedAtIso || '';
  const jp = ymd(iso, 'Asia/Tokyo');
  const th = ymd(iso, 'Asia/Bangkok');
  if (!(jp === '2026-11-23' || th === '2026-11-23')) continue;
  const acq = getNotiLogPgAcquirer(o);
  const diskOms =
    String(o.pgProvider || '').toLowerCase() === 'omspay' || String(o.routeKey || '').startsWith('omspay');
  if (acq === 'omspay') {
    omsTab.push(o);
  }
  if (diskOms && acq === 'payso') {
    const b = o.body || {};
    diskOmsButPayso.push({
      order: b.orderNo,
      tx: b.trnId || b.TransactionId,
      amt: b.amount,
      pg: o.pgProvider,
      route: o.routeKey,
    });
  }
}
console.log('OMSPAY tab today count', omsTab.length);
console.log('disk-omspay but classified payso', diskOmsButPayso.length);
diskOmsButPayso.forEach((r, i) => console.log(String(i + 1).padStart(2), r.order, r.tx, r.amt, r.pg, r.route));
