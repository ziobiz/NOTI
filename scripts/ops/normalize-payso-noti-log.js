#!/usr/bin/env node
/**
 * PAYSO 노티 로그 정규화 — ICOPAY 거래목록과 맞추기 위해:
 * - rawBody 기준으로 body 재매핑 (99/FAILED 유지, TransactionId=trnId)
 * - form+JSON 이중 통보 1건으로 합침 (PS… 우선)
 * - 운영 점검/스모크 건 제외
 * 사용: node scripts/ops/normalize-payso-noti-log.js [path/to/pg-noti.log]
 */
'use strict';

const fs = require('fs');
const path = require('path');
const paysoNoti = require('../../lib/paysoNoti');

const logPath = path.resolve(process.argv[2] || path.join(__dirname, '..', '..', 'data', 'pg-noti.log'));

function isProbe(o, body) {
  const blob = [
    body.TransactionId,
    body.trnId,
    body.orderNo,
    body.OrderNo,
    o.rawBody && String(o.rawBody).slice(0, 160),
  ]
    .map((x) => String(x || '').toUpperCase())
    .join(' ');
  return /HEALTHCHECK|DEDUP-TEST|DEDUP-ORDER|VERIFY001|VERIFYFAIL|PROBE1|COMMTEST|PAYSO-TEST|TEST-REF|TEST-PAYSO|PS-ALIAS|TESTICOPAYPROBE|SMOKEFAIL|SMOKECP|\bSMOKE\b/.test(
    blob,
  );
}

function score(o, body) {
  const tx = String(body.TransactionId || body.trnId || body.pgTxnId || '').trim();
  const order = String(body.orderNo || body.OrderNo || '').trim();
  const st = String(body.paymentStatus || '').toUpperCase();
  const status = String(body.status || '');
  const paid =
    (st === 'PAID' || status === '10') && st !== 'FAILED' && status !== '99';
  const truePaid = !!(paid && (body.paidAt || body.PaymentDate));
  let s = 0;
  /* paidAt 있는 성공이 이후 허위 FAILED 보다 우선 (ICOPAY 성공 3건 유지) */
  if (truePaid) s += 500;
  else if (paid) s += 200;
  else if (st === 'FAILED' || status === '99') s += 80;
  if (/^PS/i.test(tx)) s += 100;
  if (/^OM/i.test(order)) s += 50;
  else if (/^FS/i.test(order)) s += 40;
  if (body.paidAt || body.PaymentDate) s += 25;
  if (String(o.rawBody || '').trim().startsWith('{')) s += 10;
  const t = Date.parse(o.receivedAtIso || o.receivedAt || '') || 0;
  return s * 1e13 + t;
}

function clusterKey(o, body) {
  const tx = String(body.TransactionId || body.trnId || body.pgTxnId || '').trim();
  const order = String(body.orderNo || body.OrderNo || '').trim();
  const mid = String(o.merchantId || body.compId || '').trim();
  const amt = String(body.amount != null ? body.amount : body.Amount != null ? body.Amount : '').trim();
  const minute = String(o.receivedAtIso || o.receivedAt || '').slice(0, 16);
  /* ICOPAY 1주문=1행: OM/FS 주문번호를 PS·숫자 거래번호보다 우선 키로 사용 */
  if (/^(OM|FS)/i.test(order)) return 'ord:' + order;
  if (/^PS/i.test(tx)) return 'tx:' + tx.toLowerCase();
  if (mid && amt && minute) return 'fa:' + mid + '|' + amt + '|' + minute;
  if (tx) return 'tx:' + tx.toLowerCase();
  if (order) return 'ord:' + order;
  return 'id:' + (o.receivedAtIso || '') + ':' + Math.random();
}

function main() {
  if (!fs.existsSync(logPath)) {
    console.error('missing', logPath);
    process.exit(1);
  }
  const raw = fs.readFileSync(logPath, 'utf8');
  const lines = raw.split(/\n/).filter((l) => l.trim());
  const kept = [];
  const paysoByKey = new Map();
  let paysoIn = 0;
  let probes = 0;
  let remapped = 0;

  for (const line of lines) {
    let o;
    try {
      o = JSON.parse(line);
    } catch {
      kept.push(line);
      continue;
    }
    const pg = String(o.pgProvider || '').toLowerCase();
    const rk = String(o.routeKey || '');
    const channelPayso = paysoNoti.looksLikePaysoChannelNotify(o.body || {}, o.rawBody);
    const isPayso = pg === 'payso' || rk.startsWith('payso') || channelPayso;
    if (!isPayso) {
      kept.push(JSON.stringify(o));
      continue;
    }
    if (String(o.kind || '') === 'result') {
      /* result 는 거래목록에서 제외되므로 디스크에서도 생략해 ICOPAY 건수에 맞춤 */
      continue;
    }
    paysoIn += 1;
    const mapped = paysoNoti.mapPaysoToMerchantNotifyBody(o.body || {}, '', {
      compId: o.merchantId || (o.body && (o.body.compId || o.body.CompId)) || '',
      trnId: '',
    });
    const fromRaw = paysoNoti.enrichPaysoBodyForDisplay(o.body || {}, o.rawBody, o.contentType || '');
    const body = Object.assign({}, mapped, fromRaw, {
      paymentStatus: fromRaw.paymentStatus || mapped.paymentStatus,
      status: fromRaw.status != null ? fromRaw.status : mapped.status,
      PaymentStatus: fromRaw.PaymentStatus != null ? fromRaw.PaymentStatus : mapped.PaymentStatus,
      outcomeReasonCode: fromRaw.outcomeReasonCode || mapped.outcomeReasonCode,
      outcomeReason: fromRaw.outcomeReason || mapped.outcomeReason,
    });
    remapped += 1;
    if (isProbe(o, body)) {
      probes += 1;
      continue;
    }
    o.body = body;
    o.pgProvider = 'payso';
    if (!o.merchantId && body.compId) o.merchantId = String(body.compId);
    const key = clusterKey(o, body);
    const prev = paysoByKey.get(key);
    if (!prev || score(o, body) >= score(prev, prev.body || {})) {
      paysoByKey.set(key, o);
    }
  }

  /* form 잔여: 같은 가맹+금액+분이 PS/FS/OM 과 겹치면 제거 */
  const preferred = Array.from(paysoByKey.values());
  const strongFa = new Set();
  for (const o of preferred) {
    const b = o.body || {};
    const tx = String(b.TransactionId || b.trnId || '').trim();
    const order = String(b.orderNo || '').trim();
    if (!/^PS/i.test(tx) && !/^(OM|FS)/i.test(order)) continue;
    const mid = String(o.merchantId || b.compId || '').trim();
    const amt = String(b.amount || b.Amount || '').trim();
    const minute = String(o.receivedAtIso || '').slice(0, 16);
    if (mid && amt) strongFa.add(mid + '|' + amt + '|' + minute);
  }
  const paysoOut = [];
  for (const o of preferred) {
    const b = o.body || {};
    const tx = String(b.TransactionId || b.trnId || '').trim();
    const order = String(b.orderNo || '').trim();
    if (/^PS/i.test(tx) || /^(OM|FS)/i.test(order)) {
      paysoOut.push(o);
      continue;
    }
    const mid = String(o.merchantId || b.compId || '').trim();
    const amt = String(b.amount || b.Amount || '').trim();
    const minute = String(o.receivedAtIso || '').slice(0, 16);
    const fa = mid && amt ? mid + '|' + amt + '|' + minute : '';
    if (fa && strongFa.has(fa)) continue;
    paysoOut.push(o);
  }

  paysoOut.sort(
    (a, b) =>
      (Date.parse(a.receivedAtIso || a.receivedAt || '') || 0) -
      (Date.parse(b.receivedAtIso || b.receivedAt || '') || 0),
  );

  const outLines = kept.concat(paysoOut.map((o) => JSON.stringify(o)));
  const bak = logPath + '.pre-normalize-' + Date.now() + '.bak';
  fs.copyFileSync(logPath, bak);
  fs.writeFileSync(logPath, outLines.join('\n') + (outLines.length ? '\n' : ''), 'utf8');
  console.log(
    JSON.stringify(
      {
        logPath,
        bak,
        nonPaysoKept: kept.length,
        paysoIn,
        paysoOut: paysoOut.length,
        probesDropped: probes,
        remapped,
      },
      null,
      2,
    ),
  );
}

main();
