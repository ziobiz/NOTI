#!/usr/bin/env node
/**
 * ICOPAY 가 PAYSO 를 /noti/omspay 로 보낸 오분류 정리:
 * - routeKey omspay/* + PAYSO(PS…) → payso/webhook 로 교정
 * - PAYSO_PENDING_EXPIRED 유령(실제 OMSPAY 진행 없음) → 삭제
 * 사용: node scripts/ops/purge-false-omspay-payso.js [path/to/pg-noti.log]
 */
'use strict';

const fs = require('fs');
const path = require('path');
const paysoNoti = require('../../lib/paysoNoti');

const logPath = path.resolve(process.argv[2] || path.join(__dirname, '..', '..', 'data', 'pg-noti.log'));

function isPendingExpired(body, rawBody) {
  let rb = {};
  const raw = String(rawBody || '');
  if (raw.trim().startsWith('{')) {
    try {
      rb = JSON.parse(raw);
    } catch (_) {}
  }
  const src = Object.assign({}, body || {}, rb);
  const orc = String(src.outcomeReasonCode || src.outcome_reason_code || '')
    .trim()
    .toUpperCase();
  return orc === 'PAYSO_PENDING_EXPIRED' || orc.startsWith('PAYSO_PENDING');
}

function main() {
  if (!fs.existsSync(logPath)) {
    console.error('missing', logPath);
    process.exit(1);
  }
  const raw = fs.readFileSync(logPath, 'utf8');
  const lines = raw.split(/\n/).filter((l) => l.trim());
  const out = [];
  let dropped = 0;
  let rewritten = 0;
  for (const line of lines) {
    let o;
    try {
      o = JSON.parse(line);
    } catch {
      out.push(line);
      continue;
    }
    const rk = String(o.routeKey || '');
    const pg = String(o.pgProvider || '').toLowerCase();
    const diskOms = pg === 'omspay' || rk.startsWith('omspay');
    const channel = paysoNoti.looksLikePaysoChannelNotify(o.body || {}, o.rawBody);
    if (!diskOms && !channel) {
      out.push(JSON.stringify(o));
      continue;
    }
    if (!channel && !diskOms) {
      out.push(JSON.stringify(o));
      continue;
    }
    if (channel && diskOms) {
      if (isPendingExpired(o.body, o.rawBody)) {
        /* /noti/omspay 로 들어온 PAYSO 만료 유령 — OMSPAY 진행 내역 아님, 삭제 */
        dropped += 1;
        continue;
      }
      /* 실거래 실패 등: OMSPAY 탭/Route 에 안 보이게 PAYSO 로 교정 */
      o.pgProvider = 'payso';
      o.routeKey = 'payso/webhook';
      rewritten += 1;
      out.push(JSON.stringify(o));
      continue;
    }
    if (channel && rk.startsWith('omspay')) {
      o.pgProvider = 'payso';
      o.routeKey = 'payso/webhook';
      rewritten += 1;
      out.push(JSON.stringify(o));
      continue;
    }
    out.push(JSON.stringify(o));
  }
  const bak = logPath + '.pre-purge-false-oms-' + Date.now() + '.bak';
  fs.copyFileSync(logPath, bak);
  fs.writeFileSync(logPath, out.join('\n') + (out.length ? '\n' : ''), 'utf8');
  console.log(
    JSON.stringify(
      {
        logPath,
        bak,
        droppedPendingExpired: dropped,
        rewrittenToPayso: rewritten,
        linesOut: out.length,
      },
      null,
      2,
    ),
  );
}

main();
