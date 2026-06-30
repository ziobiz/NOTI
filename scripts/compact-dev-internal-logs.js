#!/usr/bin/env node
/**
 * 개발 노티 로그(dev-internal-noti.log) 중복 정리
 *
 * 사용 예 (NOTI 서버 data 디렉터리):
 *   node scripts/compact-dev-internal-logs.js
 *   node scripts/compact-dev-internal-logs.js --order wc39302t232313 --order wc39302t231858
 *
 * 환경변수: DATA_DIR (기본: ./data)
 */
'use strict';

const fs = require('fs');
const path = require('path');
const {
  dedupeDevInternalLogObjects,
  purgeDevInternalLogsByOrderNos,
} = require('../lib/devInternalLogDedupe');

const dataDir = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
const logPath = path.join(dataDir, 'dev-internal-noti.log');

function parseArgs(argv) {
  const orderNos = [];
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === '--order' && argv[i + 1]) {
      orderNos.push(argv[++i]);
    }
  }
  return { orderNos };
}

function main() {
  const { orderNos } = parseArgs(process.argv);
  if (!fs.existsSync(logPath)) {
    console.error('파일 없음:', logPath);
    process.exit(1);
  }
  const raw = fs.readFileSync(logPath, 'utf8');
  const lines = raw.split('\n').filter((l) => l.trim().length > 0);
  const objs = [];
  for (const line of lines) {
    try {
      objs.push(JSON.parse(line));
    } catch (_) {}
  }
  const before = objs.length;
  let working = objs;
  let purgedByOrder = 0;
  if (orderNos.length) {
    const pr = purgeDevInternalLogsByOrderNos(working, orderNos, 1);
    working = pr.kept;
    purgedByOrder = pr.removed;
    console.log('orderNo 지정 정리:', orderNos.join(', '), '→ 제거', purgedByOrder, '건');
  }
  const deduped = dedupeDevInternalLogObjects(working);
  deduped.sort((a, b) => {
    const ta = Date.parse(a.storedAtIso || a.storedAt || '') || 0;
    const tb = Date.parse(b.storedAtIso || b.storedAt || '') || 0;
    return ta - tb;
  });
  const after = deduped.length;
  const backup = logPath + '.bak-' + Date.now();
  fs.copyFileSync(logPath, backup);
  fs.writeFileSync(logPath, deduped.map((o) => JSON.stringify(o)).join('\n') + (deduped.length ? '\n' : ''), 'utf8');
  console.log('백업:', backup);
  console.log('정리 완료:', before, '→', after, '(중복 제거', before - after, '건, orderNo 추가 제거', purgedByOrder, '건)');
}

main();
