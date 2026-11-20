#!/usr/bin/env node
/**
 * NOTI server.js PG 공존 검증.
 * Usage: node verify-pg-coexistence.js <server.js> [required-pg-coexistence.json]
 * Exit 0 = OK, non-zero = missing markers (배포 거부).
 */
'use strict';

const fs = require('fs');
const path = require('path');

const serverPath = process.argv[2];
const listPath =
  process.argv[3] || path.join(__dirname, 'required-pg-coexistence.json');

if (!serverPath) {
  console.error('Usage: node verify-pg-coexistence.js <server.js> [required.json]');
  process.exit(1);
}

const s = fs.readFileSync(serverPath, 'utf8');
const cfg = JSON.parse(fs.readFileSync(listPath, 'utf8'));
const missing = [];

for (const needle of cfg.globalNeedles || []) {
  if (!s.includes(needle)) missing.push({ scope: 'global', needle });
}

for (const pg of cfg.required || []) {
  for (const needle of pg.needles || []) {
    if (!s.includes(needle)) missing.push({ scope: pg.code, needle });
  }
}

// getTransactionListLogs generic filter shape
const idx = s.indexOf('function getTransactionListLogs');
if (idx < 0) {
  missing.push({ scope: 'filter', needle: 'function getTransactionListLogs' });
} else {
  const fn = s.slice(idx, idx + 2500);
  if (!fn.includes('normalizeAdminPgSource(src)') || !fn.includes("known !== 'chillpay'")) {
    missing.push({
      scope: 'filter',
      needle: 'ADMIN_PG_LIST_FILTER_V1 generic shape',
    });
  }
}

if (missing.length) {
  console.error('PG_COEXISTENCE_FAIL');
  for (const m of missing) {
    console.error('MISSING', m.scope, JSON.stringify(m.needle));
  }
  process.exit(5);
}

const codes = (cfg.required || []).map((p) => p.code).join('+');
console.log('ADMIN_PG_LIST_FILTER_V1 ok');
console.log('PG_COEXISTENCE ' + codes + ' ok');
process.exit(0);
