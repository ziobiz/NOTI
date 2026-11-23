'use strict';
const fs = require('fs');
const path = require('path');
const root = '/srv/pg-noti';
const L = require(path.join(root, 'locales'));
const server = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
const keys = [
  'merchants_nav_register_payso',
  'merchants_nav_register_tazapay',
  'merchants_nav_register_omspay',
  'merchants_nav_register_ksnet',
  'merchants_nav_register_qbang',
  'pg_provider_tazapay',
  'pg_provider_payso',
];
let bad = 0;
for (const loc of ['ko', 'ja', 'en', 'th', 'zh']) {
  for (const k of keys) {
    const v = L.t(loc, k);
    if (v === k) {
      console.log('MISSING', loc, k);
      bad++;
    }
  }
}
const needles = [
  "merchantsRegisterLink('payso'",
  "merchantsRegisterLink('tazapay'",
  "merchantsRegisterLink('omspay'",
  "merchantsRegisterLink('ksnet'",
  "app.post('/noti/tazapay",
  "app.post('/noti/payso",
  'tazapayUrl:',
  'paysoUrl:',
];
for (const n of needles) {
  if (!server.includes(n)) {
    console.log('SERVER_MISSING', n);
    bad++;
  } else {
    console.log('SERVER_OK', n);
  }
}
console.log(
  'locales',
  fs.statSync(path.join(root, 'locales.js')).size,
  fs.statSync(path.join(root, 'locales.js')).mtime.toISOString(),
);
if (bad) {
  console.log('FAIL', bad);
  process.exit(1);
}
console.log('UI_KEYS_OK payso+tazapay+omspay+ksnet+qbang');
process.exit(0);
