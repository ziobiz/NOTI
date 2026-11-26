'use strict';
/**
 * 운영/로컬 locales.js 런타임 키 검증.
 * Deploy-Noti 게이트·배포 직후 원격 검증에 사용.
 * 키가 문자열 그대로 반환되면 실패 (UI에 merchants_nav_register_* 노출 방지).
 */
const fs = require('fs');
const path = require('path');

const localesPath = path.resolve(process.argv[2] || path.join(__dirname, '..', '..', 'locales.js'));
const serverPath = process.argv[3] ? path.resolve(process.argv[3]) : null;

if (!fs.existsSync(localesPath)) {
  console.error('FAIL locales missing:', localesPath);
  process.exit(1);
}

const L = require(localesPath);
const locs = ['ko', 'ja', 'en', 'th', 'zh'];
const requiredKeys = [
  'merchants_nav_register_chillpay',
  'merchants_nav_register_jpay',
  'merchants_nav_register_elementpay',
  'merchants_nav_register_ox',
  'merchants_nav_register_crypto',
  'merchants_nav_register_ilk',
  'merchants_nav_register_qxipay',
  'merchants_nav_register_omspay',
  'merchants_nav_register_payso',
  'merchants_nav_register_ksnet',
  'merchants_nav_register_tazapay',
  'merchants_nav_register_qbang',
  'merchants_payso_ingress_hint',
  'merchants_payso_result_hint',
  'merchants_tazapay_ingress_hint',
  'merchants_tazapay_result_hint',
  'pg_provider_payso',
  'pg_provider_tazapay',
  'pg_provider_omspay',
  'pg_provider_ksnet',
  'pg_provider_qbang',
];

let bad = 0;
for (const loc of locs) {
  for (const k of requiredKeys) {
    const v = L.t(loc, k);
    if (v == null || v === k || String(v).trim() === '') {
      console.error('MISSING', loc, k, '=>', v);
      bad++;
    }
  }
}

if (serverPath) {
  if (!fs.existsSync(serverPath)) {
    console.error('FAIL server missing:', serverPath);
    process.exit(1);
  }
  const s = fs.readFileSync(serverPath, 'utf8');
  const navNeedles = [
    "merchantsRegisterLink('payso'",
    "merchantsRegisterLink('tazapay'",
    "merchantsRegisterLink('omspay'",
    "merchantsRegisterLink('ksnet'",
    "merchantsRegisterLink('qbang'",
    "app.post('/noti/payso",
    "app.post('/noti/tazapay",
  ];
  for (const n of navNeedles) {
    if (!s.includes(n)) {
      console.error('SERVER_MISSING', n);
      bad++;
    }
  }
}

if (bad) {
  console.error('LOCALES_RUNTIME_FAIL', bad);
  process.exit(1);
}
console.log('LOCALES_RUNTIME_OK', path.basename(localesPath), requiredKeys.length, 'keys x', locs.length);
process.exit(0);
