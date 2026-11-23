'use strict';
const fs = require('fs');
const remote = fs.readFileSync('/srv/pg-noti/locales.js', 'utf8');
const keys = [
  'merchants_nav_register_payso',
  'merchants_nav_register_tazapay',
  'merchants_nav_register_ksnet',
  'merchants_nav_register_omspay',
  'merchants_nav_register_qbang',
  'pg_provider_tazapay',
  'pg_provider_payso',
  'merchants_tazapay_ingress_hint',
  'merchants_empty_tazapay',
  'cr_filter_pg_tazapay',
];
for (const k of keys) {
  const n = (remote.match(new RegExp(k.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g')) || []).length;
  console.log(n + '\t' + k);
}
console.log('mtime bytes', fs.statSync('/srv/pg-noti/locales.js').mtime.toISOString(), fs.statSync('/srv/pg-noti/locales.js').size);
