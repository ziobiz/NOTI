'use strict';
const path = require('path');
const L = require(path.join('/srv/pg-noti', 'locales'));
const locs = ['ko', 'ja', 'en', 'th', 'zh'];
const keys = [
  'merchants_nav_register_payso',
  'merchants_nav_register_tazapay',
  'merchants_nav_register_omspay',
  'merchants_nav_register_ksnet',
  'merchants_nav_register_qbang',
  'pg_provider_tazapay',
  'pg_provider_payso',
];
for (const loc of locs) {
  for (const k of keys) {
    const v = L.t(loc, k);
    const bad = v === k;
    console.log((bad ? 'MISSING' : 'ok') + ' ' + loc + ' ' + k + ' => ' + String(v).slice(0, 80));
  }
}
console.log('locales bytes', require('fs').statSync('/srv/pg-noti/locales.js').size);
console.log('has tazapay route', require('fs').readFileSync('/srv/pg-noti/server.js', 'utf8').includes("app.post('/noti/tazapay"));
