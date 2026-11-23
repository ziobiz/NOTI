#!/bin/bash
set -e
cd /srv/pg-noti
if [ ! -f config/tazapay-ingress.json ]; then
  if [ -f config/omspay-ingress.json ]; then
    node <<'NODE'
const fs = require('fs');
const oms = JSON.parse(fs.readFileSync('config/omspay-ingress.json', 'utf8'));
const url = String(oms.icopayNotifyUrl || '')
  .replace(/\/OMSPAY\b/i, '/TAZAPAY')
  .replace(/\/PAYSO\b/i, '/TAZAPAY')
  .replace(/\/KSNET\b/i, '/TAZAPAY');
const out = {
  enabled: true,
  icopayNotifyUrl: url,
  icopayOrderLookupUrl: String(oms.icopayOrderLookupUrl || ''),
  webhookSecret: '',
  requireSignature: false,
  timeoutMs: 15000,
  note: 'auto-created from omspay; set webhookSecret when ready',
};
fs.writeFileSync('config/tazapay-ingress.json', JSON.stringify(out, null, 2));
console.log('created tazapay-ingress.json from omspay');
NODE
  else
    cp config/tazapay-ingress.example.json config/tazapay-ingress.json
    echo 'created tazapay-ingress.json from example'
  fi
else
  echo 'tazapay-ingress.json already exists (kept)'
fi
node scripts/deploy/verify-pg-coexistence.js server.js
test -f lib/tazapayNoti.js
test -f lib/tazapayIngress.js
grep -F "app.post('/noti/omspay" server.js >/dev/null
grep -F "app.post('/noti/payso" server.js >/dev/null
grep -F "app.post('/noti/ksnet" server.js >/dev/null
grep -F "app.post('/noti/qbang" server.js >/dev/null
grep -F "app.post('/noti/tazapay" server.js >/dev/null
echo OK_COEXIST_omspay+payso+ksnet+qbang+tazapay
