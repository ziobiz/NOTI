'use strict';
const fs = require('fs');
const s = fs.readFileSync(require('path').join(__dirname, '..', '..', 'server.js'), 'utf8');
for (const n of [
  "merchantsRegisterLink('tazapay'",
  "merchantsRegisterLink('payso'",
  "app.post('/noti/tazapay",
  'tazapayUrl:',
]) {
  console.log(s.includes(n) ? 'OK' : 'MISSING', n);
}
