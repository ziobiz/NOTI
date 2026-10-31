/**
 * One-shot: SO-29452079 실패 로그 Comp-Id/가맹 복원 + 가맹 Callback 재전송.
 * Usage (prod): node scripts/restore-omspay-so-29452079.js
 */
'use strict';

const fs = require('fs');
const path = require('path');
const http = require('http');
const https = require('https');
const { URL } = require('url');

const ROOT = path.join(__dirname, '..');
const LOG_PATH = path.join(ROOT, 'data', 'pg-noti.log');
const MERCHANTS_PATH = path.join(ROOT, 'config', 'merchants.json');

const SO = 'SO-29452079';
const MERCHANT_ORDER = 'wc41799t065039';
const COMP_ID = '6000000054';
const OMS_SLOT = COMP_ID + '#omspay';

function loadMerchants() {
  const raw = JSON.parse(fs.readFileSync(MERCHANTS_PATH, 'utf8'));
  const map = raw && typeof raw === 'object' && !Array.isArray(raw) ? raw : {};
  return map;
}

function stampCompId(body, compId) {
  const out = body && typeof body === 'object' ? { ...body } : {};
  const id = String(compId || '').trim();
  if (id) {
    out.compId = id;
    out.CompId = id;
    out['Comp-Id'] = id;
    if (!out.merchantId || String(out.merchantId).includes('#')) out.merchantId = id;
  }
  return out;
}

function applyMerchantOrder(body, orderNo) {
  const out = { ...(body || {}) };
  const o = String(orderNo || '').trim();
  if (!o) return out;
  out.orderid = o;
  out.orderID = o;
  out.OrderNo = o;
  out.orderNo = o;
  out.note = o;
  return out;
}

function postJson(urlStr, bodyObj) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const lib = u.protocol === 'https:' ? https : http;
    const payload = JSON.stringify(bodyObj || {});
    const req = lib.request(
      {
        protocol: u.protocol,
        hostname: u.hostname,
        port: u.port || (u.protocol === 'https:' ? 443 : 80),
        path: u.pathname + (u.search || ''),
        method: 'POST',
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
          'Content-Length': Buffer.byteLength(payload),
          'User-Agent': 'ICOPAY-NOTI-resend/1.0',
        },
        timeout: 25000,
      },
      (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          resolve({
            status: res.statusCode || 0,
            data: Buffer.concat(chunks).toString('utf8').slice(0, 500),
          });
        });
      },
    );
    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('timeout'));
    });
    req.write(payload);
    req.end();
  });
}

function thailandNowString() {
  try {
    return new Intl.DateTimeFormat('en-GB', {
      timeZone: 'Asia/Bangkok',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    })
      .format(new Date())
      .replace(',', '');
  } catch (_) {
    return new Date().toISOString();
  }
}

async function main() {
  if (!fs.existsSync(LOG_PATH)) throw new Error('log missing: ' + LOG_PATH);
  const merchants = loadMerchants();
  const slot = merchants[OMS_SLOT] || merchants[COMP_ID];
  if (!slot) throw new Error('merchant missing: ' + OMS_SLOT + ' / ' + COMP_ID);
  const targetUrl = String(slot.callbackUrl || '').trim();
  if (!targetUrl) throw new Error('callbackUrl empty');

  const raw = fs.readFileSync(LOG_PATH, 'utf8');
  const lines = raw.split('\n');
  let patched = 0;
  let original = null;
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!line || !line.includes(SO) || !line.includes('omspay/webhook')) continue;
    let obj;
    try {
      obj = JSON.parse(line);
    } catch (_) {
      continue;
    }
    if (String(obj.routeKey || '') !== 'omspay/webhook') continue;
    if (!(String((obj.body && obj.body.so) || '') === SO || String((obj.body && obj.body.TransactionId) || '') === SO)) {
      continue;
    }
    if (!original) original = obj;
    const body = stampCompId(applyMerchantOrder(obj.body || {}, MERCHANT_ORDER), COMP_ID);
    body.TransactionId = SO;
    body.transaction_id = SO;
    body.so = SO;
    obj.merchantId = COMP_ID;
    obj.body = body;
    obj.targetUrl = targetUrl;
    if (String(obj.relayFailReason || '').includes('Missing X-Icopay-Comp-Id') || !obj.merchantId) {
      obj.relayStatus = obj.relayStatus === 'ok' ? 'ok' : 'fail';
      obj.restoredCompIdAt = new Date().toISOString();
      obj.restoredCompIdNote = 'ICOPAY_V5.92_header_fix_backfill';
    }
    lines[i] = JSON.stringify(obj);
    patched++;
  }
  if (!original) throw new Error('original log not found for ' + SO);
  if (patched < 1) throw new Error('no lines patched');

  const notifyBody = stampCompId(
    applyMerchantOrder(
      {
        returncode: '00',
        TransactionId: SO,
        transaction_id: SO,
        so: SO,
        pgOrderNo: SO,
        omsOrderCode: SO,
        amount: '222',
        Amount: '222',
        currency: 'THB',
        Currency: 'THB',
        paymentStatus: 'Succeeded',
        PaymentStatus: '0',
        chillPaymentStatus: 'Paid',
        outcome: 'success',
        pgKind: 'omspay',
        pgkind: 'OMSPAY',
        van: 'OMSPAY',
        method: 'waiting',
        oms_ref: String((original.body && original.body.oms_ref) || 'ch_3LRv1GVWDlvPWDprfIWG9M2WBER'),
      },
      MERCHANT_ORDER,
    ),
    COMP_ID,
  );

  console.log('POST', targetUrl);
  const relay = await postJson(targetUrl, notifyBody);
  const ok = relay.status >= 200 && relay.status < 400;
  console.log('relay HTTP', relay.status, (relay.data || '').slice(0, 200));

  const appendEntry = {
    receivedAt: thailandNowString(),
    receivedAtIso: new Date().toISOString(),
    routeKey: 'omspay/webhook',
    merchantId: COMP_ID,
    kind: 'callback',
    body: notifyBody,
    rawBody: original.rawBody,
    targetUrl,
    contentType: 'application/json',
    env: 'live',
    relayStatus: ok ? 'ok' : 'fail',
    relayFailReason: ok ? '' : 'HTTP ' + relay.status + ': ' + String(relay.data || '').slice(0, 200),
    relayFormatUsed: 'json',
    pgProvider: 'omspay',
    note: 'manual_resend_after_icopay_v592_compid_header_fix',
    manualAssignResend: true,
    merchantOrderNo: MERCHANT_ORDER,
  };
  lines.push(JSON.stringify(appendEntry));
  fs.writeFileSync(LOG_PATH, lines.join('\n'), 'utf8');
  console.log('patched lines=', patched, 'appended resend=', ok ? 'ok' : 'fail');
  if (!ok) process.exitCode = 2;
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
