'use strict';

/**
 * ElementPay webhook IP allowlist — local Node assert (no mocha required).
 * Run: node test/elementpayWebhookIp.test.js
 */

const assert = require('assert');
const ip = require('../lib/elementpayWebhookIp');

function mockReq(peer, headers) {
  return {
    socket: { remoteAddress: peer },
    headers: headers || {},
    method: 'POST',
    originalUrl: '/noti/elementpay',
  };
}

function run() {
  const wl = ip.loadWhitelist({
    webhookIpWhitelist: {
      enabled: true,
      sandboxIps: ['3.0.36.253'],
      liveIps: ['3.1.29.20'],
      extraIps: [],
      trustedProxies: ['127.0.0.1', '::1'],
    },
  });

  assert.strictEqual(ip.normalizeIpAddress('::ffff:3.0.36.253'), '3.0.36.253');
  assert.strictEqual(ip.normalizeIpAddress('3.1.29.20:443'), '3.1.29.20');

  // Direct EP sandbox IP on socket → allow
  assert.strictEqual(ip.assertAllowed(mockReq('3.0.36.253', {}), wl, null), null);
  // Live IP → allow
  assert.strictEqual(ip.assertAllowed(mockReq('3.1.29.20', {}), wl, null), null);
  // Random public IP → 403
  const deny = ip.assertAllowed(mockReq('203.0.113.50', {}), wl, null);
  assert.ok(deny && deny.status === 403);

  // Trusted proxy + X-Real-IP sandbox → allow
  assert.strictEqual(
    ip.assertAllowed(mockReq('127.0.0.1', { 'x-real-ip': '3.0.36.253' }), wl, null),
    null,
  );
  // Untrusted peer must ignore spoofed X-Real-IP
  const spoof = ip.assertAllowed(mockReq('203.0.113.9', { 'x-real-ip': '3.0.36.253' }), wl, null);
  assert.ok(spoof && spoof.status === 403);

  // Disabled whitelist → allow anything
  const off = ip.loadWhitelist({ webhookIpWhitelist: { enabled: false } });
  assert.strictEqual(ip.assertAllowed(mockReq('203.0.113.50', {}), off, null), null);

  const allowed = ip.getAllowedIps(wl);
  assert.ok(allowed.includes('3.0.36.253'));
  assert.ok(allowed.includes('3.1.29.20'));

  console.log('elementpayWebhookIp.test.js OK');
}

run();
