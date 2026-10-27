'use strict';

/**
 * ElementPay → NOTI webhook IP allowlist (Cabinet-published Sandbox/Live IPs).
 * Trusted-proxy aware: X-Real-IP / X-Forwarded-For only when TCP peer is trusted.
 */

const fs = require('fs');
const path = require('path');

const DEFAULT_SANDBOX_IPS = ['3.0.36.253'];
const DEFAULT_LIVE_IPS = ['3.1.29.20'];
const DEFAULT_TRUSTED_PROXIES = ['127.0.0.1', '::1', '::ffff:127.0.0.1'];

function parseIpList(raw) {
  if (Array.isArray(raw)) {
    return raw.map((x) => String(x || '').trim()).filter(Boolean);
  }
  return String(raw || '')
    .split(/[\s,;]+/)
    .map((x) => x.trim())
    .filter(Boolean);
}

function normalizeIpAddress(ip) {
  let s = String(ip || '').trim().toLowerCase();
  if (!s) return '';
  if (s.startsWith('::ffff:')) s = s.slice(7);
  if (s.startsWith('[') && s.endsWith(']')) s = s.slice(1, -1);
  if (/^\d{1,3}(\.\d{1,3}){3}:\d+$/.test(s)) s = s.split(':')[0];
  return s;
}

function ipInExactList(ip, list) {
  const n = normalizeIpAddress(ip);
  if (!n || !Array.isArray(list) || list.length === 0) return false;
  return list.some((x) => normalizeIpAddress(x) === n);
}

function loadWhitelist(fromFile) {
  const w = fromFile && typeof fromFile.webhookIpWhitelist === 'object' ? fromFile.webhookIpWhitelist : {};
  const sandboxIps = parseIpList(w.sandboxIps != null ? w.sandboxIps : DEFAULT_SANDBOX_IPS);
  const liveIps = parseIpList(w.liveIps != null ? w.liveIps : DEFAULT_LIVE_IPS);
  const extraIps = parseIpList(w.extraIps != null ? w.extraIps : []);
  const trustedProxies = parseIpList(
    w.trustedProxies != null ? w.trustedProxies : DEFAULT_TRUSTED_PROXIES,
  );
  return {
    enabled: w.enabled !== false,
    sandboxIps: sandboxIps.length ? sandboxIps : DEFAULT_SANDBOX_IPS.slice(),
    liveIps: liveIps.length ? liveIps : DEFAULT_LIVE_IPS.slice(),
    extraIps,
    trustedProxies: trustedProxies.length ? trustedProxies : DEFAULT_TRUSTED_PROXIES.slice(),
  };
}

function mergeWhitelistPatch(curWl, patchWl) {
  const cur = curWl || loadWhitelist({});
  const p = patchWl && typeof patchWl === 'object' ? patchWl : null;
  const next = {
    enabled: p && p.enabled !== undefined ? !!p.enabled : cur.enabled !== false,
    sandboxIps: p && p.sandboxIps !== undefined ? parseIpList(p.sandboxIps) : cur.sandboxIps.slice(),
    liveIps: p && p.liveIps !== undefined ? parseIpList(p.liveIps) : cur.liveIps.slice(),
    extraIps: p && p.extraIps !== undefined ? parseIpList(p.extraIps) : cur.extraIps.slice(),
    trustedProxies:
      p && p.trustedProxies !== undefined ? parseIpList(p.trustedProxies) : cur.trustedProxies.slice(),
  };
  if (!next.sandboxIps.length) next.sandboxIps = DEFAULT_SANDBOX_IPS.slice();
  if (!next.liveIps.length) next.liveIps = DEFAULT_LIVE_IPS.slice();
  if (!next.trustedProxies.length) next.trustedProxies = DEFAULT_TRUSTED_PROXIES.slice();
  return next;
}

function getAllowedIps(wl) {
  const w = wl || loadWhitelist({});
  const set = new Set();
  for (const ip of [].concat(w.sandboxIps || [], w.liveIps || [], w.extraIps || [])) {
    const n = normalizeIpAddress(ip);
    if (n) set.add(n);
  }
  return Array.from(set);
}

function resolveClientIp(req, trustedProxies) {
  const sock =
    (req.socket && req.socket.remoteAddress) ||
    (req.connection && req.connection.remoteAddress) ||
    '';
  const peer = normalizeIpAddress(sock);
  const trusted = Array.isArray(trustedProxies) ? trustedProxies : DEFAULT_TRUSTED_PROXIES;
  const peerTrusted = !peer || ipInExactList(peer, trusted);
  if (!peerTrusted) return { clientIp: peer, peerIp: peer, viaProxy: false, source: 'socket' };

  const xReal = normalizeIpAddress((req.headers && req.headers['x-real-ip']) || '');
  if (xReal) return { clientIp: xReal, peerIp: peer, viaProxy: true, source: 'x-real-ip' };

  const xff = (req.headers && req.headers['x-forwarded-for']) || '';
  const first = normalizeIpAddress(String(xff).split(',')[0] || '');
  if (first) return { clientIp: first, peerIp: peer, viaProxy: true, source: 'x-forwarded-for' };

  return { clientIp: peer, peerIp: peer, viaProxy: true, source: 'socket-fallback' };
}

function appendRejectLog(logPath, entry) {
  try {
    const dir = path.dirname(logPath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    const line = JSON.stringify({
      at: new Date().toISOString(),
      path: entry.path || '',
      clientIp: entry.clientIp || '',
      peerIp: entry.peerIp || '',
      source: entry.source || '',
      method: entry.method || '',
      ua: entry.ua || '',
    });
    fs.appendFileSync(logPath, line + '\n', 'utf8');
  } catch (e) {
    console.warn('[ElementPay] IP reject log write failed', e.message || e);
  }
}

/**
 * @returns {null|{status:number,body:string,resolved?:object}} null = allow
 */
function assertAllowed(req, wl, rejectLogPath) {
  const w = wl || loadWhitelist({});
  if (!w.enabled) return null;
  const resolved = resolveClientIp(req, w.trustedProxies);
  const allowed = getAllowedIps(w);
  if (ipInExactList(resolved.clientIp, allowed)) return null;
  const pathname = (req.originalUrl || req.url || req.path || '').split('?')[0];
  console.warn(
    '[ElementPay] webhook IP rejected',
    'ip=',
    resolved.clientIp || '-',
    'peer=',
    resolved.peerIp || '-',
    'path=',
    pathname,
    'src=',
    resolved.source,
  );
  if (rejectLogPath) {
    appendRejectLog(rejectLogPath, {
      path: pathname,
      clientIp: resolved.clientIp,
      peerIp: resolved.peerIp,
      source: resolved.source,
      method: req.method || '',
      ua: (req.headers && req.headers['user-agent']) || '',
    });
  }
  return {
    status: 403,
    body: 'Forbidden: ElementPay webhook IP not allowed',
    resolved,
  };
}

module.exports = {
  DEFAULT_SANDBOX_IPS,
  DEFAULT_LIVE_IPS,
  DEFAULT_TRUSTED_PROXIES,
  parseIpList,
  normalizeIpAddress,
  loadWhitelist,
  mergeWhitelistPatch,
  getAllowedIps,
  resolveClientIp,
  assertAllowed,
};
