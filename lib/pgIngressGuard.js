/**
 * PG → NOTI 고정 입구 폭주 가드.
 * 동일 주문 재시도가 ICOPAY/이벤트루프를 막지 않도록:
 * - in-flight / 성공 디듀프는 즉시 200 ACK
 * - 동시 ICOPAY forward 상한
 * - ICOPAY 연속 실패 시 서킷 오픈(빠른 실패)
 */
'use strict';

const DEFAULTS = {
  successTtlMs: Math.max(60_000, parseInt(process.env.PG_INGRESS_DEDUP_SUCCESS_TTL_MS || '900000', 10) || 900_000),
  inflightTtlMs: Math.max(5_000, parseInt(process.env.PG_INGRESS_DEDUP_INFLIGHT_TTL_MS || '120000', 10) || 120_000),
  failTtlMs: Math.max(5_000, parseInt(process.env.PG_INGRESS_DEDUP_FAIL_TTL_MS || '30000', 10) || 30_000),
  maxConcurrentPerPg: Math.max(1, parseInt(process.env.PG_INGRESS_MAX_CONCURRENT || '8', 10) || 8),
  circuitFailThreshold: Math.max(3, parseInt(process.env.PG_INGRESS_CIRCUIT_FAILS || '8', 10) || 8),
  circuitOpenMs: Math.max(5_000, parseInt(process.env.PG_INGRESS_CIRCUIT_OPEN_MS || '45000', 10) || 45_000),
  maxEntries: Math.max(1000, parseInt(process.env.PG_INGRESS_DEDUP_MAX_ENTRIES || '20000', 10) || 20_000),
};

/** @type {Map<string, { status: string, at: number, httpStatus?: number }>} */
const dedupMap = new Map();
/** @type {Map<string, number>} */
const concurrentMap = new Map();
/** @type {Map<string, { fails: number, openUntil: number }>} */
const circuitMap = new Map();

function now() {
  return Date.now();
}

function cleanupExpired(ttlRefMs) {
  if (dedupMap.size < DEFAULTS.maxEntries) {
    // light cleanup occasionally
    if (dedupMap.size > 0 && dedupMap.size % 200 === 0) {
      const t = now();
      for (const [k, v] of dedupMap) {
        const ttl =
          v.status === 'ok'
            ? DEFAULTS.successTtlMs
            : v.status === 'inflight'
              ? DEFAULTS.inflightTtlMs
              : DEFAULTS.failTtlMs;
        if (t - v.at > ttl) dedupMap.delete(k);
      }
    }
    return;
  }
  const t = now();
  for (const [k, v] of dedupMap) {
    const ttl =
      v.status === 'ok'
        ? DEFAULTS.successTtlMs
        : v.status === 'inflight'
          ? DEFAULTS.inflightTtlMs
          : DEFAULTS.failTtlMs;
    if (t - v.at > Math.min(ttl, ttlRefMs || ttl)) dedupMap.delete(k);
  }
  // hard cap: drop oldest
  if (dedupMap.size > DEFAULTS.maxEntries) {
    const overflow = dedupMap.size - DEFAULTS.maxEntries;
    let i = 0;
    for (const k of dedupMap.keys()) {
      dedupMap.delete(k);
      i += 1;
      if (i >= overflow) break;
    }
  }
}

function buildKey(pg, kind, parts) {
  const pgK = String(pg || 'pg').toLowerCase().trim() || 'pg';
  const kindK = String(kind || 'webhook').toLowerCase().trim() || 'webhook';
  const id = (parts || [])
    .map((p) => String(p == null ? '' : p).trim())
    .filter(Boolean)
    .join('|');
  return pgK + ':' + kindK + ':' + (id || '_empty');
}

/**
 * @returns {{ action: 'process'|'ack'|'reject', reason: string, retryAfterSec?: number }}
 */
function beginInbound(pg, kind, parts) {
  cleanupExpired();
  const key = buildKey(pg, kind, parts);
  const t = now();
  const prev = dedupMap.get(key);
  if (prev) {
    if (prev.status === 'ok' && t - prev.at <= DEFAULTS.successTtlMs) {
      return { action: 'ack', reason: 'dedup_ok', key, prevHttpStatus: prev.httpStatus || 200 };
    }
    if (prev.status === 'inflight' && t - prev.at <= DEFAULTS.inflightTtlMs) {
      return { action: 'ack', reason: 'dedup_inflight', key, prevHttpStatus: 200 };
    }
    if (prev.status === 'fail' && t - prev.at <= DEFAULTS.failTtlMs) {
      return {
        action: 'reject',
        reason: 'dedup_fail_cooldown',
        key,
        retryAfterSec: Math.max(1, Math.ceil((DEFAULTS.failTtlMs - (t - prev.at)) / 1000)),
      };
    }
  }

  const circuit = circuitMap.get(String(pg || '')) || { fails: 0, openUntil: 0 };
  if (circuit.openUntil && t < circuit.openUntil) {
    return {
      action: 'reject',
      reason: 'circuit_open',
      key,
      retryAfterSec: Math.max(1, Math.ceil((circuit.openUntil - t) / 1000)),
    };
  }

  const concKey = String(pg || 'pg');
  const conc = concurrentMap.get(concKey) || 0;
  if (conc >= DEFAULTS.maxConcurrentPerPg) {
    return {
      action: 'reject',
      reason: 'max_concurrent',
      key,
      retryAfterSec: 2,
    };
  }

  concurrentMap.set(concKey, conc + 1);
  dedupMap.set(key, { status: 'inflight', at: t });
  return { action: 'process', reason: 'new', key };
}

function endInbound(pg, key, result) {
  const concKey = String(pg || 'pg');
  const conc = concurrentMap.get(concKey) || 0;
  if (conc <= 1) concurrentMap.delete(concKey);
  else concurrentMap.set(concKey, conc - 1);

  const ok = !!(result && result.ok);
  const httpStatus = result && result.httpStatus != null ? Number(result.httpStatus) : ok ? 200 : 502;
  if (key) {
    dedupMap.set(String(key), {
      status: ok ? 'ok' : 'fail',
      at: now(),
      httpStatus: Number.isFinite(httpStatus) ? httpStatus : ok ? 200 : 502,
    });
  }

  const c = circuitMap.get(concKey) || { fails: 0, openUntil: 0 };
  if (ok) {
    c.fails = 0;
    c.openUntil = 0;
  } else {
    c.fails += 1;
    if (c.fails >= DEFAULTS.circuitFailThreshold) {
      c.openUntil = now() + DEFAULTS.circuitOpenMs;
      c.fails = 0;
      console.warn('[pgIngressGuard] circuit OPEN', concKey, 'ms=', DEFAULTS.circuitOpenMs);
    }
  }
  circuitMap.set(concKey, c);
}

function markOk(pg, kind, parts, httpStatus) {
  const key = buildKey(pg, kind, parts);
  dedupMap.set(key, { status: 'ok', at: now(), httpStatus: httpStatus || 200 });
  return key;
}

function stats() {
  const by = { ok: 0, inflight: 0, fail: 0 };
  for (const v of dedupMap.values()) {
    if (v.status === 'ok') by.ok += 1;
    else if (v.status === 'inflight') by.inflight += 1;
    else by.fail += 1;
  }
  const circuits = {};
  for (const [k, v] of circuitMap) {
    circuits[k] = { fails: v.fails, open: v.openUntil > now(), openUntil: v.openUntil || 0 };
  }
  const concurrent = {};
  for (const [k, v] of concurrentMap) concurrent[k] = v;
  return { dedup: by, size: dedupMap.size, concurrent, circuits, defaults: { ...DEFAULTS } };
}

module.exports = {
  DEFAULTS,
  buildKey,
  beginInbound,
  endInbound,
  markOk,
  stats,
};
