/**
 * ICOPAY pg-notify 등 개발 노티(JSON POST) 전송 정책: 재시도 판별, 백오프+지터, 멱등 키 직렬화.
 */
'use strict';

const crypto = require('crypto');

function stableStringify(value) {
  if (value === null || value === undefined) return JSON.stringify(value);
  const t = typeof value;
  if (t === 'number' || t === 'boolean' || t === 'string') return JSON.stringify(value);
  if (Array.isArray(value)) return '[' + value.map(stableStringify).join(',') + ']';
  if (t === 'object') {
    const keys = Object.keys(value).sort();
    return '{' + keys.map((k) => JSON.stringify(k) + ':' + stableStringify(value[k])).join(',') + '}';
  }
  return JSON.stringify(String(value));
}

function buildIdempotencyKey(url, payload) {
  const u = String(url || '').trim();
  return crypto.createHash('sha256').update(u + '\n' + stableStringify(payload)).digest('hex');
}

/**
 * HTTP/전송 결과 기준 재시도 여부 (본문 success/processed/received 는 이미 success 플래그에 반영된 뒤 호출)
 * ICOPAY 계약: 422 기본 retryable true — 본문에 retryable:false 가 오면 재시도 안 함.
 * @param {{ success: boolean, status?: number, retryable?: boolean }} result
 * @param {{ retryOn422?: boolean, retryOnBodyFailure?: boolean }} opts
 */
function shouldRetryDelivery(result, opts) {
  const o = opts || {};
  if (result.success) return { retry: false, reason: 'ok' };
  const st = result.status == null || result.status === '' ? 0 : Number(result.status);
  if (Number.isNaN(st) || st === 0) return { retry: true, reason: 'network_or_timeout' };
  if (st >= 500 && st < 600) return { retry: true, reason: 'server_5xx' };
  if (st === 408 || st === 429) return { retry: true, reason: 'retryable_client' };
  if (st === 422) {
    if (result.retryable === false) return { retry: false, reason: 'icopay_retryable_false' };
    if (o.retryOn422 === false) return { retry: false, reason: '422_disabled' };
    return { retry: true, reason: '422_icopay' };
  }
  if (st >= 400 && st < 500) return { retry: false, reason: 'client_' + st };
  if (st >= 200 && st < 300) {
    if (o.retryOnBodyFailure) return { retry: true, reason: 'body_rejected' };
    return { retry: false, reason: 'body_rejected_terminal' };
  }
  return { retry: false, reason: 'other' };
}

/**
 * @param {number} attemptAfterFirst 0 = 첫 재시도 전 대기, 1 = 두 번째 재시도 전 …
 */
function computeBackoffMs(attemptAfterFirst, cfg) {
  const baseMs = Math.max(1, cfg.backoffBaseMs || 1000);
  const maxMs = Math.max(baseMs, cfg.backoffMaxMs || 120000);
  const jitterRatio = Math.min(0.5, Math.max(0, cfg.jitterRatio == null ? 0.2 : cfg.jitterRatio));
  const exp = Math.min(maxMs, baseMs * Math.pow(2, Math.max(0, attemptAfterFirst)));
  const jitter = maxMs * jitterRatio * Math.random();
  return Math.min(maxMs, Math.floor(exp + jitter));
}

function terminalDeliveryState(lastResult, cfg, attempts, elapsedMs) {
  if (lastResult.success) return 'DELIVERED';
  const r = shouldRetryDelivery(lastResult, cfg);
  if (!r.retry) return 'FAILED';
  if (attempts >= cfg.maxAttempts) return 'DEAD_LETTER';
  if (elapsedMs >= cfg.totalDeadlineMs) return 'DEAD_LETTER';
  return 'PENDING';
}

/** 동일 멱등 키 전송 직렬화 (in-flight 중복 방지) */
function createPerKeyChain() {
  const tails = new Map();
  return function runSerialized(idempotencyKey, fn) {
    const key = String(idempotencyKey || 'default');
    const prev = tails.get(key) || Promise.resolve();
    const next = prev.then(() => fn()).catch((e) => {
      throw e;
    });
    tails.set(key, next.catch(() => {}));
    return next;
  };
}

module.exports = {
  stableStringify,
  buildIdempotencyKey,
  shouldRetryDelivery,
  computeBackoffMs,
  terminalDeliveryState,
  createPerKeyChain,
};
