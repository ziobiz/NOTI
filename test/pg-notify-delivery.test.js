'use strict';

const assert = require('assert');
const pg = require('../lib/pgNotifyDelivery');

async function main() {
  assert.strictEqual(pg.shouldRetryDelivery({ success: true, status: 200 }, {}).retry, false);
  assert.strictEqual(pg.shouldRetryDelivery({ success: false, status: 0 }, {}).retry, true);
  assert.strictEqual(pg.shouldRetryDelivery({ success: false, status: 503 }, {}).retry, true);
  assert.strictEqual(pg.shouldRetryDelivery({ success: false, status: 403 }, {}).retry, false);
  assert.strictEqual(pg.shouldRetryDelivery({ success: false, status: 422 }, {}).retry, true);
  assert.strictEqual(pg.shouldRetryDelivery({ success: false, status: 422 }, { retryOn422: false }).retry, false);
  assert.strictEqual(pg.shouldRetryDelivery({ success: false, status: 422, retryable: false }, {}).retry, false);
  assert.strictEqual(pg.shouldRetryDelivery({ success: false, status: 200 }, { retryOnBodyFailure: false }).retry, false);
  assert.strictEqual(pg.shouldRetryDelivery({ success: false, status: 200 }, { retryOnBodyFailure: true }).retry, true);

  const cfg = { backoffBaseMs: 1000, backoffMaxMs: 10000, jitterRatio: 0 };
  for (let i = 0; i < 20; i++) {
    const w = pg.computeBackoffMs(2, cfg);
    assert(w >= 1000 && w <= 10000, 'backoff ' + w);
  }

  const k1 = pg.buildIdempotencyKey('https://a/x', { a: 1, b: 2 });
  const k2 = pg.buildIdempotencyKey('https://a/x', { b: 2, a: 1 });
  assert.strictEqual(k1, k2);

  assert.strictEqual(
    pg.terminalDeliveryState({ success: true, status: 200 }, { maxAttempts: 8, totalDeadlineMs: 60000 }, 1, 100),
    'DELIVERED'
  );
  assert.strictEqual(
    pg.terminalDeliveryState(
      { success: false, status: 403 },
      { maxAttempts: 8, totalDeadlineMs: 60000, retryOn422: false, retryOnBodyFailure: false },
      1,
      100
    ),
    'FAILED'
  );
  assert.strictEqual(
    pg.terminalDeliveryState({ success: false, status: 500 }, { maxAttempts: 8, totalDeadlineMs: 60000 }, 8, 1000),
    'DEAD_LETTER'
  );

  let concurrent = 0;
  let maxConcurrent = 0;
  const run = pg.createPerKeyChain();
  const key = 'k';
  await Promise.all([
    run(key, async () => {
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      await new Promise((r) => setTimeout(r, 8));
      concurrent--;
      return 1;
    }),
    run(key, async () => {
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      await new Promise((r) => setTimeout(r, 8));
      concurrent--;
      return 2;
    }),
  ]);
  assert.strictEqual(maxConcurrent, 1);

  console.log('pg-notify-delivery tests OK');
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
