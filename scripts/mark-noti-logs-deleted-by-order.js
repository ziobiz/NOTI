#!/usr/bin/env node
/**
 * OrderNo 기준 노티 목록 숨김(삭제거래). 원본 .log 는 변경하지 않습니다.
 *
 * 전부 숨김:
 *   node scripts/mark-noti-logs-deleted-by-order.js wc39302t232313
 *   DELETED_BY=admin LOG_KIND=dev_internal node scripts/mark-noti-logs-deleted-by-order.js <orderNo>
 *
 * 중복 정리(최초 1건 유지, 나머지 숨김):
 *   node scripts/mark-noti-logs-deleted-by-order.js --dedupe-keep-first wc39302t232313
 *   LOG_KIND=dev_internal node scripts/mark-noti-logs-deleted-by-order.js --dedupe-keep-first <orderNo>
 *
 * LOG_KIND: pg_noti | internal | dev_internal | dealmai_webhook | * (default: *)
 */
const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, '..', 'data');
const NOTI_LOG_UI_DELETED_PATH = path.join(DATA_DIR, 'noti-log-ui-deleted.log');
const LOG_KINDS = ['pg_noti', 'internal', 'dev_internal', 'dealmai_webhook'];
const LOG_FILES = {
  pg_noti: 'pg-noti.log',
  internal: 'internal-noti.log',
  dev_internal: 'dev-internal-noti.log',
  dealmai_webhook: 'dealmai-webhook.log',
};

const rawArgs = process.argv.slice(2);
const dedupeKeepFirst = rawArgs[0] === '--dedupe-keep-first';
const orderNos = (dedupeKeepFirst ? rawArgs.slice(1) : rawArgs).map((s) => String(s || '').trim()).filter(Boolean);
const deletedBy = String(process.env.DELETED_BY || 'admin').trim() || 'admin';
const logKindEnv = String(process.env.LOG_KIND || '*').trim() || '*';
const kindsToRun =
  logKindEnv === '*' ? LOG_KINDS : LOG_KINDS.includes(logKindEnv) ? [logKindEnv] : [];

if (orderNos.length === 0 || kindsToRun.length === 0) {
  console.error(
    'Usage:\n' +
      '  node scripts/mark-noti-logs-deleted-by-order.js [--dedupe-keep-first] <orderNo> [...]\n' +
      '  LOG_KIND=dev_internal|pg_noti|internal|dealmai_webhook|*',
  );
  process.exit(1);
}

function payloadFrom(log) {
  if (!log || typeof log !== 'object') return {};
  if (log.payload && typeof log.payload === 'object') return log.payload;
  if (log.body && typeof log.body === 'object') return log.body;
  if (typeof log.body === 'string') {
    try {
      return JSON.parse(log.body);
    } catch {
      return {};
    }
  }
  return {};
}

function orderNoFrom(log) {
  const p = payloadFrom(log);
  return String(p.orderNo || p.OrderNo || log.orderNo || '').trim();
}

function storedAtFrom(log) {
  return String(log.storedAtIso || log.receivedAtIso || log.storedAt || log.receivedAt || '').trim();
}

function txIdFrom(log) {
  const p = payloadFrom(log);
  return String(p.TransactionId || p.transactionId || p.memberid || p.memberId || log.transactionId || '').trim();
}

function loadDeletedList() {
  if (!fs.existsSync(NOTI_LOG_UI_DELETED_PATH)) return [];
  return fs
    .readFileSync(NOTI_LOG_UI_DELETED_PATH, 'utf8')
    .split('\n')
    .filter((l) => l.trim())
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function isDeleted(log, logKind, deletedList) {
  const orderNo = orderNoFrom(log);
  const storedAtIso = storedAtFrom(log);
  const transactionId = txIdFrom(log);
  const merchantId = String(log.merchantId || '').trim();
  return deletedList.some((d) => {
    const dk = String(d.logKind || '*').trim();
    if (dk !== '*' && dk !== logKind) return false;
    if (d.bulkByOrderNo && d.orderNo) {
      return String(d.orderNo).trim() === orderNo && orderNo !== '';
    }
    if (d.storedAtIso && storedAtIso && String(d.storedAtIso).trim() !== storedAtIso) return false;
    if (d.orderNo && orderNo && String(d.orderNo).trim() !== orderNo) return false;
    if (d.transactionId && transactionId && String(d.transactionId).trim() !== transactionId) return false;
    if (d.merchantId && merchantId && String(d.merchantId).trim() !== merchantId) return false;
    if (d.storedAtIso && storedAtIso) return String(d.storedAtIso).trim() === storedAtIso;
    if (d.orderNo && orderNo && !d.storedAtIso) return String(d.orderNo).trim() === orderNo;
    return false;
  });
}

function loadLogArray(kind) {
  const file = path.join(DATA_DIR, LOG_FILES[kind]);
  if (!fs.existsSync(file)) return [];
  return fs
    .readFileSync(file, 'utf8')
    .split('\n')
    .filter((l) => l.trim())
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function appendNotiLogUiDeleted(entry) {
  const id = (entry.id || 'nl_' + Date.now() + '_' + Math.random().toString(36).slice(2, 10)).toString();
  const row = { id, deletedAtIso: new Date().toISOString(), ...entry };
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.appendFileSync(NOTI_LOG_UI_DELETED_PATH, JSON.stringify(row) + '\n', 'utf8');
  return id;
}

function sortKey(log, index) {
  const parsed = Date.parse(storedAtFrom(log));
  return { t: Number.isNaN(parsed) ? index : parsed, i: index };
}

function dedupeKeepFirstForKind(kind, orderNo, deletedList) {
  const logs = loadLogArray(kind);
  const matches = [];
  logs.forEach((log, index) => {
    if (orderNoFrom(log) !== orderNo) return;
    if (isDeleted(log, kind, deletedList)) return;
    matches.push({ log, index });
  });
  if (matches.length <= 1) return { hidden: 0, total: matches.length };
  matches.sort((a, b) => {
    const ka = sortKey(a.log, a.index);
    const kb = sortKey(b.log, b.index);
    if (ka.t !== kb.t) return ka.t - kb.t;
    return ka.i - kb.i;
  });
  let hidden = 0;
  for (const { log } of matches.slice(1)) {
    appendNotiLogUiDeleted({
      logKind: kind,
      orderNo,
      storedAtIso: storedAtFrom(log) || undefined,
      transactionId: txIdFrom(log) || undefined,
      merchantId: String(log.merchantId || '').trim() || undefined,
      deletedBy,
      note: 'dedupe_keep_first_script',
    });
    deletedList.push({ logKind: kind, orderNo, storedAtIso: storedAtFrom(log) });
    hidden++;
  }
  return { hidden, total: matches.length };
}

function bulkHideForKind(kind, orderNo) {
  return appendNotiLogUiDeleted({
    logKind: kind,
    orderNo,
    bulkByOrderNo: true,
    deletedBy,
    note: 'bulk_by_order_no_script',
  });
}

console.log('mode:', dedupeKeepFirst ? 'dedupe-keep-first' : 'bulk-hide-all');
console.log('NOTI trash:', NOTI_LOG_UI_DELETED_PATH);
console.log('deletedBy:', deletedBy, 'kinds:', kindsToRun.join(','));

let deletedList = loadDeletedList();

for (const orderNo of orderNos) {
  if (dedupeKeepFirst) {
    for (const kind of kindsToRun) {
      const r = dedupeKeepFirstForKind(kind, orderNo, deletedList);
      console.log(kind, orderNo, 'total=', r.total, 'hidden=', r.hidden);
    }
  } else if (logKindEnv === '*') {
    const id = bulkHideForKind('*', orderNo);
    console.log('bulk *', orderNo, '->', id);
  } else {
    for (const kind of kindsToRun) {
      const id = bulkHideForKind(kind, orderNo);
      console.log('bulk', kind, orderNo, '->', id);
    }
  }
}

console.log('done', orderNos.length, 'order(s)');
