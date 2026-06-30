'use strict';

/** 개발 노티 payload에서 업무 중복 판별 키 (orderNo·trnId·event·status) */
function devInternalPayloadBusinessKey(payload) {
  if (!payload || typeof payload !== 'object') return '';
  const orderNo = String(
    payload.orderNo ?? payload.OrderNo ?? payload.orderid ?? payload.orderID ?? payload.orderId ?? '',
  ).trim();
  const trnId = String(
    payload.trnId ??
      payload.trnID ??
      payload.TransactionId ??
      payload.transactionId ??
      payload.transNo ??
      payload.transaction_id ??
      '',
  ).trim();
  const event = String(payload.event ?? payload.Event ?? '').trim();
  const status = String(
    payload.status ?? payload.PaymentStatus ?? payload.paymentStatus ?? payload.trade_state ?? '',
  ).trim();
  const merchant = String(
    payload.mid ?? payload.MID ?? payload.memberid ?? payload.merchantId ?? payload.compId ?? '',
  ).trim();
  if (!orderNo && !trnId) return '';
  return `${orderNo}\x1f${trnId}\x1f${event}\x1f${status}\x1f${merchant}`;
}

function devInternalLogDedupeKey(log) {
  if (!log || typeof log !== 'object') return '';
  const payload = log.payload || {};
  const biz = devInternalPayloadBusinessKey(payload);
  if (!biz) return '';
  const pg = String(log.pgProvider || 'chillpay').trim().toLowerCase() || 'chillpay';
  const mid = String(log.merchantId || '').trim();
  const url = String(log.internalTargetUrl || '').trim();
  return `${pg}\x1f${mid}\x1f${biz}\x1f${url}`;
}

function devInternalForwardSuppressKey(devUrl, payload, merchantId, pgProvider) {
  const biz = devInternalPayloadBusinessKey(payload);
  if (!biz) return '';
  const url = String(devUrl || '').trim();
  const pg = String(pgProvider || 'chillpay').trim().toLowerCase() || 'chillpay';
  const mid = String(merchantId || '').trim();
  return `${url}\x1f${pg}\x1f${mid}\x1f${biz}`;
}

function dedupeDevInternalLogObjects(objs) {
  const byKey = new Map();
  const noKey = [];
  for (let i = 0; i < objs.length; i++) {
    const obj = objs[i];
    const key = devInternalLogDedupeKey(obj);
    if (!key) {
      noKey.push(obj);
      continue;
    }
    const t = Date.parse(obj.storedAtIso || obj.storedAt || '');
    const prev = byKey.get(key);
    const pt = prev ? Date.parse(prev.storedAtIso || prev.storedAt || '') : Number.NaN;
    if (!prev || (!Number.isNaN(t) && (Number.isNaN(pt) || t >= pt))) {
      byKey.set(key, obj);
    }
  }
  return noKey.concat(Array.from(byKey.values()));
}

function logMatchesOrderNo(log, orderNo) {
  const want = String(orderNo || '').trim();
  if (!want) return false;
  const payload = (log && log.payload) || {};
  const candidates = [
    payload.orderNo,
    payload.OrderNo,
    payload.orderid,
    payload.orderID,
    payload.orderId,
  ];
  return candidates.some((c) => String(c || '').trim() === want);
}

/** 특정 orderNo 목록: 각 orderNo당 최신 keepPerOrder 건만 남기고 나머지 제거 */
function purgeDevInternalLogsByOrderNos(objs, orderNos, keepPerOrder) {
  const targets = new Set((orderNos || []).map((o) => String(o).trim()).filter(Boolean));
  if (!targets.size) return { kept: objs, removed: 0 };
  const keep = Math.max(1, keepPerOrder == null ? 1 : keepPerOrder);
  const byOrder = new Map();
  for (const obj of objs) {
    for (const on of targets) {
      if (!logMatchesOrderNo(obj, on)) continue;
      const list = byOrder.get(on) || [];
      list.push(obj);
      byOrder.set(on, list);
    }
  }
  const toDrop = new Set();
  for (const [, list] of byOrder) {
    list.sort((a, b) => {
      const ta = Date.parse(a.storedAtIso || a.storedAt || '') || 0;
      const tb = Date.parse(b.storedAtIso || b.storedAt || '') || 0;
      return tb - ta;
    });
    for (let i = keep; i < list.length; i++) {
      toDrop.add(list[i]);
    }
  }
  if (!toDrop.size) return { kept: objs, removed: 0 };
  return {
    kept: objs.filter((o) => !toDrop.has(o)),
    removed: toDrop.size,
  };
}

module.exports = {
  devInternalPayloadBusinessKey,
  devInternalLogDedupeKey,
  devInternalForwardSuppressKey,
  dedupeDevInternalLogObjects,
  logMatchesOrderNo,
  purgeDevInternalLogsByOrderNos,
};
