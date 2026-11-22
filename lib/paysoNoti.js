/**
 * Payso (Pay Solutions) webhook helpers —
 * 1) PG 원본 form postback → ICOPAY …/PAYSO
 * 2) ICOPAY 통합 JSON(event=pg.payment.status) → 가맹 릴레이만 (ICOPAY 재전달 금지)
 * 가맹/노티거래내역 표시는 ChillPay·OMSPAY와 동일 키(TransactionId·PaymentStatus·PaymentDate).
 */
'use strict';

function nested(obj, dotted) {
  if (!obj || typeof obj !== 'object' || !dotted) return undefined;
  const parts = String(dotted).split('.');
  let cur = obj;
  for (const p of parts) {
    if (cur == null || typeof cur !== 'object') return undefined;
    cur = cur[p];
  }
  return cur;
}

function pick(obj, keys) {
  for (const k of keys) {
    const v = k.includes('.') ? nested(obj, k) : obj && obj[k];
    if (v != null && String(v).trim() !== '') return v;
  }
  return undefined;
}

function headerGetIgnoreCase(headers, name) {
  if (!headers || typeof headers !== 'object') return '';
  const want = String(name || '').toLowerCase();
  for (const [k, v] of Object.entries(headers)) {
    if (String(k).toLowerCase() === want) {
      return v == null ? '' : String(v).trim();
    }
  }
  return '';
}

function cleanCompId(compId) {
  const id = String(compId || '').trim();
  if (!id) return '';
  const hash = id.indexOf('#');
  return hash > 0 ? id.slice(0, hash) : id;
}

/** ICOPAY 통합 통보(JSON) — PG form 이 아님. ICOPAY 로 다시 forward 하면 루프·폭주. */
function looksLikeIcopayUnifiedNotify(body) {
  const b = body && typeof body === 'object' ? body : {};
  const ev = String(pick(b, ['event', 'Event']) || '')
    .trim()
    .toLowerCase();
  if (ev === 'pg.payment.status' || ev === 'payment.status') return true;
  const hasIcoKeys =
    !!pick(b, ['trnId', 'pgTxnId', 'paymentStatus', 'paidAt']) &&
    !!pick(b, ['orderNo', 'OrderNo', 'compId', 'CompId', 'Comp-Id']);
  const hasPaysoForm = !!pick(b, ['refno', 'refNo', 'merchantid', 'merchantId', 'statusname', 'cardtype']);
  return hasIcoKeys && !hasPaysoForm;
}

function paysoIsPaid(body) {
  const b = body && typeof body === 'object' ? body : {};
  const paySt = String(pick(b, ['paymentStatus', 'PaymentStatus']) || '')
    .trim()
    .toLowerCase();
  if (
    paySt === 'failed' ||
    paySt === 'fail' ||
    paySt === 'cancel' ||
    paySt === 'cancelled' ||
    paySt === 'void' ||
    paySt === 'error'
  ) {
    return false;
  }
  if (
    paySt === 'paid' ||
    paySt === 'succeeded' ||
    paySt === 'success' ||
    paySt === 'complete' ||
    paySt === 'completed'
  ) {
    return true;
  }
  const st = String(pick(b, ['status', 'Status', 'result']) || '')
    .trim()
    .toLowerCase();
  if (st === '99' || st === '1' || st === 'fail' || st === 'failed' || st === 'cancel' || st === 'cancelled') {
    return false;
  }
  if (st === '10' || st === '0' || st === 'cp' || st === 'paid' || st === 'success' || st === 'ok') {
    return true;
  }
  const stName = String(pick(b, ['statusname', 'statusName', 'PaymentStatusName']) || '')
    .trim()
    .toLowerCase();
  if (stName === 'completed' || stName === 'success' || stName === 'paid') return true;
  if (stName === 'failed' || stName === 'fail' || stName === 'cancel') return false;
  if (String(pick(b, ['returncode', 'PaymentStatus']) || '') === '99') return false;
  if (pick(b, ['paidAt', 'paid_at', 'PaymentDate', 'paymentDate'])) return true;
  /* Payso 기본 postback 는 성공 통보 위주 */
  if (pick(b, ['refno', 'refNo', 'total', 'amount'])) return true;
  return false;
}

/** Default/form postback 또는 ICOPAY 통합 — 가맹 릴레이 대상 */
function paysoShouldNotifyMerchant(body) {
  const b = body && typeof body === 'object' ? body : {};
  if (looksLikeIcopayUnifiedNotify(b)) return true;
  if (pick(b, ['refno', 'refNo', 'referenceNo', 'transactionId', 'transactionid', 'trnId', 'pgTxnId'])) return true;
  if (pick(b, ['total', 'amount', 'Amount'])) return true;
  return true;
}

function pickPaysoTxnId(b, extraTrnId) {
  return String(
    pick(b, [
      'trnId',
      'pgTxnId',
      'transactionId',
      'transactionid',
      'transaction_id',
      'TransactionId',
      'refno',
      'refNo',
      'referenceNo',
    ]) ||
      (extraTrnId != null ? extraTrnId : '') ||
      '',
  ).trim();
}

function pickPaysoOrderNo(b, merchantOrderNo) {
  const fromHdr = String(merchantOrderNo || '').trim();
  const fromIcoOrder = String(pick(b, ['orderNo', 'OrderNo']) || '').trim();
  const fromFormAlias = String(pick(b, ['order', 'orderid', 'orderID', 'ref1']) || '').trim();
  const fromPaysoOrderNo = String(pick(b, ['orderno']) || '').trim(); // Payso 자체 주문번호(숫자) — ICOPAY OM/FS 보다 우선순위 낮음
  const looksIco = (s) => /^(OM|FS|om|fs)/.test(String(s || '').trim());
  // ICOPAY 가 해석한 주문(헤더) 또는 JSON orderNo 를 Payso orderno 보다 우선
  if (looksIco(fromHdr)) return fromHdr;
  if (looksIco(fromIcoOrder)) return fromIcoOrder;
  if (fromHdr) return fromHdr;
  if (fromIcoOrder) return fromIcoOrder;
  if (fromFormAlias) return fromFormAlias;
  if (fromPaysoOrderNo) return fromPaysoOrderNo;
  return String(pick(b, ['refno', 'refNo']) || '').trim();
}

function pickPaysoAmount(b) {
  const amountRaw = pick(b, ['total', 'amount', 'Amount', 'true_amount', 'trueAmount']);
  if (amountRaw == null || String(amountRaw).trim() === '') return '';
  const n = Number(amountRaw);
  return Number.isFinite(n) ? String(n) : String(amountRaw).trim();
}

function pickPaysoPaymentDate(b) {
  return String(
    pick(b, ['paidAt', 'paid_at', 'PaymentDate', 'paymentDate', 'datetime', 'timestamp']) || '',
  ).trim();
}

/**
 * @param {object} evtBody Payso postback 또는 ICOPAY 통합 JSON
 * @param {string} [merchantOrderNo] X-Icopay-Order-No
 * @param {object} [extra] { compId, trnId }
 */
function mapPaysoToMerchantNotifyBody(evtBody, merchantOrderNo, extra) {
  const b = evtBody && typeof evtBody === 'object' ? evtBody : {};
  const merchantOrder = pickPaysoOrderNo(b, merchantOrderNo);
  const amount = pickPaysoAmount(b);
  const paid = paysoIsPaid(b);
  const extraTrn = extra && extra.trnId != null ? String(extra.trnId).trim() : '';
  const txId = pickPaysoTxnId(b, extraTrn);
  const paymentDate = pickPaysoPaymentDate(b);
  const currency = String(pick(b, ['currency', 'Currency', 'currencyCode']) || 'THB');

  const out = {
    event: 'pg.payment.status',
    orderNo: merchantOrder || '',
    OrderNo: merchantOrder || '',
    orderid: merchantOrder || '',
    orderID: merchantOrder || '',
    status: paid ? '10' : '99',
    paymentStatus: paid ? 'Succeeded' : 'Fail',
    PaymentStatus: paid ? '0' : '1',
    chillPaymentStatus: paid ? 'Paid' : 'Fail',
    outcome: paid ? 'success' : 'fail',
    amount: amount || undefined,
    Amount: amount || undefined,
    currency,
    Currency: currency,
    pgKind: 'payso',
    pgkind: 'PAYSO',
    van: 'PAYSO',
    ts: Date.now(),
  };
  if (txId) {
    out.TransactionId = txId;
    out.transactionId = txId;
    out.transaction_id = txId;
    out.pgTxnId = String(pick(b, ['pgTxnId']) || txId);
    out.trnId = String(pick(b, ['trnId']) || txId);
    out.refno = String(pick(b, ['refno', 'refNo']) || txId);
  }
  if (paymentDate) {
    out.PaymentDate = paymentDate;
    out.paymentDate = paymentDate;
    out.paidAt = paymentDate;
  }
  const comp = cleanCompId(
    (extra && extra.compId) || pick(b, ['compId', 'CompId', 'Comp-Id', 'merchantId', 'MerchantId']),
  );
  if (comp) {
    out.compId = comp;
    out.CompId = comp;
    out['Comp-Id'] = comp;
    out.merchantId = comp;
  }
  Object.keys(out).forEach((k) => {
    if (out[k] === undefined || out[k] === '') {
      if (
        k === 'orderNo' ||
        k === 'OrderNo' ||
        k === 'status' ||
        k === 'paymentStatus' ||
        k === 'PaymentStatus' ||
        k === 'event'
      ) {
        return;
      }
      delete out[k];
    }
  });
  return out;
}

/** 과거 잘못 매핑된 로그: rawBody(ICOPAY JSON/form)에서 표시 필드 복구 */
function enrichPaysoBodyForDisplay(body, rawBody, contentType) {
  const base = body && typeof body === 'object' ? Object.assign({}, body) : {};
  let rawObj = null;
  if (rawBody != null && String(rawBody).trim()) {
    const s = String(rawBody).trim();
    if (s.startsWith('{')) {
      try {
        rawObj = JSON.parse(s);
      } catch (_) {
        rawObj = null;
      }
    } else if (s.includes('=')) {
      rawObj = {};
      try {
        const params = new URLSearchParams(s);
        for (const [k, v] of params.entries()) rawObj[k] = v;
      } catch (_) {
        rawObj = null;
      }
    }
  }
  if (!rawObj || typeof rawObj !== 'object') return base;
  const mapped = mapPaysoToMerchantNotifyBody(rawObj, base.orderNo || base.OrderNo || '', {
    compId: base.compId || base.CompId || '',
    trnId: base.trnId || '',
  });
  /* 표시용: raw 기반 매핑을 우선하되, 기존 body 의 비어 있지 않은 보조 키는 유지 */
  return Object.assign({}, base, mapped, {
    /* 성공/실패는 raw 재해석 결과 우선 */
    paymentStatus: mapped.paymentStatus || base.paymentStatus,
    PaymentStatus: mapped.PaymentStatus != null ? mapped.PaymentStatus : base.PaymentStatus,
    status: mapped.status != null ? mapped.status : base.status,
  });
}

module.exports = {
  pick,
  headerGetIgnoreCase,
  cleanCompId,
  looksLikeIcopayUnifiedNotify,
  paysoIsPaid,
  paysoShouldNotifyMerchant,
  mapPaysoToMerchantNotifyBody,
  enrichPaysoBodyForDisplay,
  pickPaysoTxnId,
  pickPaysoOrderNo,
};
