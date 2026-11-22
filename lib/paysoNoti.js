/**
 * Payso (Pay Solutions) webhook helpers —
 * 1) PG 원본 form postback → ICOPAY …/PAYSO
 * 2) ICOPAY 통합 JSON(event=pg.payment.status, status 10/99) → 가맹 릴레이만
 * 가맹 본문: ICOPAY 통합 필드 (PG명 미포함). 실패(99/FAILED)를 성공으로 바꾸지 않음.
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

/** ICOPAY 아웃바운드 통합 통보 — ICOPAY 로 재포워드 금지 */
function looksLikeIcopayUnifiedNotify(body) {
  const b = body && typeof body === 'object' ? body : {};
  const ev = String(pick(b, ['event', 'Event']) || '')
    .trim()
    .toLowerCase();
  if (ev === 'pg.payment.status' || ev === 'payment.status') return true;
  const st = String(pick(b, ['status', 'Status']) || '').trim();
  const paySt = String(pick(b, ['paymentStatus', 'PaymentStatus']) || '')
    .trim()
    .toUpperCase();
  const unifiedStatus =
    st === '10' ||
    st === '99' ||
    paySt === 'PAID' ||
    paySt === 'FAILED' ||
    paySt === 'FAIL';
  const hasIcoKeys =
    unifiedStatus &&
    !!pick(b, ['trnId', 'pgTxnId', 'orderNo', 'OrderNo', 'compId', 'CompId', 'Comp-Id']);
  const hasPaysoForm = !!pick(b, ['refno', 'refNo', 'merchantid', 'statusname', 'cardtype']);
  return hasIcoKeys && !hasPaysoForm;
}

/** 명시적 실패 — status/paymentStatus/outcomeReasonCode (성공으로 올리면 안 됨) */
function paysoIsExplicitFailure(body) {
  const b = body && typeof body === 'object' ? body : {};
  const st = String(pick(b, ['status', 'Status']) || '')
    .trim()
    .toUpperCase();
  if (st === '99' || st === 'F0' || st === '1') return true;

  const paySt = String(pick(b, ['paymentStatus', 'PaymentStatus']) || '')
    .trim()
    .toUpperCase();
  if (
    paySt === 'FAILED' ||
    paySt === 'FAIL' ||
    paySt === 'CANCEL' ||
    paySt === 'CANCELLED' ||
    paySt === 'VOID' ||
    paySt === 'ERROR'
  ) {
    return true;
  }

  const reasonCode = String(pick(b, ['outcomeReasonCode', 'outcome_reason_code']) || '')
    .trim()
    .toUpperCase();
  if (
    reasonCode &&
    (/EXPIRED|RETURN_FAIL|FALSE_PAID|BUYER_RETURN|PENDING_EXPIRED|FAIL|ERROR|TIMEOUT|CANCEL|VOID/.test(
      reasonCode,
    ) ||
      reasonCode.indexOf('FAIL') >= 0 ||
      reasonCode.indexOf('EXPIRED') >= 0)
  ) {
    return true;
  }

  const stName = String(pick(b, ['statusname', 'statusName']) || '')
    .trim()
    .toLowerCase();
  if (stName === 'failed' || stName === 'fail' || stName === 'cancel' || stName === 'cancelled') return true;

  if (String(pick(b, ['returncode']) || '').trim() === '99') return true;
  return false;
}

/** 명시적 성공 */
function paysoIsExplicitSuccess(body) {
  const b = body && typeof body === 'object' ? body : {};
  if (paysoIsExplicitFailure(b)) return false;

  const paySt = String(pick(b, ['paymentStatus', 'PaymentStatus']) || '')
    .trim()
    .toUpperCase();
  if (
    paySt === 'PAID' ||
    paySt === 'SUCCEEDED' ||
    paySt === 'SUCCESS' ||
    paySt === 'COMPLETE' ||
    paySt === 'COMPLETED' ||
    paySt === '0'
  ) {
    return true;
  }

  const st = String(pick(b, ['status', 'Status', 'result']) || '')
    .trim()
    .toUpperCase();
  if (st === '10' || st === '0' || st === 'CP' || st === 'PAID' || st === 'SUCCESS' || st === 'OK') {
    return true;
  }

  const stName = String(pick(b, ['statusname', 'statusName']) || '')
    .trim()
    .toLowerCase();
  if (stName === 'completed' || stName === 'success' || stName === 'paid') return true;

  if (pick(b, ['paidAt', 'paid_at', 'PaymentDate', 'paymentDate'])) return true;
  return false;
}

/**
 * 결제 성공 여부.
 * 실패 신호를 최우선 — status=99 만으로 paid=true 되는 버그 방지.
 */
function paysoIsPaid(body) {
  const b = body && typeof body === 'object' ? body : {};
  if (paysoIsExplicitFailure(b)) return false;
  if (paysoIsExplicitSuccess(b)) return true;
  /* Payso 원본 성공 postback 은 보통 status=CP / statusname=COMPLETED / refno+total */
  if (pick(b, ['refno', 'refNo']) && pick(b, ['total', 'amount'])) {
    const st = String(pick(b, ['status', 'Status']) || '')
      .trim()
      .toUpperCase();
    if (!st || st === 'CP' || st === '10' || st === '0') return true;
  }
  return false;
}

function paysoShouldNotifyMerchant(body) {
  const b = body && typeof body === 'object' ? body : {};
  if (looksLikeIcopayUnifiedNotify(b)) return true;
  if (pick(b, ['refno', 'refNo', 'referenceNo', 'transactionId', 'trnId', 'pgTxnId'])) return true;
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
  const fromPaysoOrderNo = String(pick(b, ['orderno']) || '').trim();
  const looksIco = (s) => /^(OM|FS|om|fs)/.test(String(s || '').trim());
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
 * 가맹 릴레이·노티 로그용 본문 (ICOPAY 통합 필드. PG명 미포함).
 * @param {object} evtBody
 * @param {string} [merchantOrderNo]
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
  const outcomeReasonCode = String(pick(b, ['outcomeReasonCode', 'outcome_reason_code']) || '').trim();
  const outcomeReason = String(pick(b, ['outcomeReason', 'outcome_reason']) || '').trim();
  const notifyChannel = String(pick(b, ['notifyChannel', 'notify_channel']) || '').trim();
  const tsRaw = pick(b, ['ts', 'timestamp']);
  const tsNum = tsRaw != null && Number.isFinite(Number(tsRaw)) ? Number(tsRaw) : Date.now();

  const out = {
    event: 'pg.payment.status',
    orderNo: merchantOrder || '',
    status: paid ? '10' : '99',
    paymentStatus: paid ? 'PAID' : 'FAILED',
    amount: amount || undefined,
    currency,
    ts: tsNum,
  };

  /* 노티거래내역 표시 호환 (ChillPay 열) — PG 브랜드명 없음 */
  out.OrderNo = out.orderNo;
  out.orderid = out.orderNo;
  out.orderID = out.orderNo;
  out.PaymentStatus = paid ? '0' : '1';
  out.Amount = amount || undefined;
  out.Currency = currency;

  if (txId) {
    out.trnId = String(pick(b, ['trnId']) || txId);
    out.pgTxnId = String(pick(b, ['pgTxnId']) || txId);
    out.TransactionId = txId;
    out.transactionId = txId;
    out.transaction_id = txId;
  }
  if (paymentDate) {
    out.paidAt = paymentDate;
    out.PaymentDate = paymentDate;
    out.paymentDate = paymentDate;
  }
  if (outcomeReasonCode) out.outcomeReasonCode = outcomeReasonCode;
  if (outcomeReason) out.outcomeReason = outcomeReason;
  if (notifyChannel) out.notifyChannel = notifyChannel;

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
      if (k === 'orderNo' || k === 'status' || k === 'paymentStatus' || k === 'event') return;
      delete out[k];
    }
  });
  return out;
}

function enrichPaysoBodyForDisplay(body, rawBody) {
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
  return Object.assign({}, base, mapped, {
    paymentStatus: mapped.paymentStatus || base.paymentStatus,
    PaymentStatus: mapped.PaymentStatus != null ? mapped.PaymentStatus : base.PaymentStatus,
    status: mapped.status != null ? mapped.status : base.status,
    outcomeReasonCode: mapped.outcomeReasonCode || base.outcomeReasonCode,
    outcomeReason: mapped.outcomeReason || base.outcomeReason,
  });
}

/**
 * ICOPAY 가 PAYSO 실패/만료를 /noti/omspay 로 보낸 경우 등 —
 * notifyChannel·outcomeReasonCode 가 PAYSO* 이거나, 통합통보 trnId=PS… 이면 PAYSO 로 분류.
 */
function looksLikePaysoChannelNotify(body, rawBody) {
  const b = body && typeof body === 'object' ? body : {};
  let rb = null;
  if (rawBody != null && String(rawBody).trim().startsWith('{')) {
    try {
      rb = JSON.parse(String(rawBody).trim());
    } catch (_) {
      rb = null;
    }
  }
  const src = rb && typeof rb === 'object' ? Object.assign({}, b, rb) : b;
  const ch = String(pick(src, ['notifyChannel', 'notify_channel']) || '')
    .trim()
    .toUpperCase();
  if (ch.startsWith('PAYSO')) return true;
  const orc = String(pick(src, ['outcomeReasonCode', 'outcome_reason_code']) || '')
    .trim()
    .toUpperCase();
  if (orc.startsWith('PAYSO_')) return true;
  /* ICOPAY PAYSO 거래번호는 PS… — omspay 라우트로 와도 PAYSO 목록에 포함 */
  const trn = String(pick(src, ['trnId', 'TransactionId', 'transactionId']) || '').trim();
  if (/^PS/i.test(trn) && looksLikeIcopayUnifiedNotify(src)) return true;
  return false;
}

/** FS1261122151449… → 2026-11-22 (FS + optional digit + YYMMDD…) */
function paysoFsOrderCalendarYmd(orderNo) {
  const o = String(orderNo || '').trim();
  const m = o.match(/^FS\d?(\d{2})(\d{2})(\d{2})/);
  if (!m) return '';
  const yy = m[1];
  const mm = m[2];
  const dd = m[3];
  if (Number(mm) < 1 || Number(mm) > 12 || Number(dd) < 1 || Number(dd) > 31) return '';
  return '20' + yy + '-' + mm + '-' + dd;
}

module.exports = {
  pick,
  headerGetIgnoreCase,
  cleanCompId,
  looksLikeIcopayUnifiedNotify,
  looksLikePaysoChannelNotify,
  paysoFsOrderCalendarYmd,
  paysoIsExplicitFailure,
  paysoIsExplicitSuccess,
  paysoIsPaid,
  paysoShouldNotifyMerchant,
  mapPaysoToMerchantNotifyBody,
  enrichPaysoBodyForDisplay,
  pickPaysoTxnId,
  pickPaysoOrderNo,
};
