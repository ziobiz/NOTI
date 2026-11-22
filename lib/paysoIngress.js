/**
 * PAYSO ingress —
 * A) ICOPAY 아웃바운드(event=pg.payment.status) → ICOPAY 재포워드 금지, 가맹 callbackUrl 릴레이만
 * B) PAYSO 원본 form/query postback → ICOPAY …/PAYSO 포워드 + Comp-Id + 가맹 릴레이
 */
'use strict';

const path = require('path');
const axios = require('axios');
const paysoNoti = require('./paysoNoti');

function createPaysoIngress(deps) {
  const {
    CONFIG_DIR,
    loadJsonConfig,
    loadOmspayIngressConfig,
    applyIcopayPgNotifyIngressHeaders,
    extractIcopayCompIdAndOrderFromNotifyResult,
    parseNotiRawBodyToObject,
    findMerchantByCompIdForCrypto,
    MERCHANTS,
    relayToMerchant,
    appendPgNotiLog,
    stampOmspayCompIdOnBody,
    pgIngressGuard,
    respondPgIngressGuard,
  } = deps;

  const PAYSO_INGRESS_CONFIG_PATH = path.join(CONFIG_DIR, 'payso-ingress.json');

  function loadPaysoIngressConfig() {
    const fromFile = loadJsonConfig(PAYSO_INGRESS_CONFIG_PATH, {}) || {};
    const envUrl = String(process.env.PAYSO_ICOPAY_NOTIFY_URL || '').trim();
    const envLookup = String(process.env.PAYSO_ICOPAY_ORDER_LOOKUP_URL || '').trim();
    let icopayNotifyUrl = envUrl || String(fromFile.icopayNotifyUrl || '').trim();
    if (!icopayNotifyUrl || /YOUR_INGRESS_TOKEN/i.test(icopayNotifyUrl)) {
      try {
        const oms = loadOmspayIngressConfig();
        const omsUrl = String((oms && oms.icopayNotifyUrl) || '').trim();
        if (omsUrl) {
          icopayNotifyUrl = omsUrl.replace(/\/OMSPAY\b/i, '/PAYSO').replace(/\/omspay\b/i, '/PAYSO');
        }
      } catch (_) {}
    }
    if (!icopayNotifyUrl || /YOUR_INGRESS_TOKEN/i.test(icopayNotifyUrl)) {
      try {
        const ksPath = path.join(CONFIG_DIR, 'ksnet-ingress.json');
        const ks = loadJsonConfig(ksPath, {}) || {};
        const ksUrl = String(ks.icopayNotifyUrl || '').trim();
        if (ksUrl && !/YOUR_INGRESS_TOKEN/i.test(ksUrl)) {
          icopayNotifyUrl = ksUrl.replace(/\/KSNET\b/i, '/PAYSO').replace(/\/ksnet\b/i, '/PAYSO');
        }
      } catch (_) {}
    }
    let icopayOrderLookupUrl = envLookup || String(fromFile.icopayOrderLookupUrl || '').trim();
    if (!icopayOrderLookupUrl) {
      try {
        const oms = loadOmspayIngressConfig();
        icopayOrderLookupUrl = String((oms && oms.icopayOrderLookupUrl) || '').trim();
      } catch (_) {}
    }
    return {
      enabled: fromFile.enabled !== false,
      icopayNotifyUrl,
      icopayOrderLookupUrl,
      timeoutMs: Math.max(
        3000,
        Math.min(60000, Number(fromFile.timeoutMs) || Number(process.env.PAYSO_ICOPAY_TIMEOUT_MS) || 12000),
      ),
      note: String(fromFile.note || '').trim(),
    };
  }

  function stampPaysoCompIdOnBody(body, compId) {
    if (typeof stampOmspayCompIdOnBody === 'function') {
      return stampOmspayCompIdOnBody(body, compId);
    }
    const id = paysoNoti.cleanCompId(compId);
    if (!id || !body || typeof body !== 'object') return body || {};
    return Object.assign({}, body, { compId: id, CompId: id, 'Comp-Id': id, merchantId: id });
  }

  function parseIncomingBody(req) {
    const incomingContentType =
      (req.get && req.get('Content-Type')) || (req.headers && req.headers['content-type']) || '';
    const rawBodyStr = req.rawBodyBuffer ? req.rawBodyBuffer.toString('utf8') : '';
    let body = req.body;
    if (!body || typeof body !== 'object') body = {};
    if (req.method === 'GET' && req.query && typeof req.query === 'object') {
      body = Object.assign({}, body, req.query);
    }
    if (Object.keys(body).length === 0 && req.rawBodyBuffer && req.rawBodyBuffer.length) {
      try {
        body = JSON.parse(req.rawBodyBuffer.toString('utf8'));
      } catch (_) {
        const parsed = parseNotiRawBodyToObject(req.rawBodyBuffer, incomingContentType);
        if (Object.keys(parsed).length) body = parsed;
      }
    }
    return { body, rawBodyStr, incomingContentType };
  }

  function buildForwardBody(rawBodyStr, body, incomingContentType, asJson) {
    if (rawBodyStr && String(rawBodyStr).trim()) return rawBodyStr;
    if (asJson || (incomingContentType || '').toLowerCase().includes('json')) {
      return JSON.stringify(body || {});
    }
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(body || {})) {
      if (v === undefined || v === null) continue;
      params.append(k, typeof v === 'object' ? JSON.stringify(v) : String(v));
    }
    return params.toString();
  }

  async function forwardPaysoToIcopay(rawBodyStr, contentType, attempt, extraHeaders) {
    const cfg = loadPaysoIngressConfig();
    if (!cfg.enabled) {
      return { ok: false, status: 503, data: 'PAYSO ingress disabled', headers: {} };
    }
    if (!cfg.icopayNotifyUrl || /YOUR_INGRESS_TOKEN/i.test(cfg.icopayNotifyUrl)) {
      return { ok: false, status: 503, data: 'PAYSO ICOPAY notify URL not configured', headers: {} };
    }
    const headers = {
      'Content-Type':
        contentType && String(contentType).trim()
          ? String(contentType).split(';')[0].trim() + '; charset=utf-8'
          : 'application/x-www-form-urlencoded; charset=utf-8',
    };
    if (extraHeaders && typeof extraHeaders === 'object') Object.assign(headers, extraHeaders);
    applyIcopayPgNotifyIngressHeaders(headers, cfg.icopayNotifyUrl, attempt || 1);
    if (!headers['X-Icopay-Notify-Delivery']) {
      headers['X-Icopay-Notify-Delivery'] = (attempt || 1) <= 1 ? 'LIVE' : 'RETRY';
      headers['X-Noti-Attempt'] = String(Math.max(1, Math.floor(Number(attempt) || 1)));
    }
    try {
      const res = await axios.post(cfg.icopayNotifyUrl, rawBodyStr || '', {
        headers,
        timeout: cfg.timeoutMs,
        validateStatus: () => true,
        transformResponse: [(d) => d],
        responseType: 'text',
      });
      return {
        ok: res.status >= 200 && res.status < 300,
        status: res.status,
        data: res.data,
        headers: res.headers || {},
      };
    } catch (e) {
      console.error('[PAYSO] forward error', e.message || e);
      return { ok: false, status: 502, data: String(e.message || e), headers: {} };
    }
  }

  function findPaysoMerchant(compId) {
    const compIdBase = paysoNoti.cleanCompId(compId);
    if (!compIdBase) return null;
    let match = findMerchantByCompIdForCrypto(compIdBase);
    const slot = MERCHANTS.get(compIdBase + '#payso');
    if (slot && String(slot.callbackUrl || slot.resultUrl || '').trim()) {
      match = { merchantId: compIdBase + '#payso', merchant: slot, via: 'payso-slot' };
    }
    return match;
  }

  /**
   * @param {object} opts
   * @param {boolean} [opts.forceRelayIfUrl] ICOPAY 아웃바운드: enableRelay 없어도 callbackUrl 있으면 릴레이
   * @param {string} [opts.routeKey]
   */
  async function relayPaysoMerchantNotify(merchantId, merchant, notifyBody, rawBody, incomingContentType, opts) {
    const o = opts || {};
    const routeKey = o.routeKey || 'payso/webhook';
    const forceRelayIfUrl = o.forceRelayIfUrl === true;
    const enableRelay = forceRelayIfUrl
      ? !!(merchant && String(merchant.callbackUrl || '').trim())
      : !!(merchant && merchant.enableRelay);
    const targetUrl = merchant && merchant.callbackUrl ? String(merchant.callbackUrl).trim() : '';
    let relaySuccess = false;
    let relayFailReason = '';
    let relayFormat = 'json';
    if (enableRelay && targetUrl) {
      const rf = String((merchant && merchant.relayFormat) || 'json').toLowerCase();
      relayFormat = rf === 'form' ? 'form' : 'json';
      let relayOpts = { contentType: 'application/json', rawBody: JSON.stringify(notifyBody || {}) };
      if (relayFormat === 'form') {
        const params = new URLSearchParams();
        for (const [k, v] of Object.entries(notifyBody || {})) {
          if (v === undefined || v === null) continue;
          params.append(k, typeof v === 'object' ? JSON.stringify(v) : String(v));
        }
        relayOpts = {
          contentType: 'application/x-www-form-urlencoded',
          rawBody: params.toString(),
        };
      }
      try {
        let relayRes = await relayToMerchant(targetUrl, notifyBody, relayOpts);
        relaySuccess = relayRes.status >= 200 && relayRes.status < 400;
        if (!relaySuccess) {
          relayFailReason =
            'HTTP ' +
            relayRes.status +
            (relayRes.data != null ? ': ' + String(relayRes.data).slice(0, 200) : '');
          await new Promise((r) => setTimeout(r, 2000));
          relayRes = await relayToMerchant(targetUrl, notifyBody, relayOpts);
          relaySuccess = relayRes.status >= 200 && relayRes.status < 400;
          if (relaySuccess) relayFailReason = '';
          else {
            relayFailReason =
              'HTTP ' +
              relayRes.status +
              (relayRes.data != null ? ': ' + String(relayRes.data).slice(0, 200) : '');
          }
        }
      } catch (err) {
        relayFailReason = err.code || err.message || String(err);
        try {
          await new Promise((r) => setTimeout(r, 2000));
          const retryRes = await relayToMerchant(targetUrl, notifyBody, relayOpts);
          relaySuccess = retryRes.status >= 200 && retryRes.status < 400;
          if (relaySuccess) relayFailReason = '';
        } catch (err2) {
          if (!relayFailReason) relayFailReason = err2.code || err2.message || String(err2);
        }
      }
    } else if (enableRelay && !targetUrl) {
      relayFailReason = '가맹점 URL 없음';
    } else if (!enableRelay) {
      relayFailReason = forceRelayIfUrl ? '가맹점 URL 없음' : 'relay_disabled';
    }

    appendPgNotiLog({
      routeKey,
      merchantId,
      kind: 'callback',
      body: notifyBody,
      rawBody: rawBody || undefined,
      targetUrl: enableRelay ? targetUrl || '' : '',
      contentType: incomingContentType,
      env: 'live',
      relayStatus: enableRelay ? (relaySuccess ? 'ok' : 'fail') : 'skip',
      relayFailReason: relaySuccess ? '' : relayFailReason || '',
      relayFormatUsed: enableRelay && relaySuccess ? relayFormat : undefined,
      pgProvider: 'payso',
    });
    return { relaySuccess, relayFailReason };
  }

  /** A) ICOPAY → NOTI 아웃바운드: 재포워드 금지, 가맹 릴레이만, 항상 2xx */
  async function handleIcopayOutbound(req, res, ctx) {
    const { body, forwardBody, incomingContentType, guard } = ctx;
    const compIdHdr = paysoNoti.cleanCompId(
      paysoNoti.pick(body, ['compId', 'CompId', 'Comp-Id', 'merchantId']) || '',
    );
    const orderHdr = String(paysoNoti.pick(body, ['orderNo', 'OrderNo']) || '').trim();
    const trnId = String(
      paysoNoti.pick(body, ['trnId', 'pgTxnId', 'transactionId', 'refno']) || '',
    ).trim();
    const paid = paysoNoti.paysoIsPaid(body);

    if (pgIngressGuard && guard) {
      pgIngressGuard.endInbound('payso', guard.key, { ok: true, httpStatus: 200 });
    }
    console.log(
      '[PAYSO] icopay-outbound (no re-forward)',
      'trnId=',
      trnId || '-',
      'order=',
      orderHdr || '-',
      'compId=',
      compIdHdr || '(none)',
      'paid=',
      paid ? '1' : '0',
      'reason=',
      paysoNoti.pick(body, ['outcomeReasonCode']) || '-',
    );

    res.status(200).json({
      ok: true,
      accepted: true,
      source: 'icopay-outbound',
      orderNo: orderHdr || '',
      pgTxnId: trnId || '',
      paymentStatus: paid ? 'PAID' : 'FAILED',
    });

    const notifyBody = stampPaysoCompIdOnBody(
      paysoNoti.mapPaysoToMerchantNotifyBody(body, orderHdr, { compId: compIdHdr, trnId }),
      compIdHdr,
    );

    setImmediate(() => {
      Promise.resolve()
        .then(async () => {
          const match = findPaysoMerchant(compIdHdr);
          if (!match) {
            appendPgNotiLog({
              routeKey: 'payso/icopay-outbound',
              merchantId: compIdHdr || '',
              kind: 'callback',
              body: notifyBody,
              rawBody: forwardBody || undefined,
              targetUrl: '',
              contentType: incomingContentType,
              env: 'live',
              relayStatus: 'fail',
              relayFailReason: compIdHdr ? 'Merchant not found for Comp-Id' : 'Missing Comp-Id',
              pgProvider: 'payso',
            });
            return;
          }
          await relayPaysoMerchantNotify(
            match.merchantId,
            match.merchant,
            stampPaysoCompIdOnBody(notifyBody, compIdHdr),
            forwardBody,
            incomingContentType,
            { routeKey: 'payso/icopay-outbound', forceRelayIfUrl: true },
          );
        })
        .catch((e) => {
          console.error('[PAYSO] icopay-outbound relay error', e && e.message ? e.message : e);
          appendPgNotiLog({
            routeKey: 'payso/icopay-outbound',
            merchantId: compIdHdr || '',
            kind: 'callback',
            body: notifyBody,
            rawBody: forwardBody || undefined,
            targetUrl: '',
            contentType: incomingContentType,
            env: 'live',
            relayStatus: 'fail',
            relayFailReason: e && e.message ? e.message : String(e),
            pgProvider: 'payso',
          });
        });
    });
  }

  /** B) PAYSO 원본 postback → ICOPAY 포워드 + 가맹 릴레이 */
  async function handlePaysoOriginPostback(req, res, ctx) {
    const { body, forwardBody, incomingContentType, guard, refno, order } = ctx;
    const cfg = loadPaysoIngressConfig();

    if (!cfg.icopayNotifyUrl || /YOUR_INGRESS_TOKEN/i.test(cfg.icopayNotifyUrl)) {
      console.error('[PAYSO] icopayNotifyUrl not configured');
      appendPgNotiLog({
        routeKey: 'payso/webhook',
        merchantId: '',
        kind: 'callback',
        body: Object.assign({}, body),
        rawBody: ctx.rawBodyStr || undefined,
        targetUrl: '',
        contentType: incomingContentType,
        env: 'live',
        relayStatus: 'fail',
        relayFailReason: 'icopayNotifyUrl_missing',
        pgProvider: 'payso',
      });
      if (pgIngressGuard && guard) {
        pgIngressGuard.endInbound('payso', guard.key, { ok: false, httpStatus: 503 });
      }
      return res.status(503).send('PAYSO ICOPAY URL not configured');
    }

    let ico;
    try {
      ico = await forwardPaysoToIcopay(
        forwardBody,
        incomingContentType || 'application/x-www-form-urlencoded',
        1,
      );
    } catch (eFwd) {
      if (pgIngressGuard && guard) {
        pgIngressGuard.endInbound('payso', guard.key, { ok: false, httpStatus: 502 });
      }
      throw eFwd;
    }

    const extracted = extractIcopayCompIdAndOrderFromNotifyResult(ico);
    const compIdHdr = paysoNoti.cleanCompId(extracted.compId);
    const orderHdr = extracted.orderNo;
    const outStatus = ico.status && Number.isFinite(Number(ico.status)) ? Number(ico.status) : 502;
    const outCt =
      paysoNoti.headerGetIgnoreCase(ico.headers, 'content-type') || 'application/json; charset=utf-8';
    const outBody = ico.data != null ? ico.data : '';
    const forwardOk = outStatus >= 200 && outStatus < 300;
    if (pgIngressGuard && guard) {
      pgIngressGuard.endInbound('payso', guard.key, { ok: forwardOk, httpStatus: outStatus });
    }
    console.log(
      '[PAYSO] passthrough',
      'refno=',
      refno || '-',
      'order=',
      order || '-',
      'http=',
      outStatus,
      'compId=',
      compIdHdr || '(none)',
    );
    res.status(outStatus);
    res.set('Content-Type', outCt);
    res.send(outBody);

    const baseNotifyBody = stampPaysoCompIdOnBody(
      paysoNoti.mapPaysoToMerchantNotifyBody(body, orderHdr || order, {
        compId: compIdHdr,
        trnId: refno,
      }),
      compIdHdr,
    );
    if (!paysoNoti.paysoShouldNotifyMerchant(body)) {
      appendPgNotiLog({
        routeKey: 'payso/webhook',
        merchantId: compIdHdr || '',
        kind: 'callback',
        body: baseNotifyBody,
        rawBody: forwardBody || undefined,
        targetUrl: '',
        contentType: incomingContentType,
        env: 'live',
        relayStatus: 'skip',
        relayFailReason: 'event_skip',
        pgProvider: 'payso',
      });
      return;
    }

    setImmediate(() => {
      Promise.resolve()
        .then(async () => {
          let compId =
            compIdHdr || paysoNoti.cleanCompId(paysoNoti.pick(body, ['compId', 'CompId', 'Comp-Id']));
          const lookupCandidates = [];
          const pushLookupCand = (c) => {
            const v = String(c || '').trim();
            if (!v) return;
            if (lookupCandidates.indexOf(v) < 0) lookupCandidates.push(v);
          };
          pushLookupCand(orderHdr);
          pushLookupCand(refno);
          pushLookupCand(paysoNoti.pick(body, ['orderNo', 'orderno', 'order']));
          pushLookupCand(order);
          const orderForComp = lookupCandidates[0] || refno || order || '';
          if (!compId && cfg.icopayOrderLookupUrl) {
            for (const cand of lookupCandidates) {
              try {
                const lookupUrl = String(cfg.icopayOrderLookupUrl).replace(
                  /{order}/gi,
                  encodeURIComponent(cand),
                );
                const lr = await axios.get(lookupUrl, { timeout: 8000, validateStatus: () => true });
                const looked = extractIcopayCompIdAndOrderFromNotifyResult({
                  headers: lr.headers || {},
                  data: lr.data,
                });
                let fromData = looked.compId;
                if (!fromData && lr.data && typeof lr.data === 'object') {
                  const nest = lr.data.data && typeof lr.data.data === 'object' ? lr.data.data : lr.data;
                  fromData = String(nest.compId || nest.CompId || nest.merchantId || '').trim();
                }
                if (fromData) {
                  compId = paysoNoti.cleanCompId(fromData);
                  console.log('[PAYSO] order-lookup hit', 'cand=', cand, 'compId=', compId);
                  break;
                }
              } catch (_) {}
            }
          }
          const match = findPaysoMerchant(compId);
          if (!match) {
            appendPgNotiLog({
              routeKey: 'payso/webhook',
              merchantId: paysoNoti.cleanCompId(compId) || '',
              kind: 'callback',
              body: stampPaysoCompIdOnBody(
                paysoNoti.mapPaysoToMerchantNotifyBody(body, orderHdr || orderForComp, {
                  compId: paysoNoti.cleanCompId(compId),
                  trnId: refno,
                }),
                paysoNoti.cleanCompId(compId),
              ),
              rawBody: forwardBody || undefined,
              targetUrl: '',
              contentType: incomingContentType,
              env: 'live',
              relayStatus: 'fail',
              relayFailReason: compId ? 'Merchant not found for Comp-Id' : 'Missing X-Icopay-Comp-Id',
              pgProvider: 'payso',
            });
            return;
          }
          const notifyBody = stampPaysoCompIdOnBody(
            paysoNoti.mapPaysoToMerchantNotifyBody(body, orderHdr || orderForComp, {
              compId: paysoNoti.cleanCompId(compId),
              trnId: refno,
            }),
            paysoNoti.cleanCompId(compId),
          );
          await relayPaysoMerchantNotify(
            match.merchantId,
            match.merchant,
            notifyBody,
            forwardBody,
            incomingContentType,
            { routeKey: 'payso/webhook', forceRelayIfUrl: false },
          );
        })
        .catch((e) => {
          console.error('[PAYSO] merchant notify async error', e && e.message ? e.message : e);
          appendPgNotiLog({
            routeKey: 'payso/webhook',
            merchantId: compIdHdr || '',
            kind: 'callback',
            body: baseNotifyBody,
            rawBody: forwardBody || undefined,
            targetUrl: '',
            contentType: incomingContentType,
            env: 'live',
            relayStatus: 'fail',
            relayFailReason: e && e.message ? e.message : String(e),
            pgProvider: 'payso',
          });
        });
    });
  }

  async function handlePaysoWebhook(req, res) {
    const cfg = loadPaysoIngressConfig();
    const { body, rawBodyStr, incomingContentType } = parseIncomingBody(req);
    const isIcopayOutbound = paysoNoti.looksLikeIcopayUnifiedNotify(body);
    const refno = String(
      paysoNoti.pick(body, ['refno', 'refNo', 'referenceNo', 'trnId', 'pgTxnId', 'transactionId']) || '',
    ).trim();
    const merchantRef = String(
      paysoNoti.pick(body, ['merchantid', 'merchantId', 'compId', 'CompId', 'Comp-Id']) || '',
    ).trim();
    const order = String(
      paysoNoti.pick(body, ['orderNo', 'OrderNo', 'orderno', 'order', 'ref1']) || refno || merchantRef,
    ).trim();

    console.log(
      '[PAYSO] webhook',
      'outbound=',
      isIcopayOutbound ? '1' : '0',
      'refno=',
      refno || '-',
      'order=',
      order || '-',
    );

    let guard = null;
    if (pgIngressGuard && typeof pgIngressGuard.beginInbound === 'function') {
      guard = pgIngressGuard.beginInbound('payso', 'webhook', [refno, order, merchantRef]);
      if (typeof respondPgIngressGuard === 'function' && respondPgIngressGuard(res, guard, 'PAYSO')) {
        return;
      }
    }

    if (!cfg.enabled) {
      if (pgIngressGuard && guard) {
        pgIngressGuard.endInbound('payso', guard.key, { ok: false, httpStatus: 503 });
      }
      return res.status(503).send('PAYSO ingress disabled');
    }

    const forwardBody = buildForwardBody(
      rawBodyStr,
      body,
      incomingContentType,
      isIcopayOutbound,
    );
    const ctx = {
      body,
      rawBodyStr,
      forwardBody,
      incomingContentType,
      guard,
      refno,
      order,
      merchantRef,
    };

    if (isIcopayOutbound) {
      return handleIcopayOutbound(req, res, ctx);
    }
    return handlePaysoOriginPostback(req, res, ctx);
  }

  return {
    loadPaysoIngressConfig,
    forwardPaysoToIcopay,
    relayPaysoMerchantNotify,
    handlePaysoWebhook,
    stampPaysoCompIdOnBody,
  };
}

module.exports = { createPaysoIngress };
