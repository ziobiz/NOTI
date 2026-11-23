/**
 * TAZAPAY ingress —
 * A) ICOPAY 아웃바운드(event=pg.payment.status) → ICOPAY 재포워드 금지, 가맹 callbackUrl 릴레이만
 * B) Tazapay 원본 JSON webhook → 서명 검증 → ICOPAY …/TAZAPAY 포워드 + Comp-Id + 가맹 릴레이
 * 매칭 실패 건도 appendPgNotiLog (pgProvider: tazapay)
 */
'use strict';

const path = require('path');
const axios = require('axios');
const tazapayNoti = require('./tazapayNoti');

function createTazapayIngress(deps) {
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

  const TAZAPAY_INGRESS_CONFIG_PATH = path.join(CONFIG_DIR, 'tazapay-ingress.json');

  function loadTazapayIngressConfig() {
    const fromFile = loadJsonConfig(TAZAPAY_INGRESS_CONFIG_PATH, {}) || {};
    const envUrl = String(process.env.TAZAPAY_ICOPAY_NOTIFY_URL || '').trim();
    const envLookup = String(process.env.TAZAPAY_ICOPAY_ORDER_LOOKUP_URL || '').trim();
    const envSecret = String(process.env.TAZAPAY_WEBHOOK_SECRET || '').trim();
    let icopayNotifyUrl = envUrl || String(fromFile.icopayNotifyUrl || '').trim();
    if (!icopayNotifyUrl || /YOUR_INGRESS_TOKEN/i.test(icopayNotifyUrl)) {
      try {
        const oms = loadOmspayIngressConfig();
        const omsUrl = String((oms && oms.icopayNotifyUrl) || '').trim();
        if (omsUrl && !/YOUR_INGRESS_TOKEN/i.test(omsUrl)) {
          icopayNotifyUrl = omsUrl
            .replace(/\/OMSPAY\b/i, '/TAZAPAY')
            .replace(/\/PAYSO\b/i, '/TAZAPAY')
            .replace(/\/KSNET\b/i, '/TAZAPAY');
        }
      } catch (_) {}
    }
    if (!icopayNotifyUrl || /YOUR_INGRESS_TOKEN/i.test(icopayNotifyUrl)) {
      icopayNotifyUrl = '';
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
      webhookSecret: envSecret || String(fromFile.webhookSecret || '').trim(),
      requireSignature: fromFile.requireSignature !== false,
      timeoutMs: Math.max(
        3000,
        Math.min(60000, Number(fromFile.timeoutMs) || Number(process.env.TAZAPAY_ICOPAY_TIMEOUT_MS) || 15000),
      ),
      note: String(fromFile.note || '').trim(),
    };
  }

  function stampTazapayCompIdOnBody(body, compId) {
    if (typeof stampOmspayCompIdOnBody === 'function') {
      return stampOmspayCompIdOnBody(body, compId);
    }
    const id = tazapayNoti.cleanCompId(compId);
    if (!id || !body || typeof body !== 'object') return body || {};
    return Object.assign({}, body, { compId: id, CompId: id, 'Comp-Id': id, merchantId: id });
  }

  function parseIncomingBody(req) {
    const incomingContentType =
      (req.get && req.get('Content-Type')) || (req.headers && req.headers['content-type']) || '';
    const rawBodyStr = req.rawBodyBuffer ? req.rawBodyBuffer.toString('utf8') : '';
    let body = req.body;
    if (!body || typeof body !== 'object') body = {};
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

  async function forwardTazapayToIcopay(rawBodyStr, contentType, attempt, extraHeaders) {
    const cfg = loadTazapayIngressConfig();
    if (!cfg.enabled) {
      return { ok: false, status: 503, data: 'TAZAPAY ingress disabled', headers: {} };
    }
    if (!cfg.icopayNotifyUrl) {
      return { ok: false, status: 503, data: 'TAZAPAY ICOPAY notify URL not configured', headers: {} };
    }
    const headers = {
      'Content-Type':
        contentType && String(contentType).trim()
          ? String(contentType).split(';')[0].trim() + '; charset=utf-8'
          : 'application/json; charset=utf-8',
    };
    if (extraHeaders && typeof extraHeaders === 'object') Object.assign(headers, extraHeaders);
    applyIcopayPgNotifyIngressHeaders(headers, cfg.icopayNotifyUrl, attempt || 1);
    if (!headers['X-Icopay-Notify-Delivery']) {
      headers['X-Icopay-Notify-Delivery'] = (attempt || 1) <= 1 ? 'LIVE' : 'RETRY';
      headers['X-Noti-Attempt'] = String(Math.max(1, Math.floor(Number(attempt) || 1)));
    }
    try {
      const res = await axios.post(cfg.icopayNotifyUrl, rawBodyStr || '{}', {
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
      console.error('[TAZAPAY] forward error', e.message || e);
      return { ok: false, status: 502, data: String(e.message || e), headers: {} };
    }
  }

  function findTazapayMerchant(compId) {
    const compIdBase = tazapayNoti.cleanCompId(compId);
    if (!compIdBase) return null;
    let match = findMerchantByCompIdForCrypto(compIdBase);
    const slot = MERCHANTS.get(compIdBase + '#tazapay');
    if (slot && String(slot.callbackUrl || slot.resultUrl || '').trim()) {
      match = { merchantId: compIdBase + '#tazapay', merchant: slot, via: 'tazapay-slot' };
    }
    return match;
  }

  /**
   * @param {object} opts
   * @param {string} [opts.routeKey]
   * @param {boolean} [opts.forceRelayIfUrl]
   */
  async function relayTazapayMerchantNotify(
    merchantId,
    merchant,
    notifyBody,
    rawBody,
    incomingContentType,
    opts,
  ) {
    const routeKey = (opts && opts.routeKey) || 'tazapay/webhook';
    const forceRelayIfUrl = !!(opts && opts.forceRelayIfUrl);
    const enableRelay = !!(merchant && (merchant.enableRelay || forceRelayIfUrl));
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
            (relayRes.data && typeof relayRes.data === 'string'
              ? ': ' + String(relayRes.data).slice(0, 200)
              : '');
          await new Promise((r) => setTimeout(r, 2000));
          relayRes = await relayToMerchant(targetUrl, notifyBody, relayOpts);
          relaySuccess = relayRes.status >= 200 && relayRes.status < 400;
          if (relaySuccess) relayFailReason = '';
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
      pgProvider: 'tazapay',
    });
    return { relaySuccess, relayFailReason };
  }

  async function handleIcopayOutbound(req, res, ctx) {
    const { body, forwardBody, incomingContentType, guard } = ctx;
    const compIdHdr = tazapayNoti.cleanCompId(
      tazapayNoti.pick(body, ['compId', 'CompId', 'Comp-Id', 'merchantId']) || '',
    );
    const orderHdr = String(tazapayNoti.pick(body, ['orderNo', 'OrderNo']) || '').trim();
    const trnId = String(
      tazapayNoti.pick(body, ['trnId', 'pgTxnId', 'transactionId', 'TransactionId']) || '',
    ).trim();
    const paid = tazapayNoti.tazapayIsPaid(body);

    if (pgIngressGuard && guard) {
      pgIngressGuard.endInbound('tazapay', guard.key, { ok: true, httpStatus: 200 });
    }
    console.log(
      '[TAZAPAY] icopay-outbound (no re-forward)',
      'trnId=',
      trnId || '-',
      'order=',
      orderHdr || '-',
      'compId=',
      compIdHdr || '(none)',
      'paid=',
      paid ? '1' : '0',
    );

    res.status(200).json({
      ok: true,
      accepted: true,
      source: 'icopay-outbound',
      orderNo: orderHdr || '',
      pgTxnId: trnId || '',
      paymentStatus: paid ? 'PAID' : 'FAILED',
    });

    const notifyBody = stampTazapayCompIdOnBody(
      tazapayNoti.mapTazapayToMerchantNotifyBody(body, orderHdr, { compId: compIdHdr, trnId }),
      compIdHdr,
    );

    setImmediate(() => {
      Promise.resolve()
        .then(async () => {
          const match = findTazapayMerchant(compIdHdr);
          if (!match) {
            appendPgNotiLog({
              routeKey: 'tazapay/icopay-outbound',
              merchantId: compIdHdr || '',
              kind: 'callback',
              body: notifyBody,
              rawBody: forwardBody || undefined,
              targetUrl: '',
              contentType: incomingContentType,
              env: 'live',
              relayStatus: 'fail',
              relayFailReason: compIdHdr ? 'Merchant not found for Comp-Id' : 'Missing Comp-Id',
              pgProvider: 'tazapay',
            });
            return;
          }
          await relayTazapayMerchantNotify(
            match.merchantId,
            match.merchant,
            stampTazapayCompIdOnBody(notifyBody, compIdHdr),
            forwardBody,
            incomingContentType,
            { routeKey: 'tazapay/icopay-outbound', forceRelayIfUrl: true },
          );
        })
        .catch((e) => {
          console.error('[TAZAPAY] icopay-outbound relay error', e && e.message ? e.message : e);
          appendPgNotiLog({
            routeKey: 'tazapay/icopay-outbound',
            merchantId: compIdHdr || '',
            kind: 'callback',
            body: notifyBody,
            rawBody: forwardBody || undefined,
            targetUrl: '',
            contentType: incomingContentType,
            env: 'live',
            relayStatus: 'fail',
            relayFailReason: e && e.message ? e.message : String(e),
            pgProvider: 'tazapay',
          });
        });
    });
  }

  async function handleTazapayOriginWebhook(req, res, ctx) {
    const { body, rawBodyStr, forwardBody, incomingContentType, guard, order, txId } = ctx;
    const cfg = loadTazapayIngressConfig();

    if (cfg.requireSignature) {
      const ver = tazapayNoti.verifyTazapayWebhookSignature(rawBodyStr || forwardBody, req.headers, cfg.webhookSecret);
      if (!ver.ok) {
        console.warn('[TAZAPAY] signature verify failed', ver.reason || '-');
        appendPgNotiLog({
          routeKey: 'tazapay/webhook',
          merchantId: '',
          kind: 'callback',
          body: Object.assign({ pgKind: 'tazapay', signatureOk: false, signatureFailReason: ver.reason }, body),
          rawBody: rawBodyStr || undefined,
          targetUrl: '',
          contentType: incomingContentType,
          env: 'live',
          relayStatus: 'fail',
          relayFailReason: 'signature_' + (ver.reason || 'invalid'),
          pgProvider: 'tazapay',
        });
        if (pgIngressGuard && guard) {
          pgIngressGuard.endInbound('tazapay', guard.key, { ok: false, httpStatus: 401 });
        }
        return res.status(401).json({ ok: false, error: 'invalid_signature', reason: ver.reason || 'invalid' });
      }
    }

    if (!cfg.icopayNotifyUrl) {
      console.error('[TAZAPAY] icopayNotifyUrl not configured');
      appendPgNotiLog({
        routeKey: 'tazapay/webhook',
        merchantId: '',
        kind: 'callback',
        body: Object.assign({ pgKind: 'tazapay' }, body),
        rawBody: rawBodyStr || undefined,
        targetUrl: '',
        contentType: incomingContentType,
        env: 'live',
        relayStatus: 'fail',
        relayFailReason: 'icopayNotifyUrl_missing',
        pgProvider: 'tazapay',
      });
      if (pgIngressGuard && guard) {
        pgIngressGuard.endInbound('tazapay', guard.key, { ok: false, httpStatus: 503 });
      }
      return res.status(503).send('TAZAPAY ICOPAY URL not configured');
    }

    let ico;
    try {
      ico = await forwardTazapayToIcopay(forwardBody, incomingContentType || 'application/json', 1);
    } catch (eFwd) {
      if (pgIngressGuard && guard) {
        pgIngressGuard.endInbound('tazapay', guard.key, { ok: false, httpStatus: 502 });
      }
      throw eFwd;
    }

    const extracted = extractIcopayCompIdAndOrderFromNotifyResult(ico);
    const compIdHdr = tazapayNoti.cleanCompId(extracted.compId);
    const orderHdr = extracted.orderNo;
    const outStatus = ico.status && Number.isFinite(Number(ico.status)) ? Number(ico.status) : 502;
    const outCt =
      tazapayNoti.headerGetIgnoreCase(ico.headers, 'content-type') || 'application/json; charset=utf-8';
    const outBody = ico.data != null ? ico.data : '';
    const forwardOk = outStatus >= 200 && outStatus < 300;
    if (pgIngressGuard && guard) {
      pgIngressGuard.endInbound('tazapay', guard.key, { ok: forwardOk, httpStatus: outStatus });
    }
    console.log(
      '[TAZAPAY] passthrough',
      'tx=',
      txId || '-',
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

    const baseNotifyBody = stampTazapayCompIdOnBody(
      tazapayNoti.mapTazapayToMerchantNotifyBody(body, orderHdr || order, {
        compId: compIdHdr,
        trnId: txId,
      }),
      compIdHdr,
    );

    if (!tazapayNoti.tazapayShouldNotifyMerchant(body)) {
      appendPgNotiLog({
        routeKey: 'tazapay/webhook',
        merchantId: compIdHdr || '',
        kind: 'callback',
        body: baseNotifyBody,
        rawBody: forwardBody || undefined,
        targetUrl: '',
        contentType: incomingContentType,
        env: 'live',
        relayStatus: 'skip',
        relayFailReason: 'event_skip',
        pgProvider: 'tazapay',
      });
      return;
    }

    setImmediate(() => {
      Promise.resolve()
        .then(async () => {
          let compId =
            compIdHdr ||
            tazapayNoti.cleanCompId(tazapayNoti.pick(body, ['compId', 'CompId', 'Comp-Id', 'merchantId']));
          const lookupCandidates = [];
          const pushLookupCand = (c) => {
            const v = String(c || '').trim();
            if (!v) return;
            if (lookupCandidates.indexOf(v) < 0) lookupCandidates.push(v);
          };
          pushLookupCand(orderHdr);
          pushLookupCand(order);
          pushLookupCand(txId);
          pushLookupCand(tazapayNoti.pickTazapayOrderNo(body));
          const orderForComp = lookupCandidates[0] || order || txId || '';
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
                  compId = tazapayNoti.cleanCompId(fromData);
                  console.log('[TAZAPAY] order-lookup hit', 'cand=', cand, 'compId=', compId);
                  break;
                }
              } catch (_) {}
            }
          }
          const match = findTazapayMerchant(compId);
          if (!match) {
            appendPgNotiLog({
              routeKey: 'tazapay/webhook',
              merchantId: tazapayNoti.cleanCompId(compId) || '',
              kind: 'callback',
              body: stampTazapayCompIdOnBody(
                tazapayNoti.mapTazapayToMerchantNotifyBody(body, orderHdr || orderForComp, {
                  compId: tazapayNoti.cleanCompId(compId),
                  trnId: txId,
                }),
                tazapayNoti.cleanCompId(compId),
              ),
              rawBody: forwardBody || undefined,
              targetUrl: '',
              contentType: incomingContentType,
              env: 'live',
              relayStatus: 'fail',
              relayFailReason: compId ? 'Merchant not found for Comp-Id' : 'Missing X-Icopay-Comp-Id',
              pgProvider: 'tazapay',
            });
            return;
          }
          const notifyBody = stampTazapayCompIdOnBody(
            tazapayNoti.mapTazapayToMerchantNotifyBody(body, orderHdr || orderForComp, {
              compId: tazapayNoti.cleanCompId(compId),
              trnId: txId,
            }),
            tazapayNoti.cleanCompId(compId),
          );
          await relayTazapayMerchantNotify(
            match.merchantId,
            match.merchant,
            notifyBody,
            forwardBody,
            incomingContentType,
            { routeKey: 'tazapay/webhook', forceRelayIfUrl: false },
          );
        })
        .catch((e) => {
          console.error('[TAZAPAY] merchant notify async error', e && e.message ? e.message : e);
          appendPgNotiLog({
            routeKey: 'tazapay/webhook',
            merchantId: compIdHdr || '',
            kind: 'callback',
            body: baseNotifyBody,
            rawBody: forwardBody || undefined,
            targetUrl: '',
            contentType: incomingContentType,
            env: 'live',
            relayStatus: 'fail',
            relayFailReason: e && e.message ? e.message : String(e),
            pgProvider: 'tazapay',
          });
        });
    });
  }

  async function handleTazapayWebhook(req, res) {
    const cfg = loadTazapayIngressConfig();
    const { body, rawBodyStr, incomingContentType } = parseIncomingBody(req);
    const isIcopayOutbound = tazapayNoti.looksLikeIcopayUnifiedNotify(body);
    const txId = tazapayNoti.pickTazapayTxnId(body);
    const order = tazapayNoti.pickTazapayOrderNo(body) || txId;
    const evt = tazapayNoti.tazapayEventType(body);

    console.log(
      '[TAZAPAY] webhook',
      'outbound=',
      isIcopayOutbound ? '1' : '0',
      'type=',
      evt || '-',
      'tx=',
      txId || '-',
      'order=',
      order || '-',
    );

    let guard = null;
    if (pgIngressGuard && typeof pgIngressGuard.beginInbound === 'function') {
      guard = pgIngressGuard.beginInbound('tazapay', 'webhook', [txId, order, evt]);
      if (typeof respondPgIngressGuard === 'function' && respondPgIngressGuard(res, guard, 'TAZAPAY')) {
        return;
      }
    }

    if (!cfg.enabled) {
      if (pgIngressGuard && guard) {
        pgIngressGuard.endInbound('tazapay', guard.key, { ok: false, httpStatus: 503 });
      }
      return res.status(503).send('TAZAPAY ingress disabled');
    }

    const forwardBody = rawBodyStr && String(rawBodyStr).trim() ? rawBodyStr : JSON.stringify(body || {});
    const ctx = {
      body,
      rawBodyStr,
      forwardBody,
      incomingContentType,
      guard,
      txId,
      order,
    };

    if (isIcopayOutbound) {
      return handleIcopayOutbound(req, res, ctx);
    }
    return handleTazapayOriginWebhook(req, res, ctx);
  }

  async function handleTazapayBrowserResult(req, res) {
    const q = req.query && typeof req.query === 'object' ? req.query : {};
    const b = req.body && typeof req.body === 'object' ? req.body : {};
    const body = Object.assign({}, q, b);
    let compId = tazapayNoti.cleanCompId(
      tazapayNoti.pick(body, ['compId', 'CompId', 'Comp-Id', 'merchantId']),
    );
    const order = tazapayNoti.pickTazapayOrderNo(body);
    const cfg = loadTazapayIngressConfig();
    if (!compId && order && cfg.icopayOrderLookupUrl) {
      try {
        const lookupUrl = String(cfg.icopayOrderLookupUrl).replace(/{order}/gi, encodeURIComponent(order));
        const lr = await axios.get(lookupUrl, { timeout: 8000, validateStatus: () => true });
        const looked = extractIcopayCompIdAndOrderFromNotifyResult({
          headers: lr.headers || {},
          data: lr.data,
        });
        if (looked.compId) compId = tazapayNoti.cleanCompId(looked.compId);
      } catch (_) {}
    }
    let match = findTazapayMerchant(compId);
    const notifyBody = stampTazapayCompIdOnBody(
      tazapayNoti.mapTazapayToMerchantNotifyBody(body, order, { compId }),
      compId,
    );
    const incomingContentType =
      (req.get && req.get('Content-Type')) ||
      (req.headers && req.headers['content-type']) ||
      'application/json';
    appendPgNotiLog({
      routeKey: 'tazapay/result',
      merchantId: (match && match.merchantId) || compId || '',
      kind: 'result',
      body: notifyBody,
      rawBody: undefined,
      targetUrl: match && match.merchant ? String(match.merchant.resultUrl || '').trim() : '',
      contentType: incomingContentType,
      env: 'live',
      relayStatus: 'skip',
      relayFailReason: '',
      pgProvider: 'tazapay',
    });
    if (match && match.merchant && match.merchant.resultUrl) {
      const dest = String(match.merchant.resultUrl).trim();
      try {
        const u = new URL(dest);
        for (const [k, v] of Object.entries(notifyBody || {})) {
          if (v == null || typeof v === 'object') continue;
          u.searchParams.set(k, String(v));
        }
        return res.redirect(302, u.toString());
      } catch (_) {
        return res.redirect(302, dest);
      }
    }
    return res.status(200).json({ ok: true, pg: 'tazapay', orderNo: order || '', body: notifyBody });
  }

  return {
    loadTazapayIngressConfig,
    forwardTazapayToIcopay,
    relayTazapayMerchantNotify,
    handleTazapayWebhook,
    handleTazapayBrowserResult,
    stampTazapayCompIdOnBody,
  };
}

module.exports = { createTazapayIngress };
