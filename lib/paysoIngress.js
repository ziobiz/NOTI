/**
 * PAYSO postback ingress — OMSPAY/KSNET 패턴.
 * POST /noti/payso → ICOPAY …/PAYSO → Comp-Id 가맹 릴레이 + appendPgNotiLog.
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
        if (omsUrl) icopayNotifyUrl = omsUrl.replace(/\/OMSPAY\b/i, '/PAYSO').replace(/\/omspay\b/i, '/PAYSO');
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
        Math.min(120000, Number(fromFile.timeoutMs) || Number(process.env.PAYSO_ICOPAY_TIMEOUT_MS) || 25000),
      ),
      note: String(fromFile.note || '').trim(),
    };
  }

  function stampPaysoCompIdOnBody(body, compId) {
    return stampOmspayCompIdOnBody(body, compId);
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
    if (extraHeaders && typeof extraHeaders === 'object') {
      Object.assign(headers, extraHeaders);
    }
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

  async function relayPaysoMerchantNotify(merchantId, merchant, notifyBody, rawBody, incomingContentType) {
    const enableRelay = !!(merchant && merchant.enableRelay);
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
            (relayRes.data && typeof relayRes.data === 'string' ? ': ' + String(relayRes.data).slice(0, 200) : '');
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
      routeKey: 'payso/webhook',
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

  async function handlePaysoWebhook(req, res) {
    const cfg = loadPaysoIngressConfig();
    const rawBodyStr = req.rawBodyBuffer ? req.rawBodyBuffer.toString('utf8') : '';
    const incomingContentType =
      (req.get && req.get('Content-Type')) || (req.headers && req.headers['content-type']) || '';
    let body = req.body;
    if (!body || typeof body !== 'object') body = {};
    /* Control Panel POST form + Mode2 GET query */
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
    const refno = String(paysoNoti.pick(body, ['refno', 'refNo', 'referenceNo']) || '').trim();
    const txnId = String(
      paysoNoti.pick(body, ['transactionId', 'transactionid', 'transaction_id']) || '',
    ).trim();
    console.log('[PAYSO] webhook', 'refno=', refno || '-', 'txn=', txnId || '-');

    if (!cfg.enabled) {
      return res.status(503).send('PAYSO ingress disabled');
    }
    if (!cfg.icopayNotifyUrl || /YOUR_INGRESS_TOKEN/i.test(cfg.icopayNotifyUrl)) {
      console.error('[PAYSO] icopayNotifyUrl not configured');
      appendPgNotiLog({
        routeKey: 'payso/webhook',
        merchantId: '',
        kind: 'callback',
        body: Object.assign({ pgKind: 'payso' }, body),
        rawBody: rawBodyStr || undefined,
        targetUrl: '',
        contentType: incomingContentType,
        env: 'live',
        relayStatus: 'fail',
        relayFailReason: 'icopayNotifyUrl_missing',
        pgProvider: 'payso',
      });
      return res.status(503).send('PAYSO ICOPAY URL not configured');
    }

    const forwardBody =
      rawBodyStr && String(rawBodyStr).trim()
        ? rawBodyStr
        : new URLSearchParams(
            Object.entries(body || {}).reduce((acc, [k, v]) => {
              if (v != null && v !== '') acc[k] = String(v);
              return acc;
            }, {}),
          ).toString() || JSON.stringify(body || {});

    const ico = await forwardPaysoToIcopay(
      forwardBody,
      incomingContentType || 'application/x-www-form-urlencoded',
      1,
      {},
    );
    const extracted = extractIcopayCompIdAndOrderFromNotifyResult(ico);
    const compIdHdr = extracted.compId;
    const orderHdr = extracted.orderNo;

    const outStatus = ico.status && Number.isFinite(Number(ico.status)) ? Number(ico.status) : 502;
    const outCt =
      paysoNoti.headerGetIgnoreCase(ico.headers, 'content-type') || 'application/json; charset=utf-8';
    const outBody = ico.data != null ? ico.data : '';
    console.log(
      '[PAYSO] passthrough',
      'refno=',
      refno || '-',
      'http=',
      outStatus,
      'compId=',
      compIdHdr || '(none)',
      'order=',
      orderHdr || '-',
    );
    res.status(outStatus);
    res.set('Content-Type', outCt);
    res.send(outBody);

    const baseNotifyBody = stampPaysoCompIdOnBody(
      paysoNoti.mapPaysoToMerchantNotifyBody(body, orderHdr, { compId: compIdHdr }),
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
          let compId = compIdHdr || String(body.compId || body.CompId || body.merchantid || '').trim();
          const lookupCandidates = [];
          const pushLookupCand = (c) => {
            const v = String(c || '').trim();
            if (!v) return;
            if (lookupCandidates.indexOf(v) < 0) lookupCandidates.push(v);
          };
          pushLookupCand(orderHdr);
          pushLookupCand(paysoNoti.pick(body, ['orderNo', 'order', 'note']));
          pushLookupCand(txnId);
          pushLookupCand(refno);
          const orderForComp = lookupCandidates[0] || refno || '';
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
                  compId = String(fromData).split('#')[0].trim();
                  console.log('[PAYSO] order-lookup hit', 'cand=', cand, 'compId=', compId);
                  break;
                }
              } catch (_) {}
            }
          }
          const compIdBase = String(compId || '').split('#')[0].trim();
          let match = findMerchantByCompIdForCrypto(compIdBase);
          if (compIdBase) {
            const slot = MERCHANTS.get(compIdBase + '#payso');
            if (slot && String(slot.callbackUrl || slot.resultUrl || '').trim()) {
              match = { merchantId: compIdBase + '#payso', merchant: slot, via: 'payso-slot' };
            }
          }
          if (!match) {
            console.warn(
              '[PAYSO] merchant notify skipped',
              'compId=',
              compIdBase || '(none)',
              'order=',
              orderForComp || '-',
            );
            appendPgNotiLog({
              routeKey: 'payso/webhook',
              merchantId: compIdBase || '',
              kind: 'callback',
              body: stampPaysoCompIdOnBody(
                paysoNoti.mapPaysoToMerchantNotifyBody(body, orderHdr || orderForComp, {
                  compId: compIdBase,
                }),
                compIdBase,
              ),
              rawBody: forwardBody || undefined,
              targetUrl: '',
              contentType: incomingContentType,
              env: 'live',
              relayStatus: 'fail',
              relayFailReason: compIdBase ? 'Merchant not found for Comp-Id' : 'Missing X-Icopay-Comp-Id',
              pgProvider: 'payso',
            });
            return;
          }
          const notifyBody = stampPaysoCompIdOnBody(
            paysoNoti.mapPaysoToMerchantNotifyBody(body, orderHdr || orderForComp, {
              compId: compIdBase,
            }),
            compIdBase,
          );
          try {
            await relayPaysoMerchantNotify(
              match.merchantId,
              match.merchant,
              notifyBody,
              forwardBody,
              incomingContentType,
            );
          } catch (e) {
            console.error('[PAYSO] merchant relay error', e.message || e);
            appendPgNotiLog({
              routeKey: 'payso/webhook',
              merchantId: match.merchantId || compId || '',
              kind: 'callback',
              body: notifyBody,
              rawBody: forwardBody || undefined,
              targetUrl: String((match.merchant && match.merchant.callbackUrl) || '').trim(),
              contentType: incomingContentType,
              env: 'live',
              relayStatus: 'fail',
              relayFailReason: e.message || String(e),
              pgProvider: 'payso',
            });
          }
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

  return { loadPaysoIngressConfig, handlePaysoWebhook };
}

module.exports = { createPaysoIngress };
