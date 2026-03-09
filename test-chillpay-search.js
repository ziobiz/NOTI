// Simple ChillPay Search Payment test script
// Usage: node test-chillpay-search.js

const axios = require('axios');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const CONFIG_PATH = path.join(__dirname, 'config', 'chillpay-transaction.json');

function loadConfig() {
  const raw = fs.readFileSync(CONFIG_PATH, 'utf8');
  return JSON.parse(raw);
}

function buildChecksum(params, md5Key) {
  const concatStr =
    (params.OrderBy || '') +
    (params.OrderDir || '') +
    String(params.PageSize || '') +
    String(params.PageNumber || '') +
    (params.SearchKeyword || '') +
    (params.MerchantCode || '') +
    (params.PaymentChannel || '') +
    (params.RouteNo != null ? String(params.RouteNo) : '') +
    (params.OrderNo || '') +
    (params.Status || '') +
    (params.TransactionDateFrom || '') +
    (params.TransactionDateTo || '') +
    (params.PaymentDateFrom || '') +
    (params.PaymentDateTo || '') +
    (md5Key || '');

  return crypto
    .createHash('md5')
    .update(concatStr, 'utf8')
    .digest('hex')
    .toLowerCase();
}

async function main() {
  const cfg = loadConfig();
  const cred = cfg.production;

  const body = {
    OrderBy: 'TransactionId',
    OrderDir: 'DESC',
    PageSize: 50,
    PageNumber: 1,
    SearchKeyword: '',
    MerchantCode: cred.mid,
    PaymentChannel: '',
    RouteNo: null,
    OrderNo: '',
    Status: '',
    // 5일~10일 범위 (TransactionDate / PaymentDate 동일 구간)
    TransactionDateFrom: '05/03/2026 00:00:00',
    TransactionDateTo: '10/03/2026 23:59:59',
    PaymentDateFrom: '05/03/2026 00:00:00',
    PaymentDateTo: '10/03/2026 23:59:59',
  };

  body.Checksum = buildChecksum(body, cred.md5);

  console.log('== Request body ==');
  console.log(JSON.stringify(body, null, 2));

  try {
    const res = await axios.post(
      'https://api-transaction.chillpay.co/api/v1/payment/search',
      body,
      {
        headers: {
          'Content-Type': 'application/json',
          'CHILLPAY-MerchantCode': cred.mid,
          'CHILLPAY-ApiKey': cred.apiKey,
        },
        timeout: 20000,
      },
    );

    console.log('\n== Response summary ==');
    console.log('HTTP status:', res.status);
    console.log('status:', res.data && res.data.status);
    console.log('message:', res.data && res.data.message);
    console.log('totalRecord:', res.data && res.data.totalRecord);
    console.log('pageSize:', res.data && res.data.pageSize);
    console.log('pageNumber:', res.data && res.data.pageNumber);

    if (Array.isArray(res.data && res.data.data)) {
      console.log('\n== First 3 records (if any) ==');
      console.log(JSON.stringify(res.data.data.slice(0, 3), null, 2));
    }
  } catch (err) {
    console.log('\n== Error calling API ==');
    console.log(err.message);
    if (err.response && err.response.data) {
      console.log(JSON.stringify(err.response.data, null, 2));
    }
  }
}

main().catch((e) => {
  console.error('Unexpected error:', e);
});

