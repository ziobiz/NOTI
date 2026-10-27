'use strict';

const assert = require('assert');
const ilk = require('../lib/ilkNoti');

function run() {
  const body = {
    'clientReferenceInformation.code': 'ORD-QR-1',
    'orderInformation.amountDetails.totalAmount': '100.00',
    'orderInformation.amountDetails.currency': 'JPY',
    'paymentInformation.brand': 'PAYPAY',
    'processingInformation.paymentMethod': 'QR',
    status: 'SUCCESS',
    id: 'ilk-tx-9',
  };
  assert.strictEqual(ilk.ilkShouldNotifyMerchant('back_noti', body), true);
  assert.strictEqual(ilk.ilkIsFailure(body), false);
  const mapped = ilk.mapIlkToMerchantNotifyBody(body, 'back_noti');
  assert.strictEqual(mapped.returncode, '00');
  assert.strictEqual(mapped.orderid, 'ORD-QR-1');
  assert.strictEqual(mapped.transaction_id, 'ilk-tx-9');
  assert.strictEqual(mapped.paymentChannel, 'QR');
  assert.strictEqual(mapped.pgKind, 'ilk');
  assert.strictEqual(mapped.van, 'ILK');

  const fail = ilk.mapIlkToMerchantNotifyBody({ ...body, status: 'DECLINED' }, 'back_noti');
  assert.strictEqual(fail.returncode, '01');
  assert.strictEqual(fail.outcome, 'reject');

  console.log('ilkNoti.test.js OK');
}

run();
