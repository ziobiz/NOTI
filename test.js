const crypto = require('crypto');

// 1. 방금 에러가 났던 고객님의 완벽한 페이로드
const payload = {
  "OrderNo": "TEST-1772730167190",
  "CustomerId": "CUST-001",
  "Amount": 1500,
  "PhoneNumber": "0911111111",
  "Description": "TEST",
  "ChannelCode": "creditcard",
  "Currency": "764",
  "LangCode": "EN",
  "RouteNo": 2,
  "IPAddress": "1.239.164.57",
  "TokenType": "DT",
  "CreditToken": null,
  "DirectCreditToken": "dfqUHNl63kiPrXxS-mQaR40L0H-hOzff",
  "CreditMonth": null,
  "ShopID": null,
  "CustEmail": "test@sample.com",
  "SaveCard": "N"
};

// 2. 알려주신 MD5 Key (공백 제거 적용)
const secretKey = "OsU7pztJGtGTL4qIArwpJnnyrre2N4FVMO9g8cl9ualLw3VFcghL4o6RULt03xuePAGu0fRLJ0EifPm2E6jVrdBFiJoyV3Ob19pxl4Gwk1FDpZBYfCX36AvVizDBBzXFh9uUAuKcbmrCaTq0kudgHbSGlRIi0M83WMH5f".trim();

const parseVal = (val) => (val === null || val === undefined ? '' : String(val));

// 3. 17개 파라미터 규칙대로 결합
const concatData =
    parseVal(payload.OrderNo) +
    parseVal(payload.CustomerId) +
    parseVal(payload.Amount) +
    parseVal(payload.PhoneNumber) +
    parseVal(payload.Description) +
    parseVal(payload.ChannelCode) +
    parseVal(payload.Currency) +
    parseVal(payload.LangCode) +
    parseVal(payload.RouteNo) +
    parseVal(payload.IPAddress) +
    parseVal(payload.TokenType) +
    parseVal(payload.CreditToken) +
    parseVal(payload.DirectCreditToken) +
    parseVal(payload.CreditMonth) +
    parseVal(payload.ShopID) +
    parseVal(payload.CustEmail) +
    parseVal(payload.SaveCard);

const finalString = concatData + secretKey;
const checkSum = crypto.createHash('md5').update(finalString).digest('hex');

console.log("--------------------------------------------------");
console.log("1. 스크립트가 계산한 CheckSum :", checkSum);
console.log("2. 고객님이 PG사로 보낸 CheckSum : e1a98c3a52e51f7ce6b6d6b51691ccc8");
console.log("3. 일치 여부 :", checkSum === "e1a98c3a52e51f7ce6b6d6b51691ccc8");
console.log("--------------------------------------------------");