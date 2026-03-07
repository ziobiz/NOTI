# ChillPay DirectCredit API 연동 시 400 / System Error(3001) 점검 사항

가맹점에서 `sandbox-api-directcredit.chillpay.co/api/v1/payment` 호출 시  
**HTTP 400**, **Response: {"status": 3001, "message": "System Error"}** 가 나는 경우,  
아래 항목을 순서대로 확인하는 것을 권장합니다.

---

## 1. CheckSum / ConcatData 규칙 (가장 흔한 원인)

ChillPay 측에서 **CheckSum 검증 실패** 시 3001 System Error를 반환하는 경우가 많습니다.

### 1.1 ConcatData에 넣는 필드와 **순서** (17개, 구분자 없이 이어 붙임)

| 순서 | 필드명 | 비고 |
|------|--------|------|
| 1 | OrderNo | |
| 2 | CustomerId | |
| 3 | Amount | 숫자 → 문자열 (예: 10000 → "10000") |
| 4 | PhoneNumber | 없으면 빈 문자열 "" |
| 5 | Description | |
| 6 | ChannelCode | 예: "creditcard" |
| 7 | Currency | 예: "764" |
| 8 | LangCode | 예: "EN" |
| 9 | RouteNo | 숫자 → 문자열 (예: 16 → "16") |
| 10 | IPAddress | |
| 11 | TokenType | 예: "DT" |
| 12 | CreditToken | 없으면 "" |
| 13 | DirectCreditToken | 결제 토큰 |
| 14 | CreditMonth | 없으면 "" |
| 15 | ShopID | 없으면 "" |
| 16 | CustEmail | |
| 17 | SaveCard | 예: "N" |

- **MerchantCode, ApiKey는 ConcatData에 넣지 않습니다.**  
  (헤더 등 별도 필드로 보내는 API라면, CheckSum 계산 문자열에는 포함하지 않음.)
- 필드를 **빼거나 순서를 바꾸면** CheckSum이 달라져 3001이 날 수 있습니다.

### 1.2 CheckSum 계산 식

```
ConcatData = OrderNo + CustomerId + Amount + PhoneNumber + Description
           + ChannelCode + Currency + LangCode + RouteNo + IPAddress
           + TokenType + CreditToken + DirectCreditToken + CreditMonth
           + ShopID + CustEmail + SaveCard

FinalString = ConcatData + MD5Key   // 중간에 공백/줄바꿈 없음
CheckSum    = MD5(FinalString)      // 소문자 hex 32자
```

- `null`/미입력 필드는 **빈 문자열 `""`** 로 concatenate.
- MD5Key는 ChillPay에서 발급한 키를 **앞뒤 공백 제거 후** 그대로 붙입니다.

### 1.3 가맹점 ConcatData 예시와의 비교

가맹점이 보낸 예시:

```
ConcatData: KL2026030715565723TESTCUST00110000Inline Test Payment
            creditcard764EN16180.150.140.196DT
            zOomuRZ83kiNo3czB1SmQoOXEnP6Kij_
            test@asyong.comN + MD5Key
```

- **"KL"** 이 맨 앞에 있으면:  
  ChillPay DirectCredit 규격에서는 **ConcatData에 MerchantCode를 넣지 않음**.  
  → **KL 제거** 후 다시 ConcatData 조합 및 CheckSum 계산 필요.
- **"+ MD5Key"** 는 실제 구현에서 `ConcatData + MD5Key` 한 문자열을 만든 뒤 MD5 해시해야 하며,  
  문자 그대로 `" + MD5Key"` 를 붙이면 안 됩니다.

---

## 2. API 호출 방식 (우리 연동 기준)

우리 측에서는 아래와 같이 호출하고 있으며, 이 구조는 문제 없이 동작합니다.

- **Content-Type**: `application/json`
- **Body**: JSON (OrderNo, CustomerId, Amount, DirectCreditToken, CheckSum 등 위 17개 필드 + CheckSum)
- **헤더** (ChillPay가 헤더로 받는 경우):
  - `CHILLPAY-MerchantCode`: 가맹점 코드
  - `CHILLPAY-ApiKey`: API Key

가맹점이 사용하는 URL이 `sandbox-api-directcredit.chillpay.co/api/v1/payment` 인 경우,  
해당 **v1/payment** 스펙이 위와 동일한지(필드명, 헤더, ConcatData/CheckSum 규칙) 반드시 확인해야 합니다.  
문서 버전/API 버전이 다르면 필드 개수·순서가 달라질 수 있습니다.

---

## 3. 점검 체크리스트 (가맹점 전달용)

- [ ] ConcatData에 **MerchantCode를 포함하지 않았는지**
- [ ] ConcatData **17개 필드 순서**가 매뉴얼/우리 순서와 동일한지
- [ ] 빈 값 필드(PhoneNumber, CreditToken, CreditMonth, ShopID 등)를 **빈 문자열 `""`로라도 포함**했는지
- [ ] CheckSum = MD5(ConcatData + MD5Key), **중간 공백/줄바꿈 없이** 한 문자열로 붙였는지
- [ ] MD5Key에 **앞뒤 공백**이 들어가 있지 않은지
- [ ] Amount, RouteNo를 **숫자를 문자열로 변환**한 값으로 넣었는지 (예: 10000 → "10000", 16 → "16")
- [ ] 사용 중인 **API 문서 버전**과 **엔드포인트**가 동일한지 (v1/payment 스펙과 우리 연동 스펙 비교)

위를 모두 확인한 뒤에도 3001이 나오면, ChillPay 측에  
**사용 중인 API 문서 버전·엔드포인트**와 **ConcatData/CheckSum 규격**을 문의하는 것이 좋습니다.

---

## 4. 가맹점 요청 예시 분석 (KL2026030718531573 / 3001 오류)

가맹점이 보낸 요청 기준으로 점검한 내용입니다.

### 4.1 Request Body / ConcatData 요약

- **OrderNo**: `KL2026030718531573` (앞에 "KL" 포함)
- **CreditMonth**: `null`, **ShopID**: `null`
- 가맹점 ConcatData 문자열:  
  `"KL2026030718531573TESTCUST00110000Inline Test Paymentcreditcard764EN16180.150.140.196DTDYMcWi983ki2umHYnMWBSoObSU7p-8fVtest@asyong.comN"`

### 4.2 확인된 이슈

1. **ConcatData에 null 필드 누락 가능성**  
   - **CreditMonth**, **ShopID**가 JSON에서는 `null`인데, ConcatData를 만들 때 **null이면 아무 것도 붙이지 않는** 식으로 구현했을 수 있습니다.  
   - 규격상으로는 **17개 필드를 순서대로** 모두 이어 붙여야 하고, `null`/빈 값도 **반드시 빈 문자열 `""`로** 넣어야 합니다.  
   - 즉, ConcatData 생성 시 `CreditMonth`, `ShopID`를 **건너뛰지 말고** `""`를 붙여야 합니다.  
     (코드에서 `if (value == null) continue;` 같은 식으로 필드를 생략하면 안 됩니다.)

2. **OrderNo 앞 "KL"**  
   - 일부 연동에서는 OrderNo를 **순수 주문번호만** 쓰고, ConcatData에도 동일한 값만 넣습니다.  
   - 현재는 OrderNo에 "KL"이 붙어 있는데, ChillPay 쪽에서 **OrderNo를 다른 형식으로 정규화해서** CheckSum을 계산할 수 있으므로,  
     **OrderNo를 "2026030718531573"만 사용**해 보는 것도 시도해 볼 만합니다.  
     (이때는 Request Body의 `OrderNo`와 ConcatData의 1번 필드를 **완전히 동일한 값**으로 맞춰야 합니다.)

### 4.3 가맹점에서 수정할 코드 예시 (의사 코드)

```text
// null/undefined → 빈 문자열로 통일 (17개 필드 모두 순서대로 포함)
parseVal = (v) => (v == null || v === undefined) ? "" : String(v)

ConcatData =
  parseVal(OrderNo) +        // 1
  parseVal(CustomerId) +     // 2
  parseVal(Amount) +         // 3  (숫자면 String(Amount))
  parseVal(PhoneNumber) +    // 4
  parseVal(Description) +    // 5
  parseVal(ChannelCode) +    // 6
  parseVal(Currency) +       // 7
  parseVal(LangCode) +       // 8
  parseVal(RouteNo) +        // 9  (숫자면 String(RouteNo))
  parseVal(IPAddress) +      // 10
  parseVal(TokenType) +      // 11
  parseVal(CreditToken) +    // 12
  parseVal(DirectCreditToken) + // 13
  parseVal(CreditMonth) +    // 14  ← null이어도 "" 반드시 포함
  parseVal(ShopID) +         // 15  ← null이어도 "" 반드시 포함
  parseVal(CustEmail) +      // 16
  parseVal(SaveCard)         // 17

FinalString = ConcatData + MD5Key   // 공백/줄바꿈 없음
CheckSum = MD5(FinalString).toLowerCase()  // hex 32자
```

- **JSON Body**에서도 `CreditMonth`, `ShopID`를 보낼 때 `null` 대신 `""`로 보내는 것이 안전할 수 있습니다.  
  (API 스펙에서 null을 허용하면 그대로 두고, ConcatData만 위 규칙으로 맞추면 됩니다.)

### 4.4 정리

- **3001 System Error**는 대부분 **CheckSum 불일치**로 발생합니다.
- 위와 같이 **17개 필드 순서 유지**, **null → "" 처리**, **OrderNo 형식 통일**을 적용한 뒤 다시 호출해 보시고,  
  동일하면 ChillPay에 **v1/payment 정식 ConcatData/CheckSum 규격**을 한 번 더 확인 요청하는 것이 좋습니다.
