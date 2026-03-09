# ChillPay API Transaction Services 문서 분석 및 취소/무효 자동화 방안

**참조 문서**: ChillPay-API-Transaction-Services-Document-EN_v1.0.3  
**목적**: ChillPay가 무효 시 노티를 보내지 않으므로, API로 무효 건을 조회·요청하고 미들웨어에서 전산/피지 노티에 반영하는 방안 정리.

---

## 1. 취소 vs 무효 vs 환불 (정의 및 시간 제한)

**정의 (ChillPay 기준)**

| 구분 | 설명 | 우리가 할 수 있는 것 | ChillPay 노티 |
|------|------|---------------------|----------------|
| **취소 (Cancel)** | 결제 시 문제 발생 또는 고객이 결제를 취소한 경우. PG가 노티로 알려주는 상태. | **없음.** ChillPay에서 "결제 취소"를 요청·처리하는 API·기능은 없음. **100% 노티로 수신한 내용만** 취소 내역에 반영. | ✅ 취소 시 노티 발송 → 우리가 수신 후 가맹점/전산에 전달만 함 |
| **무효 (Void)** | 결제 완료 후 건을 취소(미정산). | **있음.** Request Void API로 ChillPay에 무효 요청 가능. (자동화 또는 수동) | ❌ 무효 시 ChillPay가 노티 미발송 → 우리가 API 조회·요청 후 가맹점/전산에 노티 전송 |
| **환불 (Refund)** | 이미 정산된 건 취소. | **있음.** Request Refund API로 ChillPay에 환불 요청 가능. (수동, 비용 발생) | ❌ 환불 시 ChillPay가 노티 미발송 → 우리가 API 조회·요청 후 가맹점/전산에 노티 전송 |

- **취소**: 인위적으로 만들 수 있는 거래가 아님. 노티로 들어온 내용만 표시·재전송.
- **무효·환불**: 우리가 ChillPay API로 요청할 수 있는 기능만 해당.

**노티 발송 범위 (운영 확인)**: ChillPay는 **성공(Success) 건만** callback/result 노티를 보내는 것으로 보이며, **실패(Fail)**·**REQUEST(결제 대기)** 등은 노티를 보내지 않는 것으로 알려져 있음. 정확한 스펙은 ChillPay 측 확인 필요. 우리 미들웨어는 수신하는 모든 노티를 로그·릴레이함.

### 1.1 무효/환불 가능 시간 – 환경 설정 (한국·일본 시간 기준)

**기준 시간대**: 한국(KST) 및 일본(JST) 동일 적용. **환경 설정에서 무효/환불 가능 시간을 세팅**하며, **기본값**은 아래와 같다. (취소는 노티 수신만 있으므로 시간 설정 대상이 아님.)

| 구간 | 시간 (당일 기준) | 처리 | 자동화 |
|------|------------------|------|--------|
| **자동화 취소 가능** | **당일 22:30까지** (오후 10시 30분) | Void (무효) | ✅ **자동화 가능** (Request Void API → 무효 노티 전송) |
| **수동 취소만** | **22:31 ~ 다음날 01:30** (오후 10시 31분 ~ 새벽 1시 30분) | Void (무효) | ❌ **자동화 불가** → **수동** 처리만 |
| **환불 구간** | **01:30 이후** (새벽 1시 30분 이후) | Refund (환불) | ❌ **자동화 아님** → **메뉴얼** 처리 (비용 발생) |

- **당일 22:30까지**: 미들웨어에서 “무효 요청” 버튼 등으로 **Request Void API** 호출 후 전산/피지 무효 노티 자동 전송 가능.
- **22:31 ~ 01:30**: 같은 Void이지만 **자동화는 하지 않고**, 운영자가 수동으로 ChillPay/미들웨어에서 처리.
- **01:30 이후**: 정산된 건으로 간주하여 **환불(Refund)** 로만 처리하며, **메뉴얼** (ChillPay 또는 API 수동 호출). 리펀드 비용은 발생.

**요구사항**: ChillPay에서 수동 무효/환불 처리한 뒤에도, **미들웨어에서 조회·노티 전송**으로 전산·피지에 반영할 수 있게 함.  
Transaction Services API는 **노티가 아닌 API 호출**로 조회·요청을 제공함.

---

## 2. Transaction Services API 요약

### 2.1 공통 사항

- **인증**: 요청 헤더에 `Content-Type: application/json`, `CHILLPAY-MerchantCode`, `CHILLPAY-ApiKey` 필수.
- **Checksum**: 각 API별로 정의된 파라미터를 **순서대로 이어 붙인 문자열 + MD5 Secret Key**를 MD5 해시한 32자 hex (소문자). ChillPay에서 발급한 **MD5 Secret Key** 필요.
- **환경**: SANDBOX / PROD URL 구분.

### 2.2 서비스 목록

| No | 서비스명 | URL (PROD) | Method | 역할 |
|----|----------|------------|--------|------|
| 1 | **Search Payment Transaction** | `/api/v1/payment/search` | POST | 결제 트랜잭션 검색 (Status 필터 가능) |
| 2 | Search Settlement Transaction | `/api/v1/settlement/search` | POST | 정산 트랜잭션 검색 |
| 3 | **Search Void Transaction** | `/api/v1/void/search` | POST | **무효 처리된 트랜잭션 검색** |
| 4 | Search Refund Transaction | `/api/v1/refund/search` | POST | 환불 트랜잭션 검색 |
| 5 | **Get Payment Transaction Details** | `/api/v1/payment/details` | POST | **단건 결제 상세 조회 (TransactionId)** |
| 6 | **Request Void Transaction** | `/api/v1/void/request` | POST | **무효 요청 (미정산 건)** |
| 7 | Request Refund Transaction | `/api/v1/refund/request` | POST | 환불 요청 (이미 정산된 건) |

---

## 3. 취소/무효에 직접 연관된 API 상세

### 3.1 Search Void Transaction (무효 건 조회)

- **역할**: ChillPay에서 **이미 무효 처리된 건** 목록을 API로 조회.
- **Request Body (일부)**  
  - OrderBy, OrderDir, PageSize, PageNumber, SearchKeyword  
  - MerchantCode, **OrderNo**, **Status**  
  - **TransactionDateFrom**, **TransactionDateTo**  
  - **Checksum** (No.1~10 순서로 concat + MD5 Secret Key → MD5)
- **Checksum concat 순서**:  
  `OrderBy + OrderDir + PageSize + PageNumber + SearchKeyword + MerchantCode + OrderNo + Status + TransactionDateFrom + TransactionDateTo + MD5 Secret Key`
- **Response**: `data[]` 에 transactionId, orderNo, status, amount, paymentDate 등 포함.
- **활용**: ChillPay에서 수동으로 무효 처리한 건을 **주기적으로 조회**해 우리 로그와 매칭 후, 전산/가맹점에 무효 노티를 보낼 수 있음.

### 3.2 Get Payment Transaction Details (단건 상세)

- **역할**: ChillPay **TransactionId** 한 건에 대한 상세 조회. **status** 로 Success / Voided / Refunded 등 확인 가능.
- **Request Body**: TransactionId (필수), Checksum (`TransactionId + MD5 Secret Key` → MD5).
- **활용**: 특정 건이 무효/환불 되었는지 확인할 때 사용.

### 3.3 Request Void Transaction (무효 요청)

- **역할**: **아직 정산되지 않은(settled 전) 결제**에 대해 우리 쪽에서 **무효 요청**을 API로 제출.
- **Request Body**: TransactionId (필수), Checksum (`TransactionId + MD5 Secret Key` → MD5).
- **Response**: data.transactionId, status 등.
- **제약**: "transaction hasn't settled" 인 경우만. 이미 정산된 건은 **Request Refund Transaction** 사용.
- **활용**: 미들웨어 관리자 화면에서 “이 건 무효 요청” 버튼으로 ChillPay에 무효 요청 후, 성공 시 우리가 전산/가맹점에 무효 노티 전송.

---

## 4. Appendix A – Payment Transaction Status (참고)

| No | Status | 설명 |
|----|--------|------|
| 1 | Success | 성공 |
| 2 | Fail | 실패/미완료 |
| 3 | Cancel | 고객 취소 |
| 4 | Error | 오류 |
| 5 | Request | 결제 대기 |
| 6 | **Void Requested** | 무효 요청 완료 |
| 7 | **Voided** | 무효 처리 완료 |
| 8 | Refund Requested | 환불 요청 완료 |
| 9 | Refunded | 환불 완료 |

취소/무효 자동화 시 **Voided**, **Void Requested** 상태를 기준으로 “무효 처리됨”으로 간주하면 됨.

---

## 5. 가장 이상적인 구현 의견 (권장안)

시간 제한(다음날 11시 KST)과 API 특성을 반영한 **단일 권장 구조**는 아래와 같습니다.

### 5.0 요약

| 구간 | 방식 | 미들웨어 역할 |
|------|------|----------------|
| **결제 다음날 11:00 KST 이전** | API로 무효 요청 가능 | “무효 요청” 버튼 → **Request Void API** 호출 → 성공 시 **무효 노티** 전송 (가맹점 + 전산) |
| **다음날 11:00 KST 이후** | ChillPay 수동 무효만 가능, 노티 없음 | **Search Void API**로 무효 건 조회 → 우리 로그와 매칭 → **미전송 건만** 무효 노티 전송 |

### 5.1 이상적인 두 가지 경로

1. **우리 쪽에서 무효 요청 (정해진 시간 이내)**  
   - 피지/전산 로그에서 “성공” 건 중 **결제일 기준 다음날 11:00 KST 이전**인 건만 **“무효 요청”** 버튼 노출.  
   - 클릭 시 **Request Void Transaction** 호출 → ChillPay 성공 응답 시, 해당 건에 대해 **가맹점 callback/result + 전산**으로 **무효 노티 1회 전송**.  
   - 이렇게 하면 전화 없이 **완전 자동**으로 전산·피지 반영 가능.

2. **ChillPay에서 이미 무효 처리한 건 동기화 (시간 지난 경우 또는 수동 무효)**  
   - **Search Void Transaction**을 **주기(예: 10분)** 또는 **“무효 건 조회” 버튼**으로 실행.  
   - 조회한 무효 건의 `transactionId` / `orderNo`로 우리 **pg-noti 로그**의 성공 건과 매칭.  
   - “이미 무효 노티 보냄” 플래그가 없는 건만 골라 **가맹점 + 전산**에 무효 노티 전송 후, 중복 방지 플래그 저장.  
   - 다음날 11시가 지나 ChillPay에서 수동 무효한 건도 이 경로로 자동 반영됨.

### 5.2 정리

- **취소**: 기존대로 ChillPay 노티 수신 → 릴레이/전산 전송만 유지. 별도 구현 불필요.  
- **무효(이상적)**:  
  - **11시 이전** → Request Void API + 무효 노티 자동 전송.  
  - **11시 이후/수동 무효** → Search Void API로 조회 후, 미전송 건만 무효 노티 전송.  
- **환경 분리**: 아래 8장처럼 **Sandbox / Production** 각각 **mid, ApiKey, MD5** 설정을 두고, 개발 시 Sandbox 키로 Transaction API 호출하면 됨.

---

## 6. 취소/무효 자동화 방안 (상세)

### 6.1 방안 A: ChillPay에서 이미 무효 처리한 건을 우리가 “알아서” 반영

**흐름**  
1. **주기적 조회**: Search Void Transaction API를 일정 주기(예: 5분, 10분)로 호출.  
   - TransactionDateFrom / TransactionDateTo 로 “최근 N일” 범위 지정 가능.
2. **우리 로그와 매칭**: 응답의 `data[].transactionId` 또는 `data[].orderNo` 로 `pg-noti.log` / 전산 로그에서 **해당 결제 성공 건** 검색.
3. **무효 노티 전송**: 매칭된 건에 대해  
   - 가맹점 callback/result URL로 **무효 상태 노티** 1회 전송 (피지 노티 반영),  
   - 전산 노티 URL로 **무효 상태 전산용 payload** 1회 전송 (전산 노티 반영).

**필요 사항**  
- ChillPay **MerchantCode**, **ApiKey**, **MD5 Secret Key** (Transaction Services용).  
- 무효 노티를 “ChillPay 실시간 노티와 동일한 형식”으로 보내려면, **실시간 노티용 CheckSum 규격**과 그때 쓰는 **MD5 Secret Key**가 추가로 필요 (현재 미들웨어는 수신·릴레이만 하므로, “무효용 가짜 노티” 전송 시 CheckSum 재계산 규격이 별도 문서에 있을 수 있음).

### 6.2 방안 B: 우리 미들웨어에서 “무효 요청” 후 노티 반영

**흐름**  
1. 관리자가 **피지 노티 로그 / 전산 노티 로그**에서 “성공” 건을 선택.  
2. “ChillPay 무효 요청” 버튼 클릭 시, 해당 건의 **TransactionId**로 **Request Void Transaction** API 호출.  
3. ChillPay가 무효 처리 후에도 노티를 보내지 않으므로, **성공 시** 우리가:  
   - 해당 건과 동일한 가맹점/전산 대상에 대해 **무효 노티 1회 전송** (피지·전산 양쪽 반영).

**제약**  
- Request Void는 **아직 정산되지 않은 건**만 가능. 이미 정산된 건은 ChillPay에서 수동 무효한 경우와 마찬가지로 **방안 A(Search Void)** 로 조회해 반영해야 함.

### 6.3 방안 C: 혼합 운영

- **정산 전**: 관리자가 “무효 요청” 버튼 → Request Void API 호출 → 성공 시 우리가 무효 노티 전송 (방안 B).  
- **정산 후** 또는 ChillPay에서 먼저 무효 처리한 경우: Search Void(또는 Get Payment Details)로 무효 건 조회 → 우리 로그와 매칭 후 무효 노티 전송 (방안 A).

---

## 7. 미들웨어 구현 시 권장 사항

1. **설정**  
   - ChillPay Transaction Services용 **MerchantCode, ApiKey, MD5 Secret Key** 를 환경변수 또는 설정 파일에 추가.  
   - (선택) 실시간 노티와 다른 키일 수 있으므로, “Transaction API용 키” / “노티 CheckSum용 키” 구분 저장.

2. **ChillPay API 클라이언트**  
   - Search Void, Get Payment Details, Request Void 세 가지를 **공통 Checksum 생성 + axios POST** 로 래핑.  
   - Checksum 규칙은 메뉴얼 Table별 concat 순서를 정확히 준수.

3. **무효 건 조회(방안 A)**  
   - 스케줄(또는 수동 실행)으로 Search Void 호출.  
   - 응답 `data[]` 의 transactionId/orderNo로 `pg-noti.log` 등에서 기존 “성공” 건 조회.  
   - 아직 “무효 노티를 보냈다”는 플래그가 없는 건만 골라, 가맹점 URL + 전산 URL에 무효 노티 1회 전송.  
   - “이 건은 이미 무효 노티 보냄” 표시를 로그 또는 별도 저장소에 남겨 중복 전송 방지.

4. **무효 요청(방안 B)**  
   - 피지/전산 로그 화면에서 건별 “무효 요청” 버튼 노출.  
   - 클릭 시 Request Void API 호출 → 성공 시 해당 건과 동일한 merchantId/targetUrl에 대해 **무효 노티 1회 전송** (relay + 전산 저장 로직 재사용).  
   - 실시간 노티 형식을 그대로 쓰려면 PaymentStatus=2(또는 ChillPay 무효 코드)와 **노티용 CheckSum 재계산** 필요 → ChillPay 실시간 노티 스펙 확인 필요.

5. **무효 노티 형식**  
   - 가맹점/전산이 “무효”를 구분하는 필드(PaymentStatus, status 등)를 동일하게 맞추고,  
   - ChillPay 실시간 노티와 동일 형식이면 **CheckSum**을 해당 스펙대로 재계산해 전송하는 것이 안전함.

---

## 8. 환경 설정

### 8.1 Sandbox / Production 키

Transaction API(무효·환불 조회/요청) 호출 시 **Sandbox**와 **Production**을 구분해 사용하려면, 환경 설정에 아래 항목을 **입력 창**으로 둡니다.

| 환경 | 입력 항목 | 용도 |
|------|-----------|------|
| **Sandbox** | mid (MerchantCode), ApiKey, MD5 | 개발/테스트 시 `sandbox-api-transaction.chillpay.co` 호출 |
| **Production** | mid (MerchantCode), ApiKey, MD5 | 운영 시 `api-transaction.chillpay.co` 호출 |

- **저장 위치**: 기존 설정 구조에 맞춰 `config/chillpay-transaction-api.json` 또는 `config/site-settings.json` 내 블록으로 저장. (환경변수만 쓸 수도 있으나, 관리자 화면에서 수정 가능하게 하려면 설정 파일 + 관리자 UI 권장.)
- **사용**: 무효/환불 조회·요청 시, 현재 앱 환경 또는 관리자 선택에 따라 해당 mid/ApiKey/MD5와 Base URL을 사용.

### 8.2 취소 가능 시간 (자동화 Void 마감 시각)

- **설정 항목**: “취소(무효) 자동화 가능 마감 시각”을 **환경 설정에서 세팅**.
- **기본값**: **한국·일본 시간 기준 당일 22:30** (오후 10시 30분).  
  - 이 시각까지의 건만 “무효 요청” 버튼 등 **자동화 취소(Void)** 대상으로 노출.
- **22:31 ~ 01:30** 구간은 자동화 비대상(수동만), **01:30 이후**는 환불(Refund) 구간으로 메뉴얼 처리.
- 구현 시: 결제 시각(또는 결제일) + 위 마감 시각을 KST/JST로 비교해, 자동화 Void 가능 여부를 판단.

### 8.3 리펀드(환불) 자동화 가능 여부

**질문**: 리펀드 비용을 내더라도 **자동화 취소(환불)** 기능이 있는가?

**답**: **있습니다.** ChillPay Transaction Services 문서에 **Request Refund Transaction** API가 정의되어 있습니다.

- **역할**: 이미 정산된(settled) 건에 대해 **환불 요청**을 API로 제출.
- **자동화 의미**: 관리자가 “환불 요청” 버튼 클릭 → 미들웨어가 **Request Refund API** 호출 → ChillPay가 환불 처리 → (ChillPay는 환불 노티를 안 보낼 수 있으므로) 성공 시 **우리가 전산/피지에 환불 노티 전송**까지 한 번에 처리하면, “자동화 환불”로 쓸 수 있습니다.
- **비용**: 환불 수수료는 ChillPay/카사 정책에 따르며, 자동화 여부와는 별개입니다.
- **정리**: 01:30 이후 구간을 “메뉴얼만”으로 둘지, “버튼 한 번으로 API 호출 + 노티 전송”까지 자동화할지는 **운영 정책** 선택입니다. **기술적으로는 자동화(API 호출 + 노티 전송) 구현 가능**합니다.

---

## 9. 정리

| 항목 | 내용 |
|------|------|
| **메뉴얼 성격** | ChillPay에 **API로 접속**해 결제/정산/무효/환불을 **조회·요청**하는 서비스 정의. 노티 발송이 아님. |
| **취소 가능 시간** | 환경 설정으로 세팅. 기본: **한국·일본 시간 당일 22:30**까지 자동화 Void, **22:31~01:30** 수동만, **01:30 이후** 환불(메뉴얼). |
| **무효 “알기”** | **Search Void Transaction**으로 ChillPay에서 이미 무효 처리한 건 목록 조회 가능. |
| **무효 “시키기”** | **Request Void Transaction**으로(설정 시각 이내) 우리가 무효 요청 가능. |
| **이상적 구현** | 22:30 이전 → Request Void + 무효 노티. 22:31~01:30 수동. 01:30 이후 → Refund(메뉴얼 또는 API 자동화 선택). |
| **리펀드 자동화** | **Request Refund Transaction** API 있음 → 비용은 발생하지만 **자동화(버튼 → API → 노티)** 구현 가능. |
| **환경 설정** | Sandbox/Production **mid, ApiKey, MD5** + **취소 가능 시간(기본 22:30)** 입력. |
| **추가 확인** | “무효 노티”를 ChillPay와 동일한 형식으로 보낼 때 사용하는 **실시간 노티 CheckSum 규칙** 및 **MD5 Key** 확인 필요. |

이 문서와 메뉴얼을 기준으로, 미들웨어에 “무효 조회/무효 요청 + 무효 노티 전송” 기능과 Sandbox/Production 키 설정을 추가하면 ChillPay 수동 무효 후에도 전산·피지 노티를 자동으로 맞출 수 있습니다.

---

## 10. 구현 현황 (미들웨어)

| 기능 | 구현 | 비고 |
|------|------|------|
| Request Void + 무효 노티 전송 | ✅ | 피지 노티/취소환불 메뉴에서 "무효 요청" 버튼 |
| Request Refund + 환불 노티 전송 | ✅ | 동일 |
| **Search Void + 미전송 건만 무효 노티** | ✅ | **취소환불 > 무효거래** 페이지의 **"ChillPay 무효 건 동기화"** 버튼. 최근 7일 무효 건 조회 후 우리 pg-noti 로그와 매칭, 아직 무효 노티를 보내지 않은 건만 가맹점/전산에 전송. 중복 방지 이력은 `data/chillpay-void-noti-sent.json` 에 저장. |
| **Search Refund + 미전송 건만 환불 노티** | ✅ | **취소환불 > 환불거래** 페이지의 **"ChillPay 환불 건 동기화"** 버튼. 최근 7일 환불 건 조회 후 우리 pg-noti 로그와 매칭, 아직 환불 노티를 보내지 않은 건만 가맹점/전산에 전송. 중복 방지 이력은 `data/chillpay-refund-noti-sent.json` 에 저장. |
| Get Payment Transaction Details | ❌ | 단건 상태 확인 필요 시 추가 가능 |
