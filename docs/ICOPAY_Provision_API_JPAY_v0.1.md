# ICOPAY ↔ NOTI Provision API 스펙 (JPAY 전용 1차)

| 항목 | 내용 |
|------|------|
| **문서 버전** | 0.1 (초안) |
| **작성 목적** | ICOPAY 운영관리 「노티생성」에서 NOTI 가맹(JPAY)을 API로 자동 등록 |
| **NOTI 저장소** | [ziobiz/NOTI](https://github.com/ziobiz/NOTI) |
| **NOTI 관리자** | https://noti.icopay.net/admin/login (신규등록 → JPAY 등록과 동일 결과) |
| **ICOPAY 연동 필드** | 가맹 업체관리 → **JPAY 수신통보 URL** (`pay_notifyurl`, `pay_callbackurl`) |

---

## 1. 배경

현재 가맹 등록 시 운영자가 **두 번** 작업합니다.

1. **NOTI** 관리자에서 JPAY 가맹 신규등록 → PG Callback/Result URL 발급
2. **ICOPAY** 업체관리에서 발급 URL을 **JPAY 수신통보 URL**에 수동 입력

| ICOPAY 화면 라벨 | JPAY 전문 필드 | NOTI에서 발급하는 URL (예) |
|------------------|----------------|---------------------------|
| Notify / Callback URL (NOTI MW) | `pay_notifyurl` | `https://noti.icopay.net/noti/callback/j{N}` |
| Callback / Result URL (NOTI MW) | `pay_callbackurl` | `https://noti.icopay.net/noti/result/j{N}` |

> **용어 정리:** PG(JPAY)→NOTI **Callback** 경로 = ICOPAY **Notify**. PG→NOTI **Result** 경로 = ICOPAY **Callback(브라우저 복귀)**. 본 문서 응답 JSON에는 ICOPAY 필드명(`icopayJpayNotifyUrl`, `icopayJpayCallbackUrl`)을 함께 반환합니다.

**1차 범위:** JPAY만. ChillPay(CHILLPAY) provision은 2차.

**개발 순서:** NOTI에서 Provision API 구현 → ICOPAY에서 호출·운영 화면 연동.

---

## 2. NOTI 구현 요약

- 기존 `POST /admin/merchants` (JPAY 분기)와 **동일한 저장 로직**을 함수로 추출
- `config/merchants.json` (또는 `config-test/merchants.json`)에 가맹 레코드 생성
- `findNextAvailableJpayMerchantSlot()` / `buildJpayRouteCallbackKey` / `buildJpayRouteResultKey` 재사용
- 관리자 UI `/admin/merchants?kind=jpay` 목록에 API로 생성한 가맹도 동일 표시
- `appendConfigChangeLog` 감사 로그에 `source: icopay-provision` 기록

---

## 3. 베이스 URL·환경

| 환경 | 베이스 URL | 설정 폴더 |
|------|------------|-----------|
| 운영 | `https://noti.icopay.net` | `config/` |
| 테스트 | `https://test.noti.icopay.net` (또는 운영 정책에 따름) | `config-test/` (`APP_ENV=test`) |

---

## 4. 인증

| 항목 | 값 |
|------|-----|
| 방식 | `Authorization: Bearer {provision_api_key}` |
| 키 저장 | NOTI `config/noti-provision.json` 또는 환경변수 `NOTI_PROVISION_API_KEY` |
| (권장) IP 허용 | ICOPAY 서버 egress IP 화이트리스트 |
| TLS | 필수 (HTTPS) |

| HTTP | errorCode | 설명 |
|------|-----------|------|
| 401 | `UNAUTHORIZED` | API 키 없음·불일치 |
| 403 | `FORBIDDEN` | IP·환경 거부 |

---

## 5. 엔드포인트

### 5.1 JPAY 가맹 생성 (Provision) — **1차 필수**

```
POST /api/v1/icopay/merchants/provision
Content-Type: application/json
Authorization: Bearer {provision_api_key}
X-Icopay-Request-Id: {uuid}          (선택, 멱등·감사용)
```

#### 요청 본문

```json
{
  "merchantId": "M000123",
  "pgKind": "jpay",
  "internalTargetId": "ONTL_HQ_JPY",
  "callbackUrl": "https://merchant.example.com/webhook",
  "resultUrl": "https://merchant.example.com/pay-result",
  "routeNo": "",
  "jpaySlotNo": null,
  "options": {
    "enableRelay": true,
    "enableInternal": true,
    "enableDevInternal": false,
    "relayFormat": "raw",
    "resultDeliveryMode": "auto"
  },
  "icopayMeta": {
    "compId": "M000123",
    "orgUnitId": 12345,
    "provisionedBy": "icopay-ops"
  }
}
```

#### 요청 필드

| 필드 | 필수 | 설명 |
|------|------|------|
| `merchantId` | **Y** | NOTI `merchants.json` 키. ICOPAY `compId` 사용 권장 |
| `pgKind` | **Y** | 1차: `"jpay"` 만 허용 |
| `internalTargetId` | **Y** | NOTI 전산 대상 ID (`internal-targets` 설정에 존재해야 함) |
| `callbackUrl` | 조건부 | `enableRelay=true` 일 때 가맹 서버 웹훅(원문 릴레이 대상) |
| `resultUrl` | 조건부 | 브라우저 복귀(RESULT) 대상 |
| `routeNo` | N | 비우면 슬롯에서 `j{N}` 자동 (`jpayRouteTokenForSlot`) |
| `jpaySlotNo` | N | `null`이면 `findNextAvailableJpayMerchantSlot()` 자동 할당 |
| `options` | N | 미지정 시 NOTI 관리자 JPAY 신규등록 기본값과 동일 |
| `options.enableRelay` | N | 기본 `true` (관리자 UI 기본과 동일 권장) |
| `options.enableInternal` | N | 기본 `true` — ICOPAY 전산 노티 릴레이 |
| `options.enableDevInternal` | N | 기본 `false` |
| `options.relayFormat` | N | `raw` \| `json` \| `form` |
| `options.resultDeliveryMode` | N | `auto` \| `autot` \| `no_browser_redirect` \| `post_force_redirect` |
| `icopayMeta` | N | 감사·추적용(저장만, 라우팅 로직에 미사용) |

#### NOTI 내부 저장 매핑 (기존 admin POST 와 동일)

| NOTI 필드 | 값 |
|-----------|-----|
| `merchantPgKind` | `jpay` |
| `jpayRouteCallbackKey` | `jpay/callback/j{N}` |
| `jpayRouteResultKey` | `jpay/result/j{N}` |
| `routeNo` | `j{N}` (또는 요청 `routeNo`) |
| `callbackUrl` / `resultUrl` | 요청 값 |
| `internalTargetId` | 요청 값 |

#### 성공 응답 `201 Created`

```json
{
  "success": true,
  "data": {
    "merchantId": "M000123",
    "pgKind": "jpay",
    "slot": 20,
    "routeNo": "j20",
    "jpayRouteCallbackKey": "jpay/callback/j20",
    "jpayRouteResultKey": "jpay/result/j20",
    "pgCallbackUrl": "https://noti.icopay.net/noti/callback/j20",
    "pgResultUrl": "https://noti.icopay.net/noti/result/j20",
    "icopayJpayNotifyUrl": "https://noti.icopay.net/noti/callback/j20",
    "icopayJpayCallbackUrl": "https://noti.icopay.net/noti/result/j20",
    "internalTargetId": "ONTL_HQ_JPY",
    "callbackUrl": "https://merchant.example.com/webhook",
    "resultUrl": "https://merchant.example.com/pay-result",
    "created": true,
    "provisionRequestId": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```

#### ICOPAY 저장 매핑 (2차 ICOPAY 개발 시)

| ICOPAY DB (`tb_merchant_notify_url`) | NOTI 응답 필드 |
|--------------------------------------|----------------|
| `url_type=JPAY_NOTIFY` → `noti_url` | `icopayJpayNotifyUrl` |
| `url_type=JPAY_CALLBACK` → `noti_url` | `icopayJpayCallbackUrl` |

#### 멱등 (권장)

| 상황 | HTTP | `data.created` |
|------|------|----------------|
| 신규 생성 | 201 | `true` |
| 동일 `merchantId` + `pgKind=jpay`, 동일 슬롯·URL로 재요청 | 200 | `false` |
| 존재하나 슬롯·URL·설정 불일치 | 409 | — |
| 동일 `X-Icopay-Request-Id` 재전송 | 최초와 동일 응답 | — |

---

### 5.2 JPAY 가맹 조회 — **1차 권장**

```
GET /api/v1/icopay/merchants/{merchantId}?pgKind=jpay
Authorization: Bearer {provision_api_key}
```

- **200** — §5.1 `data` 와 동일 구조
- **404** — `MERCHANT_NOT_FOUND`

---

### 5.3 JPAY 가맹 수정 — **2차 (1차 제외)**

- `callbackUrl`, `resultUrl`, `internalTargetId`, `options` 일부만 변경
- **슬롯·PG URL(`jpayRoute*`)은 변경 불가**

---

### 5.4 JPAY 가맹 삭제 — **2차 (1차 제외)**

- 운영 복구용. PG·ICOPAY에 URL이 이미 등록된 경우 `409` + 수동 확인 안내

---

## 6. 에러 응답 공통 형식

```json
{
  "success": false,
  "errorCode": "JPAY_SLOT_EXHAUSTED",
  "message": "사용 가능한 JPAY 노티 슬롯이 없습니다.",
  "details": {}
}
```

| HTTP | errorCode | 의미 |
|------|-----------|------|
| 400 | `INVALID_REQUEST` | JSON 파싱·필수 필드 누락 |
| 400 | `INVALID_PG_KIND` | 1차에서 `jpay` 외 요청 |
| 400 | `INVALID_INTERNAL_TARGET` | `internalTargetId` 미등록 |
| 400 | `INVALID_MERCHANT_ID` | 빈 ID·허용되지 않는 형식 |
| 400 | `JPAY_SLOT_INVALID` | `jpaySlotNo` 범위·형식 오류 |
| 400 | `JPAY_ROUTE_CONFLICT` | 지정 슬롯이 다른 가맹에 사용 중 |
| 400 | `MERCHANT_URL_REQUIRED` | `enableRelay=true` 인데 callback/result URL 없음 |
| 401 | `UNAUTHORIZED` | API 키 없음·불일치 |
| 403 | `FORBIDDEN` | IP·환경 거부 |
| 404 | `MERCHANT_NOT_FOUND` | 조회 대상 없음 |
| 409 | `MERCHANT_ALREADY_EXISTS` | 다른 설정으로 이미 존재 |
| 409 | `JPAY_SLOT_EXHAUSTED` | 자동 할당 가능 슬롯 없음 |
| 500 | `INTERNAL_ERROR` | 저장·내부 오류 |
| 503 | `NOTI_CONFIG_LOCKED` | 설정 파일 잠금·쓰기 실패 |

---

## 7. 요청 예시 (cURL)

### 7.1 JPAY 가맹 자동 슬롯 생성

```bash
curl -sS -X POST "https://noti.icopay.net/api/v1/icopay/merchants/provision" \
  -H "Authorization: Bearer YOUR_PROVISION_API_KEY" \
  -H "Content-Type: application/json" \
  -H "X-Icopay-Request-Id: 550e8400-e29b-41d4-a716-446655440000" \
  -d '{
    "merchantId": "M000123",
    "pgKind": "jpay",
    "internalTargetId": "ONTL_HQ_JPY",
    "callbackUrl": "https://merchant.example.com/wp-json/icopay/v1/webhook",
    "resultUrl": "https://merchant.example.com/icopay/pay-result",
    "icopayMeta": {
      "compId": "M000123",
      "orgUnitId": 12345,
      "provisionedBy": "icopay-ops"
    }
  }'
```

### 7.2 조회

```bash
curl -sS "https://noti.icopay.net/api/v1/icopay/merchants/M000123?pgKind=jpay" \
  -H "Authorization: Bearer YOUR_PROVISION_API_KEY"
```

---

## 8. NOTI 구현 체크리스트

- [ ] `POST /admin/merchants` JPAY 분기 → `provisionJpayMerchant(body)` 공통 함수 추출
- [ ] `POST /api/v1/icopay/merchants/provision` 라우트 + Bearer 인증
- [ ] `GET /api/v1/icopay/merchants/:merchantId?pgKind=jpay`
- [ ] 슬롯 중복 검사 (`JPAY_ROUTE_CONFLICT`) — 기존 admin 로직과 동일
- [ ] `saveMerchants()` + `appendConfigChangeLog({ source: 'icopay-provision', ... })`
- [ ] `APP_ENV=test` 시 `config-test/` 분리 동작 확인
- [ ] API 키·(선택) IP 화이트리스트 설정 문서화
- [ ] 관리자 UI 목록과 API 생성 가맹 동일 표시 확인

---

## 9. ICOPAY 2차 연동 (NOTI API 배포 후)

NOTI Provision API 운영 반영 후 ICOPAY에서 진행:

1. **본사설정** — NOTI Provision 베이스 URL, API Key
2. **운영관리 → 노티생성** — 가맹 선택 → [JPAY 노티 생성]
3. API 응답 `icopayJpayNotifyUrl` / `icopayJpayCallbackUrl` → `tb_merchant_notify_url` 저장
4. (선택) `tb_merchant_noti_mw_link` — `noti_merchant_id`, `slot`, `provisioned_at` 매핑

관련 ICOPAY 코드 (참고):

- `MerchantNotifyUrl.URL_TYPE_JPAY_NOTIFY` / `URL_TYPE_JPAY_CALLBACK`
- `CompService.saveMerchantPayNotifyUrls(..., jpayNotifyUrl, jpayCallbackUrl)`
- `JpayPaymentService.resolveMerchantJpayNotifyUrl` / `resolveMerchantJpayCallbackUrl`

---

## 10. 1차 / 2차 범위

| 1차 (본 문서) | 2차 |
|---------------|-----|
| JPAY `POST` provision | ChillPay provision |
| 슬롯 자동·지정 할당 | 가맹 수정 API |
| ICOPAY용 URL 필드 반환 | 가맹 삭제 API |
| 멱등 (`merchantId` 기준) | 대량 일괄 생성 |
| `GET` 조회 (권장) | ICOPAY 운영관리 UI |

---

## 11. NOTI 팀 전달 문구 (복사용)

> ICOPAY 운영관리 「노티생성」 연동을 위해, 관리자 **JPAY 신규등록**(`POST /admin/merchants`, `kind=jpay`)과 동일하게 `merchants.json` 가맹·슬롯·PG URL을 생성하는 **`POST /api/v1/icopay/merchants/provision`** (Bearer 인증)을 1차로 개발해 주세요.  
> 응답에 ICOPAY `pay_notifyurl` / `pay_callbackurl` 에 넣을 전체 URL(`icopayJpayNotifyUrl`, `icopayJpayCallbackUrl`)을 포함해 주세요.  
> 상세 스펙: ICOPAY 저장소 `docs/ICOPAY_Provision_API_JPAY_v0.1.md`

---

## 12. 변경 이력

| 버전 | 일자 | 내용 |
|------|------|------|
| 0.1 | 2026-06-29 | 초안 — JPAY provision 1차, NOTI 개발 요청용 |
