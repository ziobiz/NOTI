# NOTI 추가 개발 요청 — ElementPay 브라우저 Result 입구

**요청 측:** ICOPAY  
**대상:** NOTI 미들웨어 (`noti.icopay.net`)  
**일자:** 2026-08-13  
**목적:** 가맹점은 Callback/Result URL·연동 문서를 **바꾸지 않고**, 본사가 결제대행사만 ElementPay로 전환해도 동일하게 동작하게 한다.

---

## 1. 배경 (왜 필요한가)

ElementPay(EP) Cabinet에는 **Webhook(서버 통보) URL을 본사 고정 1개만** 등록할 수 있다.  
가맹점마다 다른 Result(브라우저 복귀) URL을 EP에 넣을 수 없다(1:N).

| 채널 | EP → | NOTI 역할 | 가맹 |
|------|------|-----------|------|
| 서버 Webhook | `POST /noti/elementpay` (**이미 구현**) | ICOPAY 전달 + 가맹 **callbackUrl** 릴레이 | 서버 Callback (기존 계약 유지) |
| 브라우저 Result | **입구 없음 (본 요청)** | 가맹 **resultUrl**로 브라우저 전달 | Result 페이지 (기존 계약 유지) |

가맹이 수정 없이 쓰려면 **Result도 NOTI가 받아 가맹 resultUrl로 넘기는 입구**가 필요하다.

---

## 2. 목표 흐름

```
[본사 1회] EP Cabinet Webhooks
  = https://noti.icopay.net/noti/elementpay

[가맹마다 — NOTI 레코드만, EP에 가맹 도메인 등록 금지]
  callbackUrl = 가맹 서버 Callback
  resultUrl   = 가맹 브라우저 Result

[결제 시작 — ICOPAY]
  _successUrl / _rejectUrl / _waitingUrl
    = https://noti.icopay.net/noti/result/elementpay
      (+ 필요 시 order 등 식별 파라미터)

[결제 후]
  서버:     EP → /noti/elementpay → ICOPAY + 가맹 callbackUrl
  브라우저: EP → /noti/result/elementpay → 가맹 resultUrl
```

가맹은 계속 **자기 callback/result URL만** 유지한다. PG 전환은 본사·NOTI·ICOPAY만 변경.

---

## 3. 개발 범위 (NOTI)

### 3.1 신규 엔드포인트 (필수)

| 항목 | 내용 |
|------|------|
| URL | `https://noti.icopay.net/noti/result/elementpay` |
| Method | `GET` 및 `POST` 모두 (EP 리다이렉트 방식 차이 대비) |
| 인증 | 공개 입구(브라우저). 가맹 비밀 URL 아님. 남용 완화는 order 유효성·레이트리밋 등 선택 |

**처리 순서**

1. 쿼리/바디에서 주문 식별값 추출 (`order` / `orderNo` / EP가 붙이는 동등 필드).
2. `pgKind=elementpay` 가맹 중 해당 주문에 매칭되는 가맹 조회.  
   - 우선: ICOPAY에 order 조회해 Comp-Id 확보 후 NOTI 가맹 매칭 (권장)  
   - 또는: NOTI/ICOPAY가 합의한 매핑(세션·대기주문)  
   - Webhook과 동일하게 Comp-Id ↔ NOTI `merchantId` 매칭 가능하면 재사용.
3. 가맹 `resultUrl`이 있으면, 가맹의 **`resultDeliveryMode`** 규칙으로 브라우저 전달  
   (`auto` / `autot` / `POST` / `POST_302` 등 — **기존 NOTI Result 전달 로직 재사용**).
4. 페이로드: 가맹이 이미 파싱하는 **동일 통보 필드 스키마**  
   (`mapElementPayToMerchantNotifyBody` 또는 동일 계약). EP 원문만 던지지 말 것.
5. 가맹을 못 찾거나 `resultUrl` 비어 있으면: 안내 페이지 또는 합의된 ICOPAY 결제완료 URL로 폴백. **EP에 가맹 도메인 노출 금지.**

### 3.2 Provision / 관리 UI (권장)

- `pgKind=elementpay` provision·조회 응답에 고정 Result 입구 명시:

```json
{
  "pgKind": "elementpay",
  "elementpayWebhookUrl": "https://noti.icopay.net/noti/elementpay",
  "elementpayResultUrl": "https://noti.icopay.net/noti/result/elementpay"
}
```

- ElementPay 가맹 화면에 위 두 URL 표시.
- **슬롯형 `/noti/result/j{N}` 발급은 EP에 하지 않음.** 입구는 본사 고정 1개.

### 3.3 로그

- kind=`result`, routeKey=`elementpay/result` (또는 동등).
- merchantId, order, targetUrl(resultUrl), relayStatus 기록.

### 3.4 범위 밖 (이번 요청에서 하지 않음)

- EP Cabinet에 가맹 Result URL 등록.
- Webhook(`/noti/elementpay`) 재구현 (이미 있음 — Cabinet에는 **이것만** 등록).
- 가맹 API/문서 변경 (가맹은 기존 Callback/Result 유지).

---

## 4. ICOPAY 측 (NOTI 완료 후 연동 — 참고)

NOTI Result 입구가 준비되면 ICOPAY에서:

1. ElementPay `initPayment` 의 `_successUrl` / `_rejectUrl` / `_waitingUrl` 에  
   `elementpayResultUrl` 사용 (가맹 쇼핑몰 URL 금지).
2. 운영관리 노티생성·수신통보 안내에 해당 NOTI Result URL 반영(선택).
3. EP Cabinet Webhooks는 계속 `…/noti/elementpay` 만.

---

## 5. 인수 테스트

| # | 시나리오 | 기대 |
|---|----------|------|
| 1 | EP Webhook → `/noti/elementpay` | ICOPAY 적재 + 가맹 **callbackUrl** 수신 (기존) |
| 2 | 결제 완료 후 브라우저가 `/noti/result/elementpay` 도착 | 가맹 **resultUrl**로 이동/POST, 기존 필드 스키마 |
| 3 | resultDeliveryMode=`json`이 아닌 브라우저 모드 | 기존 Result 모드와 동일 동작 |
| 4 | 알 수 없는 order | 폴백만, 타 가맹으로 잘못 전달 없음 |
| 5 | 가맹점 코드/URL 변경 없음 | 본사 EP 전환만으로 Callback+Result 유지 |

---

## 6. 체크리스트 (NOTI 개발자용)

- [x] `GET|POST /noti/result/elementpay` 구현
- [x] order → EP 가맹 매칭 (Comp-Id / ICOPAY 협조 포함)
- [x] `resultUrl` + `resultDeliveryMode` 로 브라우저 전달
- [x] 가맹 통보 스키마 유지 (원문 EP form만 전달 금지)
- [x] provision 응답·관리 UI에 `elementpayResultUrl` 노출
- [x] 로그 kind=result
- [ ] 위 인수 테스트 통과
- [ ] ICOPAY에 Result URL·매칭 방식 확정 통보

---

## 7. 한 줄 요약

**EP Webhook은 `/noti/elementpay`(기존). 가맹 무수정 Result를 위해 `/noti/result/elementpay` 입구를 NOTI에 추가하고, 가맹 `resultUrl`로 릴레이하면 된다.**
