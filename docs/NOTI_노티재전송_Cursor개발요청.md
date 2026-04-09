# NOTI(노티미들웨어) — ICOPAY 연동 재전송·HTTP 계약 개발 요청 (Cursor용)

## 배경·목표

- NOTI가 ICOPAY(`pg-app` 공개 엔드포인트, 예: `/api/open/pg-notify/{token}/…`)로 PG 노티를 전달한다.
- ICOPAY가 **수신·파싱·저장(및 합의된 후처리)에 실패**한 경우, NOTI는 이를 **명확히 구분**해 **재전송(백오프)** 할 수 있어야 한다.
- 권장 패턴: **동기 HTTP 계약** — 응답 **HTTP 상태 코드**와(선택) **JSON 본문**으로 성공/실패를 구분한다.

## ICOPAY(pg-app) 응답 계약 (구현됨 — NOTI 파싱 기준)

적용 경로: `/api/open/pg-notify/...` 중 **브라우저용 pay-result 리다이렉트가 아닌** 서버-투-서버 응답(JSON·폼 POST 등).

### 성공 — HTTP 200

- 본문 JSON: `success: true`, `processed: true`, `retryable: false`, `inboundId`(수신 로그 PK)
- 본사설정 `notifyOkResponse`가 JSON 객체면 키 병합, 문자열이면 `result` 필드 등으로 포함

### 파싱·가맹점 분기 실패 — HTTP 422

- `success: false`, `processed: false`, `retryable: true`(기본)
- `errorCode`: `processStatus` 값(예: `URL_PAY_NEEDS_COMP_ID`, `MERCHANT_UNRESOLVED`)
- `message`: `errorMessage`
- `inboundId`: 수신 로그는 저장된 뒤 응답하므로 포함됨

### pg_trnsctn 후처리(dispatch) 예외 — HTTP 503

- `errorCode`: `PG_TRNSCTN_DISPATCH_FAILED`, `retryable: true`

### 응답 JSON 생성 실패 — HTTP 500

- `errorCode`: `RESPONSE_BUILD_ERROR`, `retryable: true`

### 토큰/HMAC/IP 거절

- HTTP **403** (기존과 동일, 본문은 텍스트일 수 있음)

### 네트워크

- HTTP **408 / 429 / 5xx**, 연결·타임아웃 — NOTI 재시도 정책으로 처리

### 멱등

- 동일 노티 건에 대해 ICOPAY가 중복을 안전하게 처리할 **키**(헤더 `Idempotency-Key` 또는 본문 필드) 합의 후, NOTI는 재전송 시 **동일 키·동일 원문** 유지

## NOTI에서 구현할 기능 (범위)

### 1. 전달 결과 해석

- ICOPAY로 `POST`(또는 합의된 메서드) 후:
 - `statusCode`, 응답 본문(일부만 저장 가능, 최대 길이 제한), 소요 시간(ms)
- (선택) JSON이면 `success` / `processed` 등 합의 필드 파싱

### 2. 재시도 정책

- **최대 시도 횟수**, **지수 백오프 + 지터**, **총 상한 시간**
- 동일 건 **동시 in-flight 방지**(락 또는 상태 머신)
- 재시도 대상 HTTP 코드를 **설정 테이블/환경변수**로 조정 가능하면 유지보수에 유리

### 3. 상태 영속화

- 건 단위 상태 예: `PENDING` → `DELIVERED` / `FAILED` / `DEAD_LETTER`
- 필드 예: `attemptCount`, `nextAttemptAt`, `lastHttpStatus`, `lastError`, `lastResponseSnippet`, `idempotencyKey`

### 4. DLQ·알림

- 최대 재시도 초과 시 **DEAD_LETTER** 및 운영 알림(슬랙/메일 등, NOTI 인프라에 맞게)
- (선택) 관리용 **수동 재전송** API/화면

### 5. 관측·추적

- 로그: 노티 ID, 주문/거래 식별자, ICOPAY URL(토큰 마스킹), `attempt`, `statusCode`
- (선택) 메트릭: 성공률, 재시도 횟수, DLQ 적재 건수

### 6. 기존 보안 유지

- ICOPAY가 요구하는 **HMAC·원문 기준 서명**, **허용 IP**, **TLS** — 재시도에도 **요청 원문 불변**

## ICOPAY에서 병행 개발할 내용 (NOTI와 합의)

- 서버-투-서버 JSON 노티 경로에 한해: 실패 시 **비-2xx** 또는 명시적 실패 JSON
- 브라우저 **RESULT 리다이렉트** 등 사용자 경로는 기존 동작 유지(경로 분리)
- 멱등 처리(동일 노티 중복 수신 시 안전)

## 테스트 시나리오 (NOTI)

1. ICOPAY가 **2xx** → NOTI는 재전송 안 함, 상태 `DELIVERED`
2. ICOPAY **5xx** 또는 타임아웃 → NOTI는 백오프 재시도, 상한 후 DLQ
3. ICOPAY **422**(합의된 재시도 가능 코드) → 재시도 후 성공 시 `DELIVERED`
4. ICOPAY **403** → 재시도 중단 또는 DLQ(설정에 따름)
5. 동일 건 **연속 POST** 시 NOTI 측 중복 스케줄이 나오지 않음
6. 재시도 시 **본문·서명**이 최초와 동일

## 비범위(이번 티켓에서 제외 가능)

- 메시지 큐 기반 전환(Kafka/SQS 등) — 필요 시 별도 과제
- ICOPAY 쪽 구현 상세 — 별도 저장소 이슈

## 참고

- PG 저장소 문서/이슈와 **합의된 HTTP 코드·JSON 스키마**를 단일 표로 고정한 뒤 양쪽 배포 권장

---

*본 문서는 ICOPAY(PG) 저장소에서 NOTI 팀·Cursor 작업 배포용으로 유지한다.*

## NOTI 미들웨어 구현 요약 (이 저장소)

| 문서 항목 | 구현 |
|-----------|------|
| 결과 해석 | `statusCode`, `durationMs`, 본문 프리뷰; JSON `success`/`processed`/`received`; `retryable` → 재시도 판별에 반영 |
| 422 재시도 | 기본 재시도; `PG_NOTIFY_RETRY_ON_422=0` 등으로 비활성화; 본문 `retryable: false` 시 재시도 안 함 |
| 403 등 4xx | 비재시도(408·429 제외) |
| 5xx·타임아웃 | 재시도 |
| 멱등 | 헤더 `Idempotency-Key` = 페이로드 기반 SHA-256(64hex), 재시도마다 동일 |
| 상태 | `data/pg-notify-delivery-jobs.json`, PENDING/DELIVERED/FAILED/DEAD_LETTER |
| DLQ 알림 | `PG_NOTIFY_DLQ_EMAIL_TO` + ChillPay SMTP; 콘솔·`mail-logs`에 `pg_notify_dlq` 기록 |
| 수동 재전송 | `/admin/pg-notify-delivery` (개발 노티 권한) |
| URL 로그 | DEAD_LETTER 콘솔·알림에서 pg-notify 토큰 마스킹 |

코드: `lib/pgNotifyDelivery.js`, `server.js`의 `sendToInternal` / `sendDevInternalHttpNotify` / pg-notify 배송 작업·워커.
