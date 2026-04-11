# NOTI(노티미들웨어) — ICOPAY 연동 재전송·HTTP 계약 개발 요청 (Cursor용)

## 배경·목표

- NOTI가 ICOPAY(`pg-app` 공개 엔드포인트, 예: `/api/open/pg-notify/{token}/…`)로 PG 노티를 전달한다.
- ICOPAY가 **수신·파싱·저장(및 합의된 후처리)에 실패**한 경우, NOTI는 이를 **명확히 구분**해 **재전송(백오프)** 할 수 있어야 한다.
- 권장 패턴: **동기 HTTP 계약** — 응답 **HTTP 상태 코드**와(선택) **JSON 본문**으로 성공/실패를 구분한다.

## 노티 연동 vs URL 결제 연동 — 반드시 구분 (결제대행사·연동 개발사)

**노티(Notify / 서버→서버 콜백)** 와 **URL 결제(브라우저 결제·결과 페이지 흐름)** 는 ICOPAY에서 **연동 목적·검증·가맹점 분기 규칙이 다릅니다.** 문서·명세·테스트를 한 덩어리로 취급하면 안 됩니다.

| 구분 | 역할 | ICOPAY 쪽 전제(요약) |
|------|------|----------------------|
| **노티** | 대행사·NOTI가 **Notify URL**로 보내는 **POST**(JSON 또는 폼). 승인/변경 결과를 **서버가 수신** | 배포설정에서 **노티 연동**(`integ_noti_yn`)이 켜진 바인딩 기준. 가맹점 분기는 **MID + 루트번호**가 중심. 서버 간이므로 **`Content-Type: application/json` POST** 는 **200 + JSON**(또는 합의된 422/503) 응답을 전제로 함(브라우저 리다이렉트와 혼동 금지). |
| **URL 결제** | 고객 **브라우저**가 결제/복귀 URL을 오감. 공통 MID·다가맹점 구조가 흔함 | **URL 결제** 연동(`integ_url_pay_yn`) 및 업체코드(**`compId` / `icopayCompId=`**) 요구가 노티와 **다를 수 있음**. **GET·폼 POST** 는 사용자 **결과 페이지(pay-result)** 로 보내는 경로와 맞물릴 수 있음. |

- **혼동 시 증상**: 노티인데 URL결제 전용 규칙(업체코드 강제·응답을 리다이렉트로 처리 등)을 적용하면 **수신 실패·재전송 중단·가맹점 오분기**가 난다.
- **권장**: 결제대행사·연동 개발사 납품 명세에 **「노티 URL 스펙」절과 「URL 결제(브라우저)」절을 분리**하고, 엔드포인트·필수 파라미터·성공 응답 형식(HTTP 상태·본문)을 채널별로 명시한다.

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

## NOTI → ICOPAY 요청 헤더 (수신 성격: 라이브 / 재전송) — **노티미들웨어 개발 요청용**

ICOPAY(`pg-app`)는 노티 수신 시 아래 헤더를 읽어 DB·관리 화면 **「노티수령정보」**의 **수신성격**에 `LIVE`(라이브) / `RETRY`(재전송) / `UNKNOWN`(미표시)를 저장한다. **헤더가 없으면 항상 미표시**이므로, 운영에서 구분을 쓰려면 NOTI가 **ICOPAY로 POST할 때** 헤더를 넣어야 한다.

### 1) 권장 헤더 (명시적)

| 헤더 이름 | 값 | 의미 |
|-----------|-----|------|
| **`X-Icopay-Notify-Delivery`** | `LIVE` | NOTI가 해당 건을 ICOPAY로 **최초 전달**하는 HTTP 요청(실시간 파이프라인의 첫 POST). |
| **`X-Icopay-Notify-Delivery`** | `RETRY` | 동일 논리 건(동일 `Idempotency-Key`·동일 원문 정책)에 대해 **이전 전달이 실패·재시도 대상**이어서 NOTI가 **다시** ICOPAY로 POST하는 요청. |

- 값은 **대소문자 무관** (`live` / `LIVE` 동일).
- 동일 의미의 별칭 헤더(ICOPAY가 동일하게 처리): **`X-Noti-Delivery`**, **`X-Notify-Delivery`** — 값은 위와 같이 `LIVE` 또는 `RETRY`.
- (선택) 레거시 호환: `PRIMARY`, `FIRST` → LIVE 취급 / `RESEND`, `DUPLICATE` → RETRY 취급.

**우선순위:** `X-Icopay-Notify-Delivery`(및 별칭 `X-Noti-Delivery`, `X-Notify-Delivery`)가 **비어 있지 않으면** 이 값이 **시도 번호 헤더보다 우선**한다.

### 2) 대안 헤더 (시도 횟수)

다음 헤더 **이름** 중 하나에 **정수**를 넣어도 된다. (여러 개 오면 구현에서 먼저 잡히는 하나를 사용하면 됨.)

| 헤더 이름 (예시) | 값 | ICOPAY 해석 |
|------------------|-----|-------------|
| `X-Noti-Attempt` | `1` | **LIVE** |
| `X-Noti-Attempt` | `2`, `3`, … | **RETRY** |
| `X-Delivery-Attempt` | 동일 규칙 | 동일 |
| `X-Retry-Attempt` | 동일 규칙 | 동일 |
| `X-Icopay-Notify-Attempt` | 동일 규칙 | 동일 |

- **`1` 이하(정수 1)** → LIVE, **`2` 이상** → RETRY.
- 숫자가 아니면 해당 헤더는 무시되고, 다른 헤더도 없으면 **미표시(UNKNOWN)**.

### 3) NOTI 구현 시 권장 동작

1. **첫 ICOPAY POST:** `X-Icopay-Notify-Delivery: LIVE` **또는** `X-Noti-Attempt: 1`.
2. **재전송 POST:** 본문·`Idempotency-Key`(합의 시) 등 **기존 정책과 동일**하게 유지하고, `X-Icopay-Notify-Delivery: RETRY` **또는** `X-Noti-Attempt`를 **2 이상**으로 올린다.
3. **브라우저**로 보내는 URL 결제 복귀 등 **ICOPAY 노티 URL이 아닌 경로**에는 적용하지 않아도 된다. 대상은 **서버-투-서버**로 ICOPAY `pg-notify`에 POST하는 노티만.

### 4) 요청 예시 (개발 참고)

```http
POST /api/open/pg-notify/{token}/cb01 HTTP/1.1
Host: {icopay-host}
Content-Type: application/json
X-Icopay-Notify-Delivery: RETRY
Idempotency-Key: {동일-키}

{…본문 JSON…}
```

```http
POST /api/open/pg-notify/{token}/cb01 HTTP/1.1
Host: {icopay-host}
Content-Type: application/json
X-Noti-Attempt: 2

{…본문 JSON…}
```

### 5) ICOPAY 쪽 참조

- 판별 로직: 저장소 `pg-app` — `com.pg.util.NotifyIngressDeliveryKindResolver`
- DB 컬럼: `tb_pg_notify_inbound.ingress_delivery_kind` (`LIVE` / `RETRY` / `NULL`·`UNKNOWN`)

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
| 수신성격(ICOPAY) | 내부 URL에 `/api/open/pg-notify/` 포함 시 `X-Icopay-Notify-Delivery` + `X-Noti-Attempt` 설정. 끄기: `PG_NOTIFY_ICOPAY_INGRESS_HEADERS=0` |
| 시도 번호 | 실시간 파이프라인 첫 POST=1(LIVE); 동일 흐름 내 2초 재시도=2(RETRY); 개발 노티 배송 job은 `attemptHistory` 길이·`icopayNotifyHttpAttemptBase`로 누적(프로세스 재개·DLQ 수동 재시도 시에도 RETRY 유지). 관리 화면 **전산/개발 재전송** 단발 POST는 `X-Noti-Attempt: 2`(RETRY) |
| 상태 | `data/pg-notify-delivery-jobs.json`, PENDING/DELIVERED/FAILED/DEAD_LETTER |
| DLQ 알림 | `PG_NOTIFY_DLQ_EMAIL_TO` + ChillPay SMTP; 콘솔·`mail-logs`에 `pg_notify_dlq` 기록 |
| 수동 재전송 | `/admin/pg-notify-delivery` (개발 노티 권한); DLQ 재시도 시 `manualRetries`를 `icopayNotifyHttpAttemptBase`에 반영 |
| URL 로그 | DEAD_LETTER 콘솔·알림에서 pg-notify 토큰 마스킹 |

코드: `lib/pgNotifyDelivery.js`, `server.js`의 `sendToInternal` / `sendDevInternalHttpNotify` / pg-notify 배송 작업·워커.
