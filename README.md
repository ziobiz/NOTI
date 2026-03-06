# PG 결제 노티 미들웨어

PG사 결제 결과 노티(웹훅)를 수신해  
1) **가맹점**에는 원본 그대로 릴레이하고,  
2) **전산**에는 PDF 4 규격으로 가공해 전송하는 서버입니다.

## 기술 스택

- Node.js, Express, Axios

## 설치 및 실행

```bash
npm init -y
npm install express axios
npm start
```

환경 변수(선택):

- `PORT` – 서버 포트 (기본 3000)
- `APP_ENV` – **test** | **production** (미설정 시 production). 테스트/라이브 설정 폴더 분리용.
- `INTERNAL_NOTI_URL` – 전산 노티 수신 URL (설정 시 가공 데이터 전송)

## API

- **POST /noti/:merchantId**  
  PG 노티 수신. `merchantId`로 가맹점 조회 후  
  - 가맹점 `callbackUrl`로 **원문 그대로** POST  
  - `INTERNAL_NOTI_URL`이 있으면 **가공 데이터**로 POST  

- **GET /health**  
  헬스 체크

## 가맹점 모킹

`server.js` 상단 `MERCHANTS` Map에서 `merchantId` → `callbackUrl`, `routeNo`, `internalCustomerId` 설정.  
실제 DB 연동 시 이 부분만 DB 조회로 교체하면 됩니다.

## 전산 가공 규격 (PDF 4)

- **Amount**: 통화별 소수점 변환 (예: 100배 단위 → 실제 금액)
- **RouteNo**: 가맹점별 루트 번호 추가
- **CustomerId**: 전산용 고정값으로 치환
- **CustomerName**: PG 값 유지 (없으면 빈 문자열)
- **CardNumber**: 마스킹값 `000000000000` 추가

금액/통화 규칙이 다르면 `transformForInternal()` 함수만 수정하면 됩니다.

## 테스트 환경 / 라이브 환경 분리

동일 코드로 **테스트(샌드박스)** 와 **라이브(운영)** 두 환경을 나누어 운영할 수 있습니다.

### 동작 방식

- **APP_ENV=test** → 설정 디렉터리: `config-test/` (가맹점, 회원, 전산 설정 등 모두 별도)
- **APP_ENV=production** 또는 미설정 → 설정 디렉터리: `config/` (기존과 동일)

서버 시작 시 로그에 `[test]` 또는 `[production]` 과 사용 중인 config 경로가 출력됩니다.

### 1) 같은 서버에서 두 프로세스로 실행 (권장)

```bash
# 라이브 (기본) – 포트 3000, config/
PORT=3000 APP_ENV=production node server.js

# 테스트 – 포트 3001, config-test/
PORT=3001 APP_ENV=test node server.js
```

**PM2 예시**

```bash
# 라이브
APP_ENV=production PORT=3000 pm2 start server.js --name pg-noti-live

# 테스트
APP_ENV=test PORT=3001 pm2 start server.js --name pg-noti-test

pm2 save
pm2 startup
```

- **라이브**: `https://noti.icopay.net` → Nginx에서 `localhost:3000`으로 프록시
- **테스트**: `https://test.noti.icopay.net` (또는 `noti.icopay.net:3001`) → Nginx에서 `localhost:3001`로 프록시

### 2) 설정 폴더 준비

- **라이브**: 기존처럼 `config/` 사용 (가맹점, 회원, 전산 노티 설정 등).
- **테스트**: 최초 실행 전에 `config-test/` 폴더를 만들고, 필요 시 `config/` 내용을 복사한 뒤 테스트용으로 수정합니다.

```bash
cp -r config config-test
# config-test/ 내부 JSON을 테스트용 URL·가맹점 정보로 수정
```

테스트 환경에서는 PG 샌드박스 URL, 테스트 가맹점, 테스트 전산 수신 URL만 넣어 두면 됩니다.

### 3) 정리

| 구분       | APP_ENV   | 설정 폴더   | 포트 예시 | 접속 예시                    |
|------------|-----------|-------------|-----------|-----------------------------|
| 라이브     | production | `config/`   | 3000      | https://noti.icopay.net     |
| 테스트     | test      | `config-test/` | 3001  | https://test.noti.icopay.net |

두 환경은 **설정·데이터가 완전히 분리**되므로, 테스트에서 실수해도 라이브 데이터에는 영향이 없습니다.

---

## 서버 배포

실제 서버에 올리는 방법은 **[DEPLOY.md](./DEPLOY.md)** 를 참고하세요.

- **VPS/VM**: PM2로 백그라운드 실행, Nginx 리버스 프록시·HTTPS
- **Docker**: `docker build` / `docker run`으로 배포
- **PaaS**: Railway, Render 등에 Git 연결 후 배포
