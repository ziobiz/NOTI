# 서버 배포 가이드

이 프로젝트를 실제 서버에 올리는 방법입니다. 환경에 맞는 방법 하나를 선택하면 됩니다.

---

## 방법 1: VPS/클라우드 VM (Linux) + PM2

AWS EC2, GCP VM, 네이버 클라우드, 카페24 등 **Linux 서버**가 있을 때 추천합니다.

### 1) 서버에 Node.js 설치

```bash
# Ubuntu/Debian 예시 (Node 20 LTS)
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs
```

### 2) 프로젝트 업로드 후 실행

```bash
# 프로젝트 폴더로 이동
cd /home/your-user/Notificaiton   # 실제 경로로 변경

# 의존성 설치
npm install --production

# 환경 변수 설정 (전산 API 사용 시)
export PORT=3000
export INTERNAL_NOTI_URL=https://your-internal-api.com/noti

# PM2로 백그라운드 실행 (재부팅 시 자동 재시작은 아래 참고)
npx pm2 start server.js --name "pg-noti-relay"
npx pm2 save
npx pm2 startup   # 부팅 시 자동 실행 설정
```

### 3) (선택) Nginx 리버스 프록시 + HTTPS

80/443 포트로 받고, Node는 내부 포트(예: 3000)로 넘기려면:

```nginx
# /etc/nginx/sites-available/noti-relay
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/noti-relay /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

HTTPS는 Let's Encrypt 사용: `sudo apt install certbot python3-certbot-nginx && sudo certbot --nginx`

#### HTTPS 인증서 방식 정리 (이 프로젝트 문서 기준)

- **TLS 종료**: Nginx가 443에서 받고, Node는 내부 포트(예: 3000)만 사용합니다.
- **인증서**: Certbot **nginx** 플러그인으로 발급한 Let’s Encrypt PEM이 일반적으로  
  `/etc/letsencrypt/live/<도메인>/fullchain.pem` 에 있습니다.

#### 자동 갱신 (권장: 운영체제)

Ubuntu/Debian 등에서는 패키지 설치 시 **`certbot.timer`** 가 등록되는 경우가 많습니다.  
아래로 확인한 뒤 **enabled** 이면 별도 설정 없이 주기적으로 `certbot renew` 가 돌아갑니다.

```bash
systemctl status certbot.timer
```

갱신 후 Nginx에 새 인증서를 쓰게 하려면, 발급 당시 `--nginx` 플러그인을 썼다면 renewal 설정에 훅이 잡혀 있는 경우가 많습니다.  
수동으로 맞추려면 `/etc/letsencrypt/renewal/*.conf` 의 `[renewalparams]` / 훅 문서를 참고하거나,  
갱신 후 `sudo systemctl reload nginx` 를 renewal hook에 넣으면 됩니다.

#### 관리 화면(서버 관리)에서 만료일 보기

Node 프로세스가 PEM 파일을 **읽을 수 있어야** 합니다. 환경변수 예시는 `deploy/ssl-monitor.env.example` 를 참고하세요.

- **`MONITOR_SSL_CERT_PATH`**: `fullchain.pem` 절대 경로(가장 명확).
- **자동 탐지(Linux)**: 위 변수를 비우면 `/etc/letsencrypt/live` 아래에 **인증서 디렉터리가 하나뿐**일 때 그 `fullchain.pem` 을 씁니다.  
  서버에 여러 도메인 인증서가 있으면 **`MONITOR_SSL_LE_DOMAIN=your-domain.com`** 으로 고정하세요.

#### 앱에서 갱신 명령까지 돌리기 (선택)

기본 권장은 **시스템 타이머만** 사용하는 것입니다.  
그래도 관리 화면의 스케줄/버튼으로 `certbot renew` 를 호출하려면:

1. `MONITOR_SSL_RENEW_CMD` 에 실행할 셸 한 줄을 넣고  
2. `MONITOR_SSL_AUTO_RENEW_ENABLED=1` 로 켭니다.  
3. Node 사용자가 `certbot`(및 필요 시 `systemctl reload nginx`)을 **비대화형**으로 실행할 수 있게 sudoers 등을 맞춰야 합니다.

예시 스크립트: `scripts/ssl-renew-certbot-nginx.sh` (서버에서 실행 권한 부여 후 경로를 `MONITOR_SSL_RENEW_CMD`에 지정).

---

## 방법 2: Docker로 배포

Docker가 있는 서버(또는 Docker 호스팅)에서 사용합니다.

### 빌드 및 실행

```bash
# 이미지 빌드
docker build -t pg-noti-relay .

# 실행 (환경 변수는 -e 로 전달)
docker run -d --name pg-noti-relay -p 3000:3000 \
  -e INTERNAL_NOTI_URL=https://your-internal-api.com/noti \
  pg-noti-relay
```

---

## 방법 3: PaaS (Heroku, Railway, Render 등)

코드를 Git에 올려두고, 서비스에 연결해 배포하는 방식입니다.

### 공통 설정

- **시작 명령**: `npm start` (이미 `package.json`에 있음)
- **환경 변수**: 대시보드에서 `PORT`(필요 시), `INTERNAL_NOTI_URL` 설정
- **포트**: PaaS가 주는 `PORT`를 쓰면 되므로 코드 수정 불필요

### Railway 예시

1. [railway.app](https://railway.app) 로그인 후 New Project → Deploy from GitHub
2. 저장소 선택 후 배포
3. Variables에서 `INTERNAL_NOTI_URL` 추가
4. 생성된 URL이 노티 수신 주소 (예: `https://xxx.railway.app/noti/merchant_test`)

### Render 예시

1. [render.com](https://render.com) → New → Web Service
2. 저장소 연결, Build Command: `npm install`, Start Command: `npm start`
3. Environment에 `INTERNAL_NOTI_URL` 추가

---

## 배포 후 확인

- **헬스 체크**: `GET https://your-server/health` → `{"status":"ok"}`
- **노티 테스트**: `POST https://your-server/noti/merchant_test` Body에 PG 노티 JSON 전송

---

## 체크리스트

| 항목 | 설명 |
|------|------|
| **방화벽** | 서버에서 노티를 받을 포트(80/443 또는 3000) 오픈 |
| **환경 변수** | `INTERNAL_NOTI_URL`, 필요 시 `PORT` 설정 |
| **가맹점 정보** | 실제 가맹점은 `server.js`의 `MERCHANTS` 또는 추후 DB로 이전 |
| **HTTPS** | PG사/가맹점이 HTTPS만 허용하면 Nginx 등으로 SSL 적용 |
| **인증서 자동 갱신** | Linux+VPS면 `certbot.timer` 활성화 확인(위 "HTTPS 인증서 및 자동 갱신"). 관리 화면 연동은 `deploy/ssl-monitor.env.example` 참고 |
