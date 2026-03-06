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
