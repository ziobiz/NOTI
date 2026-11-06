#!/bin/bash
# NOTI = ICOPAY 연동 미들웨어. PG(pg-app)와 동일: 부팅 자동 기동 + 5분 워치독.
# 사용: sudo bash install-pg-noti-autostart.sh
set -euo pipefail

NOTI_DIR="${NOTI_DIR:-/srv/pg-noti}"
PM2_NAME="${PM2_NAME:-pg-noti-relay}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WATCHDOG_DST="${NOTI_DIR}/scripts/ops/pg-noti-watchdog.sh"
CRON_DST="/etc/cron.d/pg-noti-watchdog"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "root 로 실행하세요: sudo bash $0"
  exit 1
fi

if [[ ! -f "$NOTI_DIR/server.js" ]]; then
  echo "없음: $NOTI_DIR/server.js"
  exit 1
fi

export PATH="/usr/local/bin:/usr/bin:/bin:${PATH}"
if ! command -v pm2 >/dev/null 2>&1; then
  echo "오류: pm2 없음. npm i -g pm2 후 재실행"
  exit 1
fi

mkdir -p "$(dirname "$WATCHDOG_DST")"
SRC_WD="$SCRIPT_DIR/pg-noti-watchdog.sh"
if [[ -f "$SRC_WD" ]]; then
  # 이미 $WATCHDOG_DST 위치에서 실행 중이면 cp 생략 (same file 오류 방지)
  if [[ "$(readlink -f "$SRC_WD" 2>/dev/null || echo "$SRC_WD")" != "$(readlink -f "$WATCHDOG_DST" 2>/dev/null || echo "$WATCHDOG_DST")" ]]; then
    cp -f "$SRC_WD" "$WATCHDOG_DST"
  fi
fi
chmod 755 "$WATCHDOG_DST"

if ! systemctl is-enabled pm2-root.service >/dev/null 2>&1; then
  echo "pm2-root 미 enable → pm2 startup 적용"
  env PATH="$PATH" pm2 startup systemd -u root --hp /root >/tmp/pm2-startup.out 2>&1 || true
  tail -n 20 /tmp/pm2-startup.out || true
  systemctl enable pm2-root.service 2>/dev/null || true
fi
systemctl start pm2-root.service 2>/dev/null || true

cd "$NOTI_DIR"
if pm2 describe "$PM2_NAME" >/dev/null 2>&1; then
  # 설치만으로 불필요 재시작하지 않음 — dump 갱신만
  :
else
  PORT="${NOTI_PORT:-3000}" APP_ENV=production NODE_ENV=production \
    pm2 start server.js --name "$PM2_NAME" || true
fi
pm2 save
systemctl enable pm2-root.service 2>/dev/null || true

cat >"$CRON_DST" <<EOF
# ICOPAY NOTI: /health 다운 시 자동 복구 (5분) — pg-app 워치독과 동일 정책
*/5 * * * * root NOTI_DIR=${NOTI_DIR} PM2_NAME=${PM2_NAME} ${WATCHDOG_DST} >/dev/null 2>&1
EOF
chmod 644 "$CRON_DST"

echo "=== verify ==="
systemctl is-enabled pm2-root.service || true
systemctl is-active pm2-root.service || true
pm2 list | head -n 20
curl -sS -o /dev/null -w "health=%{http_code}\n" --connect-timeout 3 "http://127.0.0.1:${NOTI_PORT:-3000}/health" || true
ls -la "$CRON_DST" "$WATCHDOG_DST"
echo "install OK. 부팅 시 PM2 자동 기동 + 5분 워치독 활성."
