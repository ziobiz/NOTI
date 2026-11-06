#!/bin/bash
# pg-noti-relay 헬스 워치독 — 부팅 후 미기동·크래시·PM2 고아 상태 복구
# (ICOPAY pg-app 워치독과 동일 정책)
# cron: */5 * * * * root /srv/pg-noti/scripts/ops/pg-noti-watchdog.sh
set -euo pipefail

NOTI_DIR="${NOTI_DIR:-/srv/pg-noti}"
PM2_NAME="${PM2_NAME:-pg-noti-relay}"
PORT="${NOTI_PORT:-3000}"
HEALTH_URL="${NOTI_HEALTH_URL:-http://127.0.0.1:${PORT}/health}"

export PATH="/usr/local/bin:/usr/bin:/bin:${PATH}"

ok=0
if command -v curl >/dev/null 2>&1; then
  code=$(curl -sS -o /dev/null -w '%{http_code}' --connect-timeout 3 "$HEALTH_URL" 2>/dev/null || echo 000)
  if [[ "$code" == "200" ]]; then ok=1; fi
elif command -v ss >/dev/null 2>&1; then
  if ss -tlnp 2>/dev/null | grep -q ":${PORT}"; then ok=1; fi
fi

if [[ "$ok" -eq 1 ]]; then
  exit 0
fi

logger -t pg-noti-watchdog "health fail (${HEALTH_URL}) — attempting recovery"

if command -v systemctl >/dev/null 2>&1; then
  if systemctl list-unit-files 2>/dev/null | grep -q '^pm2-root.service'; then
    systemctl start pm2-root.service 2>/dev/null || true
  fi
fi

if command -v pm2 >/dev/null 2>&1; then
  cd "$NOTI_DIR" || true
  if pm2 describe "$PM2_NAME" >/dev/null 2>&1; then
    pm2 restart "$PM2_NAME" --update-env || true
  else
    pm2 start "$NOTI_DIR/server.js" --name "$PM2_NAME" || true
  fi
  pm2 save || true
  logger -t pg-noti-watchdog "invoked pm2 restart/start ${PM2_NAME}"
  exit 0
fi

logger -t pg-noti-watchdog "recovery failed: pm2 not found"
exit 1
