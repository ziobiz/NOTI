#!/usr/bin/env sh
# Optional hook for MONITOR_SSL_RENEW_CMD (관리 화면·앱 스케줄에서 갱신 시도 시).
# 기본 자동 갱신은 OS의 certbot.timer 가 담당하는 것이 안전합니다(DEPLOY.md 참고).
# 이 스크립트는 sudo 없이 root 로만 실행하거나, deploy 사용자에 NOPASSWD 를 줄 때 사용합니다.
#
# 예: MONITOR_SSL_RENEW_CMD=/path/to/Notificaiton/scripts/ssl-renew-certbot-nginx.sh
set -eu
certbot renew -q
if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet nginx 2>/dev/null; then
  systemctl reload nginx
fi
