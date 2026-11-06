# NOTI ↔ ICOPAY(pg-app) 부팅 자동 기동 동등 정책

## 왜 필수인가

ICOPAY `pg-app` 과 NOTI(`pg-noti-relay`) 는 한 결제 플로우의 양쪽이다.  
한쪽만 살아 있으면 웹훅·가맹 통보·노티거래내역이 끊긴다.

2026-11-06 pg-app: Power key 재부팅 → 자동 기동 없음 → 장시간 502.  
동일 위험이 NOTI에도 있으면 **같은 방어**를 둔다.

## NOTI 조치

| 항목 | 내용 |
|------|------|
| 부팅 자동 기동 | `pm2-root.service` (systemd enable) |
| 배포 | `pm2 restart` + **`pm2 save` 필수** |
| 5분 워치독 | `/health` 실패 시 pm2 restart + save |
| 설치 | `scripts/ops/install-pg-noti-autostart.sh` |

## 확인

```bash
systemctl is-enabled pm2-root
pm2 list
curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:3000/health
ls /etc/cron.d/pg-noti-watchdog
```
