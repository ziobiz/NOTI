#!/usr/bin/env node
/**
 * 오늘(태국일) PG 노티는 수신됐으나 개발노티(OK)가 없는 건을 ICOPAY 등 개발 대상으로 일괄 재전송.
 *
 * 운영 서버 (pm2 중지 없이 별도 프로세스 1회 실행 권장):
 *   cd /path/to/NOTI
 *   NOTI_RUN_RESEND_DEV_MISSED=1 APP_ENV=production node server.js
 *
 * 미리보기만:
 *   NOTI_RUN_RESEND_DEV_MISSED=1 NOTI_RESEND_DEV_DRY_RUN=1 APP_ENV=production node server.js
 *
 * 특정 일자 (YYYY-MM-DD, 태국일):
 *   NOTI_RUN_RESEND_DEV_MISSED=1 NOTI_RESEND_DEV_DATE=2026-07-01 APP_ENV=production node server.js
 */
const path = require('path');
process.env.NOTI_RUN_RESEND_DEV_MISSED = '1';
require(path.join(__dirname, '..', 'server.js'));
