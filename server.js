/**
 * PG 결제 노티 미들웨어 서버
 * 1) 가맹점 callbackUrl로 수신 데이터 그대로 릴레이
 * 2) 전산 저장용으로 가공 후 내부 API로 전송 (PDF 4 규격)
 */
const express = require('express');
const axios = require('axios');
const session = require('express-session');
const cookieParser = require('cookie-parser');
const speakeasy = require('speakeasy');
const fs = require('fs');
const path = require('path');
const bcrypt = require('bcryptjs');
const QRCode = require('qrcode');
const crypto = require('crypto');
const { t } = require('./locales');

const app = express();
const SUPPORTED_LOCALES = ['ko', 'ja', 'en', 'th', 'zh'];
function getLocale(req) {
  const lang = req.cookies?.lang || req.query?.lang || 'ko';
  return SUPPORTED_LOCALES.includes(lang) ? lang : 'ko';
}
function setLocaleCookie(res, lang) {
  if (SUPPORTED_LOCALES.includes(lang)) {
    res.cookie('lang', lang, { maxAge: 365 * 24 * 60 * 60 * 1000, httpOnly: false });
  }
}
// PG 노티(/noti/*)는 원문 그대로 가맹점에 전달하기 위해 raw body 선점
app.use((req, res, next) => {
  const isNotiPost = (req.path.startsWith('/noti/') || req.path === '/noti') && req.method === 'POST';
  if (!isNotiPost) return next();
  const rawParser = express.raw({ type: () => true });
  rawParser(req, res, (err) => {
    if (err) return next(err);
    const buf = Buffer.isBuffer(req.body) ? req.body : Buffer.from(String(req.body || ''));
    req.rawBodyBuffer = buf;
    const ct = (req.headers['content-type'] || '').split(';')[0].trim().toLowerCase();
    if (ct.includes('application/json')) {
      try { req.body = JSON.parse(buf.toString('utf8')); } catch { req.body = {}; }
    } else if (ct.includes('application/x-www-form-urlencoded')) {
      req.body = Object.fromEntries(new URLSearchParams(buf.toString('utf8')));
    } else {
      req.body = {};
    }
    next();
  });
});
app.use((req, res, next) => {
  if ((req.path.startsWith('/noti/') || req.path === '/noti') && req.method === 'POST') return next();
  express.json()(req, res, next);
});
app.use((req, res, next) => {
  if ((req.path.startsWith('/noti/') || req.path === '/noti') && req.method === 'POST') return next();
  express.urlencoded({ extended: true })(req, res, next);
});
app.use(cookieParser());
app.use(
  session({
    secret: process.env.SESSION_SECRET || 'change-this-secret',
    resave: false,
    saveUninitialized: false,
    cookie: { maxAge: 60 * 60 * 1000 },
  }),
);

// 정적 파일 (favicon.ico 등) — static 폴더
const staticDir = path.join(__dirname, 'static');
if (!fs.existsSync(staticDir)) fs.mkdirSync(staticDir, { recursive: true });
app.use('/static', express.static(staticDir));

// 브라우저 기본 파비콘 경로(/favicon.ico) 지원
// - 환경설정(site-settings.json)의 favicon 값이 있으면 그 URL로 리다이렉트
// - 없으면 static/favicon.ico 파일이 있으면 그 파일 제공
app.get('/favicon.ico', (req, res) => {
  const site = loadSiteSettings();
  const faviconUrl = (site && site.favicon) ? String(site.favicon).trim() : '';
  res.setHeader('Cache-Control', 'no-cache');
  if (faviconUrl) return res.redirect(faviconUrl);
  const localPath = path.join(staticDir, 'favicon.ico');
  if (fs.existsSync(localPath)) return res.sendFile(localPath);
  return res.status(204).end();
});

// 트래픽 수집 (모든 요청)
app.use((req, res, next) => {
  const pathname = (req.path || req.url || '').split('?')[0];
  TRAFFIC_HITS.push({ path: pathname, at: new Date().toISOString() });
  if (TRAFFIC_HITS.length > TRAFFIC_HITS_MAX) TRAFFIC_HITS.shift();
  next();
});

const RELAY_TIMEOUT_MS = 15000;
const INTERNAL_TIMEOUT_MS = 10000;

// ===== 테스트 / 라이브 환경 분리 (APP_ENV=test | production) =====
const APP_ENV = (process.env.APP_ENV || process.env.NODE_ENV || 'production').toLowerCase() === 'test' ? 'test' : 'production';
const CONFIG_DIR = path.join(__dirname, APP_ENV === 'test' ? 'config-test' : 'config');
const ADMIN_CONFIG_PATH = path.join(CONFIG_DIR, 'admin.json');
const MEMBERS_CONFIG_PATH = path.join(CONFIG_DIR, 'members.json');
const OPERATOR_PERMISSIONS_PATH = path.join(CONFIG_DIR, 'operator-permissions.json');
const PASSWORD_RESET_REQUESTS_PATH = path.join(CONFIG_DIR, 'password-reset-requests.json');
const MERCHANTS_CONFIG_PATH = path.join(CONFIG_DIR, 'merchants.json');

// 역할: SUPER_ADMIN > ADMIN > OPERATOR
const ROLES = { SUPER_ADMIN: 'SUPER_ADMIN', ADMIN: 'ADMIN', OPERATOR: 'OPERATOR' };
const PAGE_KEYS = [
  'merchants',
  'pg_logs',
  'internal_logs',
  'dev_internal_logs',
  'pg_result',
  'internal_result',
  'dev_result',
  'traffic_analysis',
  'internal_targets',
  'internal_noti_settings',
  'dev_internal_noti_settings',
  'test_config',
  'test_run',
  'test_history',
  'account',
  'settings',
  'account_reset',
  'cancel_refund',
];
const INITIAL_PASSWORD_SUFFIX = '1!';
const INTERNAL_TARGETS_CONFIG_PATH = path.join(CONFIG_DIR, 'internal-targets.json');
const INTERNAL_NOTI_SETTINGS_PATH = path.join(CONFIG_DIR, 'internal-noti-settings.json');
const DEV_INTERNAL_NOTI_SETTINGS_PATH = path.join(CONFIG_DIR, 'dev-internal-noti-settings.json');
const SITE_SETTINGS_PATH = path.join(CONFIG_DIR, 'site-settings.json');
const TEST_CONFIGS_CONFIG_PATH = path.join(CONFIG_DIR, 'test-configs.json');
const CHILLPAY_TRANSACTION_CONFIG_PATH = path.join(CONFIG_DIR, 'chillpay-transaction.json');

const DEFAULT_SIDEBAR_TITLE = 'PG 노티 관리자';
const DEFAULT_SIDEBAR_SUB = 'Webhooks & Internal Notices';

function loadSiteSettings() {
  try {
    const raw = fs.readFileSync(SITE_SETTINGS_PATH, 'utf8');
    const o = JSON.parse(raw);
    return {
      sidebarTitle: (o && o.sidebarTitle != null && String(o.sidebarTitle).trim()) ? String(o.sidebarTitle).trim() : DEFAULT_SIDEBAR_TITLE,
      sidebarSub: (o && o.sidebarSub != null) ? String(o.sidebarSub).trim() : DEFAULT_SIDEBAR_SUB,
      pageTitle: (o && o.pageTitle != null) ? String(o.pageTitle).trim() : '',
      favicon: (o && o.favicon != null) ? String(o.favicon).trim() : '',
    };
  } catch (e) {
    return { sidebarTitle: DEFAULT_SIDEBAR_TITLE, sidebarSub: DEFAULT_SIDEBAR_SUB, pageTitle: '', favicon: '' };
  }
}

function saveSiteSettings(o) {
  ensureConfigDir();
  const cur = loadSiteSettings();
  const toSave = {
    sidebarTitle: (o && o.sidebarTitle != null) ? String(o.sidebarTitle).trim() || cur.sidebarTitle : cur.sidebarTitle,
    sidebarSub: (o && o.sidebarSub != null) ? String(o.sidebarSub).trim() : cur.sidebarSub,
    pageTitle: (o && o.pageTitle != null) ? String(o.pageTitle).trim() : cur.pageTitle,
    favicon: (o && o.favicon != null) ? String(o.favicon).trim() : cur.favicon,
  };
  fs.writeFileSync(SITE_SETTINGS_PATH, JSON.stringify(toSave, null, 2));
  return toSave;
}

// ========== ChillPay Transaction API (무효/환불). 취소는 노티 수신만, 무효/환불만 API 요청 가능 ==========
// 기준 시간: 태국 시간(ICT)을 기준으로 구간을 나누고, 화면에는 일본 시간(JST)도 함께 표기.
// 기본값(일본 시간 기준 설명):
// - 자동 무효 가능(자동 무효 버튼 활성): 당일 00:01 ~ 21:00
// - 수동(이메일) 환불 요청 가능       : 21:01 ~ 다음날 23:59
// - 환불 가능 일자                    : 다음날 00:00부터 N일간 (기본 7일, 환경설정에서 조정 가능)
const DEFAULT_VOID_CUTOFF_HOUR = 21;
const DEFAULT_VOID_CUTOFF_MINUTE = 0;
// 환불 시작 시각은 "다음날 00:00"을 의미하며, 실제 날짜 보정은 getVoidRefundWindow에서 처리.
const DEFAULT_REFUND_START_HOUR = 0;
const DEFAULT_REFUND_START_MINUTE = 0;
const DEFAULT_CHILLPAY_TIMEZONE = 'Asia/Bangkok';
const DEFAULT_SYNC_RESULT_DISPLAY_MINUTES = 30;
const DEFAULT_REFUND_WINDOW_DAYS = 7;
const DEFAULT_PG_TRANSACTION_SYNC_INTERVAL_MINUTES = 30;
const DEFAULT_PG_TRANSACTION_INITIAL_SYNC_MONTHS = 3;
const DEFAULT_AMOUNT_DISPLAY_OP = '/'; // '*', '/', '+', '-'
const DEFAULT_AMOUNT_DISPLAY_VALUE = 100;

function loadChillPayTransactionConfig() {
  try {
    let raw;
    try {
      // 기본 경로 (APP_ENV에 따라 config 또는 config-test)
      raw = fs.readFileSync(CHILLPAY_TRANSACTION_CONFIG_PATH, 'utf8');
    } catch (e) {
      // 테스트 환경(config-test)에서 파일이 없으면 운영(config) 경로를 한 번 더 시도
      try {
        const fallbackPath = path.join(__dirname, 'config', 'chillpay-transaction.json');
        raw = fs.readFileSync(fallbackPath, 'utf8');
      } catch (e2) {
        throw e; // 둘 다 없으면 기존 처리로 이동
      }
    }
    const o = JSON.parse(raw);
    return {
      sandbox: (o && o.sandbox) ? { mid: String(o.sandbox.mid || '').trim(), apiKey: String(o.sandbox.apiKey || '').trim(), md5: String(o.sandbox.md5 || '').trim() } : { mid: '', apiKey: '', md5: '' },
      production: (o && o.production) ? { mid: String(o.production.mid || '').trim(), apiKey: String(o.production.apiKey || '').trim(), md5: String(o.production.md5 || '').trim() } : { mid: '', apiKey: '', md5: '' },
      voidCutoffHour: Number.isFinite(o && o.voidCutoffHour) ? o.voidCutoffHour : DEFAULT_VOID_CUTOFF_HOUR,
      voidCutoffMinute: Number.isFinite(o && o.voidCutoffMinute) ? o.voidCutoffMinute : DEFAULT_VOID_CUTOFF_MINUTE,
      refundStartHour: Number.isFinite(o && o.refundStartHour) ? o.refundStartHour : DEFAULT_REFUND_START_HOUR,
      refundStartMinute: Number.isFinite(o && o.refundStartMinute) ? o.refundStartMinute : DEFAULT_REFUND_START_MINUTE,
      refundWindowDays: Number.isFinite(o && o.refundWindowDays) && o.refundWindowDays > 0 ? o.refundWindowDays : DEFAULT_REFUND_WINDOW_DAYS,
      amountDisplayOp: (o && typeof o.amountDisplayOp === 'string' && ['*', '/', '+', '-'].includes(o.amountDisplayOp)) ? o.amountDisplayOp : DEFAULT_AMOUNT_DISPLAY_OP,
      amountDisplayValue: Number.isFinite(o && o.amountDisplayValue) ? o.amountDisplayValue : DEFAULT_AMOUNT_DISPLAY_VALUE,
      syncResultDisplayMinutes: Number.isFinite(o && o.syncResultDisplayMinutes) && o.syncResultDisplayMinutes > 0 ? o.syncResultDisplayMinutes : DEFAULT_SYNC_RESULT_DISPLAY_MINUTES,
      pgTransactionSyncIntervalMinutes: Number.isFinite(o && o.pgTransactionSyncIntervalMinutes) && o.pgTransactionSyncIntervalMinutes > 0 ? Math.min(1440, o.pgTransactionSyncIntervalMinutes) : DEFAULT_PG_TRANSACTION_SYNC_INTERVAL_MINUTES,
      pgTransactionInitialSyncMonths: Number.isFinite(o && o.pgTransactionInitialSyncMonths) && o.pgTransactionInitialSyncMonths > 0 ? Math.min(60, o.pgTransactionInitialSyncMonths) : DEFAULT_PG_TRANSACTION_INITIAL_SYNC_MONTHS,
      timezone: (o && o.timezone && String(o.timezone).trim()) ? String(o.timezone).trim() : DEFAULT_CHILLPAY_TIMEZONE,
      useSandbox: !!(o && o.useSandbox === true),
      emailFrom: (o && o.emailFrom != null) ? String(o.emailFrom).trim() : '',
      companyName: (o && o.companyName != null) ? String(o.companyName).trim() : '',
      contactName: (o && o.contactName != null) ? String(o.contactName).trim() : '',
      emailTo: (o && o.emailTo != null) ? String(o.emailTo).trim() : 'help@chillpay.co.th',
      emailBodyTemplate: (o && o.emailBodyTemplate != null) ? String(o.emailBodyTemplate).trim() : '아래 거래에 대해 취소를 요청합니다.\n\nTransactionId(transNo): {{transNo}}\nOrderNo: {{orderNo}}\nAmount: {{amount}}\nRoute No. {{routeNo}}\nPaymentDate: {{paymentDate}}\nMID: {{mid}}\n',
    };
  } catch (e) {
    return {
      sandbox: { mid: '', apiKey: '', md5: '' },
      production: { mid: '', apiKey: '', md5: '' },
      voidCutoffHour: DEFAULT_VOID_CUTOFF_HOUR,
      voidCutoffMinute: DEFAULT_VOID_CUTOFF_MINUTE,
      refundStartHour: DEFAULT_REFUND_START_HOUR,
      refundStartMinute: DEFAULT_REFUND_START_MINUTE,
      refundWindowDays: DEFAULT_REFUND_WINDOW_DAYS,
      syncResultDisplayMinutes: DEFAULT_SYNC_RESULT_DISPLAY_MINUTES,
      pgTransactionSyncIntervalMinutes: DEFAULT_PG_TRANSACTION_SYNC_INTERVAL_MINUTES,
      pgTransactionInitialSyncMonths: DEFAULT_PG_TRANSACTION_INITIAL_SYNC_MONTHS,
      timezone: DEFAULT_CHILLPAY_TIMEZONE,
      useSandbox: APP_ENV === 'test',
      emailFrom: '',
      companyName: '',
      contactName: '',
      emailTo: 'help@chillpay.co.th',
      emailBodyTemplate: '아래 거래에 대해 취소를 요청합니다.\n\nTransactionId(transNo): {{transNo}}\nOrderNo: {{orderNo}}\nAmount: {{amount}}\nRoute No. {{routeNo}}\nPaymentDate: {{paymentDate}}\nMID: {{mid}}\n',
    };
  }
}

function saveChillPayTransactionConfig(o) {
  ensureConfigDir();
  const cur = loadChillPayTransactionConfig();
  const toSave = {
    sandbox: o && o.sandbox ? { mid: String(o.sandbox.mid || '').trim(), apiKey: String(o.sandbox.apiKey || '').trim(), md5: String(o.sandbox.md5 || '').trim() } : cur.sandbox,
    production: o && o.production ? { mid: String(o.production.mid || '').trim(), apiKey: String(o.production.apiKey || '').trim(), md5: String(o.production.md5 || '').trim() } : cur.production,
    voidCutoffHour: o && Number.isFinite(o.voidCutoffHour) ? o.voidCutoffHour : cur.voidCutoffHour,
    voidCutoffMinute: o && Number.isFinite(o.voidCutoffMinute) ? o.voidCutoffMinute : cur.voidCutoffMinute,
    refundStartHour: o && Number.isFinite(o.refundStartHour) ? o.refundStartHour : cur.refundStartHour,
    refundStartMinute: o && Number.isFinite(o.refundStartMinute) ? o.refundStartMinute : cur.refundStartMinute,
    refundWindowDays: o && Number.isFinite(o.refundWindowDays) && o.refundWindowDays > 0 ? o.refundWindowDays : cur.refundWindowDays,
    syncResultDisplayMinutes: o && Number.isFinite(o.syncResultDisplayMinutes) && o.syncResultDisplayMinutes > 0 ? o.syncResultDisplayMinutes : cur.syncResultDisplayMinutes,
    pgTransactionSyncIntervalMinutes: o && Number.isFinite(o.pgTransactionSyncIntervalMinutes) && o.pgTransactionSyncIntervalMinutes > 0 ? Math.min(1440, o.pgTransactionSyncIntervalMinutes) : cur.pgTransactionSyncIntervalMinutes,
    pgTransactionInitialSyncMonths: o && Number.isFinite(o.pgTransactionInitialSyncMonths) && o.pgTransactionInitialSyncMonths > 0 ? Math.min(60, o.pgTransactionInitialSyncMonths) : cur.pgTransactionInitialSyncMonths,
    timezone: (o && o.timezone != null && String(o.timezone).trim()) ? String(o.timezone).trim() : cur.timezone,
    amountDisplayOp: (o && typeof o.amountDisplayOp === 'string' && ['*', '/', '+', '-'].includes(o.amountDisplayOp)) ? o.amountDisplayOp : cur.amountDisplayOp || DEFAULT_AMOUNT_DISPLAY_OP,
    amountDisplayValue: o && Number.isFinite(o.amountDisplayValue) ? o.amountDisplayValue : cur.amountDisplayValue,
    useSandbox: o && typeof o.useSandbox === 'boolean' ? o.useSandbox : cur.useSandbox,
    emailFrom: (o && o.emailFrom != null) ? String(o.emailFrom).trim() : cur.emailFrom,
    companyName: (o && o.companyName != null) ? String(o.companyName).trim() : cur.companyName,
    contactName: (o && o.contactName != null) ? String(o.contactName).trim() : cur.contactName,
    emailTo: (o && o.emailTo != null) ? String(o.emailTo).trim() : cur.emailTo,
    emailBodyTemplate: (o && o.emailBodyTemplate != null) ? String(o.emailBodyTemplate).trim() : cur.emailBodyTemplate,
  };
  fs.writeFileSync(CHILLPAY_TRANSACTION_CONFIG_PATH, JSON.stringify(toSave, null, 2));
  return toSave;
}

function ensureConfigDir() {
  if (!fs.existsSync(CONFIG_DIR)) {
    fs.mkdirSync(CONFIG_DIR, { recursive: true });
  }
}

function maskChillPaySecret(val) {
  const s = val && typeof val === 'string' ? val.trim() : '';
  if (s.length <= 2) return s;
  if (s.length <= 4) return s.substring(0, 2) + '*'.repeat(Math.max(0, s.length - 2));
  const midLen = s.length - 4;
  return s.substring(0, 2) + '*'.repeat(midLen) + s.substring(s.length - 2);
}

function loadAdminConfig() {
  try {
    const raw = fs.readFileSync(ADMIN_CONFIG_PATH, 'utf8');
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

function saveAdminConfig(config) {
  ensureConfigDir();
  fs.writeFileSync(ADMIN_CONFIG_PATH, JSON.stringify(config, null, 2));
}

ensureConfigDir();
let adminConfig = loadAdminConfig();
if (!adminConfig) {
  const username = process.env.ADMIN_USER || 'admin';
  const password = process.env.ADMIN_PASSWORD || 'admin1234';
  const passwordHash = bcrypt.hashSync(password, 10);
  adminConfig = {
    username,
    passwordHash,
    otpSecret: process.env.ADMIN_OTP_SECRET || '',
  };
  saveAdminConfig(adminConfig);
}

// ===== 회원(멀티 역할) 저장소: SUPER_ADMIN, ADMIN, OPERATOR =====
function loadMembers() {
  try {
    const raw = fs.readFileSync(MEMBERS_CONFIG_PATH, 'utf8');
    const arr = JSON.parse(raw);
    const list = Array.isArray(arr) ? arr : [];
    // 기본값 보정 (구버전 members.json 호환)
    return list.map((m) => ({
      ...m,
      otpFailCount: Number.isFinite(m && m.otpFailCount) ? m.otpFailCount : 0,
      otpLocked: m && m.otpLocked === true,
    }));
  } catch (e) {
    return [];
  }
}

function saveMembers(members) {
  ensureConfigDir();
  fs.writeFileSync(MEMBERS_CONFIG_PATH, JSON.stringify(members, null, 2));
}

function loadOperatorPermissions() {
  try {
    const raw = fs.readFileSync(OPERATOR_PERMISSIONS_PATH, 'utf8');
    const obj = JSON.parse(raw);
    return obj && typeof obj === 'object' ? obj : {};
  } catch (e) {
    return {};
  }
}

function saveOperatorPermissions(perm) {
  ensureConfigDir();
  fs.writeFileSync(OPERATOR_PERMISSIONS_PATH, JSON.stringify(perm, null, 2));
}

function loadPasswordResetRequests() {
  try {
    const raw = fs.readFileSync(PASSWORD_RESET_REQUESTS_PATH, 'utf8');
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch (e) {
    return [];
  }
}

function savePasswordResetRequests(arr) {
  ensureConfigDir();
  fs.writeFileSync(PASSWORD_RESET_REQUESTS_PATH, JSON.stringify(arr, null, 2));
}

let MEMBERS = loadMembers();
let OPERATOR_PERMISSIONS = loadOperatorPermissions();

function getMemberByUserId(userId) {
  return (MEMBERS || []).find((m) => m.userId === userId);
}
function getMemberById(id) {
  return (MEMBERS || []).find((m) => m.id === id);
}
function getMemberPermissions(userId) {
  return OPERATOR_PERMISSIONS[userId] || [];
}

// 최초 실행 시 admin.json 기반으로 SUPER_ADMIN 1명 생성
if (!MEMBERS || MEMBERS.length === 0) {
  const un = adminConfig.username || 'admin';
  MEMBERS = [
    {
      id: 'member-' + Date.now(),
      role: ROLES.SUPER_ADMIN,
      name: 'Super Admin',
      country: '',
      userId: un,
      email: '',
      birthDate: '',
      passwordHash: adminConfig.passwordHash,
      otpSecret: adminConfig.otpSecret || '',
      otpRequired: false,
      otpFailCount: 0,
      otpLocked: false,
      canAssignPermission: false,
      mustChangePassword: false,
      createdAt: new Date().toISOString(),
    },
  ];
  saveMembers(MEMBERS);
}

function verifyOtp(secret, token) {
  if (!secret) return false;
  if (!token) return false;
  try {
    return speakeasy.totp.verify({
      secret,
      encoding: 'base32',
      token,
      window: 1,
    });
  } catch (e) {
    console.error('[OTP 검증 실패]', e.message);
    return false;
  }
}

function requireAuth(req, res, next) {
  if (req.session && req.session.member) {
    return next();
  }
  return res.redirect('/admin/login');
}

function requireAdmin(req, res, next) {
  if (req.session && req.session.member) return next();
  if (req.session && req.session.adminUser) return next();
  return res.redirect('/admin/login');
}

function requireRole(roles) {
  const arr = Array.isArray(roles) ? roles : [roles];
  return (req, res, next) => {
    if (!req.session || !req.session.member) return res.redirect('/admin/login');
    if (arr.includes(req.session.member.role)) return next();
    return res.status(403).send('접근 권한이 없습니다.');
  };
}

function requirePage(pageKey) {
  return (req, res, next) => {
    if (!req.session || !req.session.member) return res.redirect('/admin/login');
    if (req.session.mustSetupOtp && pageKey !== 'account') {
      return res.redirect('/admin/account?forceOtp=1');
    }
    const m = req.session.member;
    if (m.role === ROLES.SUPER_ADMIN || m.role === ROLES.ADMIN) return next();
    if (m.role === ROLES.OPERATOR && m.permissions && m.permissions.includes(pageKey)) return next();
    return res.status(403).send('해당 페이지 접근 권한이 없습니다.');
  };
}

function requirePageAny(keys) {
  return (req, res, next) => {
    if (!req.session || !req.session.member) return res.redirect('/admin/login');
    if (req.session.mustSetupOtp) return res.redirect('/admin/account?forceOtp=1');
    const m = req.session.member;
    if (m.role === ROLES.SUPER_ADMIN || m.role === ROLES.ADMIN) return next();
    if (m.role === ROLES.OPERATOR && m.permissions && keys.some((k) => m.permissions && m.permissions.includes(k))) return next();
    return res.status(403).send('해당 페이지 접근 권한이 없습니다.');
  };
}

// ----- 공통 JSON 설정 로더 -----
function loadJsonConfig(filePath, fallback) {
  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(raw);
  } catch (e) {
    return fallback;
  }
}

function saveJsonConfig(filePath, value) {
  ensureConfigDir();
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2));
}

// ========== 전산 노티 대상 설정 (internal targets) ==========
// id -> { id, name, callbackUrl, resultUrl }
function loadInternalTargets() {
  const loaded = loadJsonConfig(INTERNAL_TARGETS_CONFIG_PATH, null);
  if (loaded && typeof loaded === 'object') {
    const map = new Map();
    Object.entries(loaded).forEach(([id, v]) => map.set(id, v));
    return map;
  }
  // 초기 기본 4개 설정
  const initial = new Map([
    [
      'ONTL_HQ_JPY',
      {
        id: 'ONTL_HQ_JPY',
        name: 'Ontheline HQ JPY',
        callbackUrl: 'https://api.soonpay.co.kr/noti/CHILL/FXHJ',
        resultUrl: 'https://api.soonpay.co.kr/noti/CHILL/FXHJ',
      },
    ],
    [
      'ONTL_HQ_USD',
      {
        id: 'ONTL_HQ_USD',
        name: 'Ontheline HQ USD',
        callbackUrl: 'https://api.soonpay.co.kr/noti/CHILL/FXHU',
        resultUrl: 'https://api.soonpay.co.kr/noti/CHILL/FXHU',
      },
    ],
    [
      'ONTL_JP_JPY',
      {
        id: 'ONTL_JP_JPY',
        name: 'Ontheline JP JPY',
        callbackUrl: 'https://api.soonpay.co.kr/noti/CHILL/JHJ',
        resultUrl: 'https://api.soonpay.co.kr/noti/CHILL/JHJ',
      },
    ],
    [
      'ONTL_JP_USD',
      {
        id: 'ONTL_JP_USD',
        name: 'Ontheline JP USD',
        callbackUrl: 'https://api.soonpay.co.kr/noti/CHILL/JHU',
        resultUrl: 'https://api.soonpay.co.kr/noti/CHILL/JHU',
      },
    ],
  ]);

  const obj = {};
  initial.forEach((v, id) => {
    obj[id] = v;
  });
  saveJsonConfig(INTERNAL_TARGETS_CONFIG_PATH, obj);
  return initial;
}

const INTERNAL_TARGETS = loadInternalTargets();

function saveInternalTargets() {
  const obj = {};
  INTERNAL_TARGETS.forEach((v, id) => {
    obj[id] = v;
  });
  saveJsonConfig(INTERNAL_TARGETS_CONFIG_PATH, obj);
}

function findInternalTargetUrl(internalTargetId, kind) {
  if (!internalTargetId) return null;
  const t = INTERNAL_TARGETS.get(internalTargetId);
  if (!t) return null;
  if (kind === 'result') {
    return t.resultUrl || t.callbackUrl;
  }
  return t.callbackUrl;
}
function getInternalTargetName(internalTargetId) {
  if (!internalTargetId) return '-';
  const t = INTERNAL_TARGETS.get(internalTargetId);
  return (t && t.name) ? t.name : (internalTargetId || '-');
}

// ========== 전산 노티 설정 (통화별: 금액 가공, RouteNo, CustomerId, 오리지널) ==========
const CURRENCY_CODES = ['392', '840', '410', '764'];
const DEFAULT_AMOUNT_RULES = { '392': '/100', '840': '/100', '410': '=', '764': '=' };
const DEFAULT_ROUTE_NO_MODE = { '392': 'current', '840': 'current', '410': 'current', '764': 'current' };
const DEFAULT_CUSTOMER_ID_MODE = { '392': 'current', '840': 'current', '410': 'current', '764': 'current' };
const DEFAULT_ORIGINAL = { '392': false, '840': false, '410': false, '764': false };

function loadInternalNotiSettingsFull() {
  const loaded = loadJsonConfig(INTERNAL_NOTI_SETTINGS_PATH, null);
  const amountRules = loaded && loaded.amountRules && typeof loaded.amountRules === 'object'
    ? { ...DEFAULT_AMOUNT_RULES, ...loaded.amountRules }
    : DEFAULT_AMOUNT_RULES;
  const routeNoMode = loaded && loaded.routeNoMode && typeof loaded.routeNoMode === 'object'
    ? { ...DEFAULT_ROUTE_NO_MODE, ...loaded.routeNoMode }
    : DEFAULT_ROUTE_NO_MODE;
  const customerIdMode = loaded && loaded.customerIdMode && typeof loaded.customerIdMode === 'object'
    ? { ...DEFAULT_CUSTOMER_ID_MODE, ...loaded.customerIdMode }
    : DEFAULT_CUSTOMER_ID_MODE;
  const original = loaded && loaded.original && typeof loaded.original === 'object'
    ? { ...DEFAULT_ORIGINAL, ...loaded.original }
    : DEFAULT_ORIGINAL;
  return { amountRules, routeNoMode, customerIdMode, original };
}

function loadInternalNotiSettings() {
  const full = loadInternalNotiSettingsFull();
  return full.amountRules;
}

let INTERNAL_AMOUNT_RULES = loadInternalNotiSettings();

function saveInternalNotiSettings(fullOrAmountRules) {
  ensureConfigDir();
  const isFull = fullOrAmountRules && typeof fullOrAmountRules === 'object' && fullOrAmountRules.amountRules !== undefined;
  const full = isFull ? fullOrAmountRules : { amountRules: fullOrAmountRules || INTERNAL_AMOUNT_RULES };
  const current = loadInternalNotiSettingsFull();
  const toSave = {
    amountRules: full.amountRules || current.amountRules,
    routeNoMode: full.routeNoMode || current.routeNoMode,
    customerIdMode: full.customerIdMode || current.customerIdMode,
    original: full.original !== undefined ? full.original : current.original,
  };
  saveJsonConfig(INTERNAL_NOTI_SETTINGS_PATH, toSave);
  INTERNAL_AMOUNT_RULES = toSave.amountRules;
}

/** 통화별 금액 가공: X100(곱하기), /100(나누기), =(그대로) */
function applyAmountRule(amountNum, currencyCode) {
  const rule = (INTERNAL_AMOUNT_RULES && INTERNAL_AMOUNT_RULES[String(currencyCode)]) || '=';
  const num = Number(amountNum) || 0;
  if (rule === 'X100') return num * 100;
  if (rule === '/100') return num / 100;
  return num;
}

// ---------- 개발 노티 설정 (전산 노티 설정과 별도 관리) ----------
function loadDevInternalNotiSettingsFull() {
  const loaded = loadJsonConfig(DEV_INTERNAL_NOTI_SETTINGS_PATH, null);
  const amountRules =
    loaded && loaded.amountRules && typeof loaded.amountRules === 'object'
      ? { ...DEFAULT_AMOUNT_RULES, ...loaded.amountRules }
      : DEFAULT_AMOUNT_RULES;
  const routeNoMode =
    loaded && loaded.routeNoMode && typeof loaded.routeNoMode === 'object'
      ? { ...DEFAULT_ROUTE_NO_MODE, ...loaded.routeNoMode }
      : DEFAULT_ROUTE_NO_MODE;
  const customerIdMode =
    loaded && loaded.customerIdMode && typeof loaded.customerIdMode === 'object'
      ? { ...DEFAULT_CUSTOMER_ID_MODE, ...loaded.customerIdMode }
      : DEFAULT_CUSTOMER_ID_MODE;
  const original =
    loaded && loaded.original && typeof loaded.original === 'object'
      ? { ...DEFAULT_ORIGINAL, ...loaded.original }
      : DEFAULT_ORIGINAL;
  return { amountRules, routeNoMode, customerIdMode, original };
}

function loadDevInternalNotiSettings() {
  const full = loadDevInternalNotiSettingsFull();
  return full.amountRules;
}

let DEV_INTERNAL_AMOUNT_RULES = loadDevInternalNotiSettings();

function saveDevInternalNotiSettings(fullOrAmountRules) {
  ensureConfigDir();
  const isFull =
    fullOrAmountRules && typeof fullOrAmountRules === 'object' && fullOrAmountRules.amountRules !== undefined;
  const full = isFull ? fullOrAmountRules : { amountRules: fullOrAmountRules || DEV_INTERNAL_AMOUNT_RULES };
  const current = loadDevInternalNotiSettingsFull();
  const toSave = {
    amountRules: full.amountRules || current.amountRules,
    routeNoMode: full.routeNoMode || current.routeNoMode,
    customerIdMode: full.customerIdMode || current.customerIdMode,
    original: full.original !== undefined ? full.original : current.original,
  };
  saveJsonConfig(DEV_INTERNAL_NOTI_SETTINGS_PATH, toSave);
  DEV_INTERNAL_AMOUNT_RULES = toSave.amountRules;
}

function applyDevAmountRule(amountNum, currencyCode) {
  const rule = (DEV_INTERNAL_AMOUNT_RULES && DEV_INTERNAL_AMOUNT_RULES[String(currencyCode)]) || '=';
  const num = Number(amountNum) || 0;
  if (rule === 'X100') return num * 100;
  if (rule === '/100') return num / 100;
  return num;
}

// ========== 테스트 결제 환경 설정 (test payment configs) ==========
// id -> { id, name, environment, merchantCode, routeNo, apiKey, md5Key, currency, paymentApiUrl, returnUrl, useTestResultPage }
function loadTestConfigs() {
  const loaded = loadJsonConfig(TEST_CONFIGS_CONFIG_PATH, null);
  if (loaded && typeof loaded === 'object') {
    const map = new Map();
    Object.entries(loaded).forEach(([id, v]) => map.set(id, v));
    return map;
  }
  // 초기값 없음
  return new Map();
}

const TEST_CONFIGS = loadTestConfigs();

function saveTestConfigs() {
  const obj = {};
  TEST_CONFIGS.forEach((v, id) => {
    obj[id] = v;
  });
  saveJsonConfig(TEST_CONFIGS_CONFIG_PATH, obj);
}

// ========== 가맹점 라우팅 설정 (merchantId -> { routeCallbackKey, routeResultKey, callbackUrl, resultUrl, routeNo, internalCustomerId, internalTargetId, enableRelay, enableInternal, enableDevInternal }) ==========
// routeCallbackKey / routeResultKey 는 PG에 등록할 우리 쪽 노티 URL의 마지막 경로입니다.
// 예) PG callback URL: https://api.our-system.com/noti/rount_c1  → routeCallbackKey = 'rount_c1'
function loadMerchants() {
  const loaded = loadJsonConfig(MERCHANTS_CONFIG_PATH, null);
  if (loaded && typeof loaded === 'object') {
    const map = new Map();
    Object.entries(loaded).forEach(([id, v]) => map.set(id, v));
    return map;
  }
  const initial = new Map([
    [
      'merchant_test',
      {
        merchantId: 'merchant_test',
        routeCallbackKey: 'rount_c1',
        routeResultKey: 'rount_r1',
        callbackUrl: 'https://webhook.site/test-callback',
        resultUrl: 'https://webhook.site/test-result',
        routeNo: '7',
        internalCustomerId: 'M035594',
        internalTargetId: 'ONTL_HQ_JPY',
        enableRelay: true,
        enableInternal: true,
        enableDevInternal: false,
        relayFormat: 'raw',
      },
    ],
  ]);
  const obj = {};
  initial.forEach((v, id) => {
    obj[id] = v;
  });
  saveJsonConfig(MERCHANTS_CONFIG_PATH, obj);
  return initial;
}

let MERCHANTS = loadMerchants();

function saveMerchants() {
  const obj = {};
  MERCHANTS.forEach((v, id) => {
    obj[id] = v;
  });
  saveJsonConfig(MERCHANTS_CONFIG_PATH, obj);
}

// 트래픽 수집 (관리자 페이지 접근, 최근 10000건 - 메모리 전용)
const TRAFFIC_HITS = [];
const TRAFFIC_HITS_MAX = 10000;

// ===== 노티/테스트 로그 파일 경로 =====
const DATA_DIR = path.join(__dirname, 'data');
const PG_NOTI_LOG_PATH = path.join(DATA_DIR, 'pg-noti.log');
const CHILLPAY_VOID_NOTI_SENT_PATH = path.join(DATA_DIR, 'chillpay-void-noti-sent.json');
const CHILLPAY_REFUND_NOTI_SENT_PATH = path.join(DATA_DIR, 'chillpay-refund-noti-sent.json');
const CHILLPAY_PG_TRANSACTIONS_PATH = path.join(DATA_DIR, 'chillpay-pg-transactions.json');
const CHILLPAY_PG_TRANSACTIONS_BACKUPS_PATH = path.join(DATA_DIR, 'chillpay-pg-transactions-backups.json');
const PG_TRANSACTIONS_RESET_BACKUPS_MAX = 2;
const INTERNAL_LOG_PATH = path.join(DATA_DIR, 'internal-noti.log');
const DEV_INTERNAL_LOG_PATH = path.join(DATA_DIR, 'dev-internal-noti.log');
const TEST_LOG_PATH = path.join(DATA_DIR, 'test-payments.log');
const CONFIG_LOG_PATH = path.join(DATA_DIR, 'config-change.log');
const VOID_REFUND_NOTI_LOG_PATH = path.join(DATA_DIR, 'void-refund-noti.log');
const VOID_UI_DELETED_PATH = path.join(DATA_DIR, 'void-ui-deleted.log');
const VOID_UI_DELETED_RETENTION_DAYS = 31;

// ChillPay PG 거래 동기화용 최근 API 오류 (페이지 상단 안내용)
let PG_TRANSACTIONS_LAST_ERROR_PROD = null;
let PG_TRANSACTIONS_LAST_ERROR_SANDBOX = null;
function setPgTransactionsLastError(useSandbox, msg) {
  if (useSandbox) {
    PG_TRANSACTIONS_LAST_ERROR_SANDBOX = msg || null;
  } else {
    PG_TRANSACTIONS_LAST_ERROR_PROD = msg || null;
  }
}

// 무효/강제무효 목록에서 "목록에서 제거"한 건 (1달 보관 후 자동 삭제, 복원 가능). 삭제 시 삭제자·시각 기록.
function loadVoidUiDeletedList() {
  const cutoff = Date.now() - VOID_UI_DELETED_RETENTION_DAYS * 24 * 60 * 60 * 1000;
  try {
    if (!fs.existsSync(VOID_UI_DELETED_PATH)) return [];
    const raw = fs.readFileSync(VOID_UI_DELETED_PATH, 'utf8');
    const lines = raw.split('\n').filter((l) => l.trim().length > 0);
    const entries = [];
    const toKeep = [];
    for (const line of lines) {
      try {
        const obj = JSON.parse(line);
        const t = Date.parse(obj.deletedAtIso || obj.deletedAt || '');
        if (Number.isNaN(t)) continue;
        if (t < cutoff) continue; // 1달 지난 건 제외 (자동 삭제)
        entries.push(obj);
        toKeep.push(line);
      } catch (_) {}
    }
    if (lines.length !== toKeep.length) {
      try { fs.writeFileSync(VOID_UI_DELETED_PATH, toKeep.join('\n') + (toKeep.length ? '\n' : ''), 'utf8'); } catch (_) {}
    }
    return entries.sort((a, b) => (Date.parse(b.deletedAtIso || '') || 0) - (Date.parse(a.deletedAtIso || '') || 0));
  } catch {
    return [];
  }
}
function isVoidUiDeleted(transactionId, merchantId, env, deletedList) {
  const tid = String(transactionId || '').trim();
  const mid = String(merchantId || '').trim();
  const e = (env || 'live').toString().toLowerCase();
  return deletedList.some((d) => String(d.transactionId || '').trim() === tid && String(d.merchantId || '').trim() === mid && (String(d.env || 'live').toLowerCase() === e));
}
function appendVoidUiDeleted(entry) {
  const id = (entry.id || 'vd_' + Date.now() + '_' + (Math.random().toString(36).slice(2, 10))).toString();
  const row = { id, deletedAtIso: new Date().toISOString(), ...entry };
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.appendFile(VOID_UI_DELETED_PATH, JSON.stringify(row) + '\n', () => {});
  } catch (_) {}
  return id;
}
function removeVoidUiDeletedById(id) {
  try {
    if (!fs.existsSync(VOID_UI_DELETED_PATH)) return false;
    const raw = fs.readFileSync(VOID_UI_DELETED_PATH, 'utf8');
    const lines = raw.split('\n').filter((l) => l.trim().length > 0);
    const kept = [];
    for (const line of lines) {
      try {
        const obj = JSON.parse(line);
        if (String(obj.id || '') === String(id)) continue;
        kept.push(line);
      } catch (_) { kept.push(line); }
    }
    if (kept.length === lines.length) return false;
    fs.writeFileSync(VOID_UI_DELETED_PATH, kept.join('\n') + (kept.length ? '\n' : ''), 'utf8');
    return true;
  } catch {
    return false;
  }
}

// ===== 공통: JSON Lines 로그 로더 (최근 7일 메모리 유지, 30일 초과 파일 정리) =====
function loadJsonLogFile(filePath, isoKey) {
  try {
    if (!fs.existsSync(filePath)) return [];
    const raw = fs.readFileSync(filePath, 'utf8');
    if (!raw.trim()) return [];
    const lines = raw.split('\n').filter((l) => l.trim().length > 0);
    const now = Date.now();
    const memCutoff = now - 7 * 24 * 60 * 60 * 1000; // 최근 7일
    const diskCutoff = now - 30 * 24 * 60 * 60 * 1000; // 최근 30일
    const keptLines = [];
    const memEntries = [];
    for (const line of lines) {
      let obj;
      try {
        obj = JSON.parse(line);
      } catch {
        // 손상된 라인은 그대로 보관만 (삭제하지 않음)
        keptLines.push(line);
        continue;
      }
      const iso =
        (isoKey && obj[isoKey]) ||
        obj.receivedAtIso ||
        obj.storedAtIso ||
        obj.loggedAtIso ||
        obj.at ||
        null;
      let t = Number.NaN;
      if (iso) {
        t = Date.parse(iso);
      }
      if (!Number.isNaN(t)) {
        if (t >= memCutoff) memEntries.push(obj);
        if (t >= diskCutoff) {
          keptLines.push(JSON.stringify(obj));
        }
      } else {
        // 시간 정보가 없으면 파일에는 남기되, 메모리에는 싣지 않음
        keptLines.push(JSON.stringify(obj));
      }
    }
    // 30일 이내 기록만 다시 파일에 저장
    try {
      if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
      fs.writeFileSync(filePath, keptLines.join('\n') + (keptLines.length ? '\n' : ''));
    } catch {
      // 파일 정리는 실패해도 서비스에는 영향 없음
    }
    return memEntries;
  } catch {
    return [];
  }
}

// ===== PG 노티 로그 (최근 7일 메모리 + 파일에도 기록) =====
let NOTI_LOGS = loadJsonLogFile(PG_NOTI_LOG_PATH, 'receivedAtIso');

function appendPgNotiLog(entry) {
  const nowIso = new Date().toISOString();
  const log = {
    receivedAt: entry.receivedAt || getThailandNowString(),
    receivedAtIso: entry.receivedAtIso || nowIso,
    ...entry,
  };
  NOTI_LOGS.push(log);
  // 메모리에서는 최근 7일만 유지
  const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;
  NOTI_LOGS = NOTI_LOGS.filter((e) => {
    const t = Date.parse(e.receivedAtIso || e.receivedAt);
    return !Number.isNaN(t) && t >= cutoff;
  });
  // 파일에도 기록
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.appendFile(PG_NOTI_LOG_PATH, JSON.stringify(log) + '\n', () => {});
  } catch {
    // 로그 기록 실패는 서비스에 영향 없음
  }
}

// ===== 전산 노티 로그 (최근 7일 메모리 + 파일에도 기록) =====
let INTERNAL_LOGS = loadJsonLogFile(INTERNAL_LOG_PATH, 'storedAtIso');

function appendInternalLog(entry) {
  const nowIso = new Date().toISOString();
  const log = {
    ...entry,
    storedAt: getThailandNowString(),
    storedAtIso: nowIso,
  };
  INTERNAL_LOGS.push(log);
  const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;
  INTERNAL_LOGS = INTERNAL_LOGS.filter((e) => {
    const t = Date.parse(e.storedAtIso || e.storedAt);
    return !Number.isNaN(t) && t >= cutoff;
  });
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.appendFile(INTERNAL_LOG_PATH, JSON.stringify(log) + '\n', () => {});
  } catch {
    // 무시
  }
}

// ===== 개발 노티 로그 (최근 7일 메모리 + 파일에도 기록) =====
let DEV_INTERNAL_LOGS = loadJsonLogFile(DEV_INTERNAL_LOG_PATH, 'storedAtIso');

function appendDevInternalLog(entry) {
  const nowIso = new Date().toISOString();
  const log = {
    ...entry,
    storedAt: getThailandNowString(),
    storedAtIso: nowIso,
  };
  DEV_INTERNAL_LOGS.push(log);
  const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;
  DEV_INTERNAL_LOGS = DEV_INTERNAL_LOGS.filter((e) => {
    const t = Date.parse(e.storedAtIso || e.storedAt);
    return !Number.isNaN(t) && t >= cutoff;
  });
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.appendFile(DEV_INTERNAL_LOG_PATH, JSON.stringify(log) + '\n', () => {});
  } catch {
    // 무시
  }
}

// ===== 설정 변경 로그 (관리자 설정, 가맹점, 전산 대상 등) =====
function appendConfigChangeLog(entry) {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    const log = {
      at: new Date().toISOString(),
      ...entry,
    };
    fs.appendFile(CONFIG_LOG_PATH, JSON.stringify(log) + '\n', () => {});
  } catch {
    // 무시
  }
}

// ===== 테스트 결제 로그 (최근 7일 메모리 + 파일에도 기록) =====
let TEST_LOGS = loadJsonLogFile(TEST_LOG_PATH, 'loggedAtIso');

function appendTestLog(entry) {
  const nowIso = new Date().toISOString();
  const log = {
    ...entry,
    loggedAt: getThailandNowString(),
    loggedAtIso: nowIso,
  };
  TEST_LOGS.push(log);
  const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;
  TEST_LOGS = TEST_LOGS.filter((e) => {
    const t = Date.parse(e.loggedAtIso || e.loggedAt);
    return !Number.isNaN(t) && t >= cutoff;
  });
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.appendFile(TEST_LOG_PATH, JSON.stringify(log) + '\n', () => {});
  } catch {
    // 무시
  }
}

function normalizeCurrencyCode(value) {
  if (!value) return '';
  const v = String(value).toUpperCase();
  if (v === 'JPY' || v === '392') return '392'; // JPY
  if (v === 'USD' || v === '840') return '840'; // USD
  if (v === 'THB' || v === '764') return '764'; // THB
  if (v === 'KRW' || v === 'KOR' || v === '410') return '410'; // KRW(KOR)
  return value;
}

// 태국 시간(Asia/Bangkok) 문자열 생성 (노티 로그용)
function getThailandNowString() {
  return new Date().toLocaleString('th-TH', {
    timeZone: 'Asia/Bangkok',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
}

// 4개 시간대(태국/일본/싱가포르/미국) 포맷 (로그 표시용)
function formatTimeMultiTZ(isoString) {
  if (!isoString) return '-';
  const d = new Date(isoString);
  if (Number.isNaN(d.getTime())) return '-';
  const opts = {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  };
  const th = d.toLocaleString('en-CA', { ...opts, timeZone: 'Asia/Bangkok' }).replace(',', '');
  const jp = d.toLocaleString('en-CA', { ...opts, timeZone: 'Asia/Tokyo' }).replace(',', '');
  const sg = d.toLocaleString('en-CA', { ...opts, timeZone: 'Asia/Singapore' }).replace(',', '');
  const us = d.toLocaleString('en-CA', { ...opts, timeZone: 'America/New_York' }).replace(',', '');
  return { th, jp, sg, us };
}

// 결과 요약용: 날짜·시각 분리, 태국·일본만 (수신일, 수신시각 TH/JP)
function formatDateAndTimeTHJP(isoString) {
  if (!isoString) return { date: '-', timeTh: '-', timeJp: '-' };
  const d = new Date(isoString);
  if (Number.isNaN(d.getTime())) return { date: '-', timeTh: '-', timeJp: '-' };
  const dateOpts = { year: 'numeric', month: '2-digit', day: '2-digit', timeZone: 'Asia/Bangkok' };
  const timeOpts = { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false };
  const date = d.toLocaleDateString('en-CA', dateOpts);
  const timeTh = d.toLocaleTimeString('en-CA', { ...timeOpts, timeZone: 'Asia/Bangkok' });
  const timeJp = d.toLocaleTimeString('en-CA', { ...timeOpts, timeZone: 'Asia/Tokyo' });
  return { date, timeTh, timeJp };
}

// DirectCredit용 CheckSum 생성 (매뉴얼 Table 1.3 + 제공 스크립트와 동일한 로직)
function generateDirectCreditCheckSum(payload, secretKey) {
  const parseVal = (val) => (val === null || val === undefined ? '' : String(val));

  const concatData =
    parseVal(payload.OrderNo) +
    parseVal(payload.CustomerId) +
    parseVal(payload.Amount) +
    parseVal(payload.PhoneNumber) +
    parseVal(payload.Description) +
    parseVal(payload.ChannelCode) +
    parseVal(payload.Currency) +
    parseVal(payload.LangCode) +
    parseVal(payload.RouteNo) +
    parseVal(payload.IPAddress) +
    parseVal(payload.TokenType) +
    parseVal(payload.CreditToken) +
    parseVal(payload.DirectCreditToken) +
    parseVal(payload.CreditMonth) +
    parseVal(payload.ShopID) +
    parseVal(payload.CustEmail) +
    parseVal(payload.SaveCard);

  const finalString = concatData + (secretKey ? secretKey.trim() : '');
  return crypto.createHash('md5').update(finalString, 'utf8').digest('hex');
}

// 전산(온더라인) 노티 수신 URL (환경변수로 설정, 없으면 전송 스킵)
const INTERNAL_NOTI_URL = process.env.INTERNAL_NOTI_URL || '';

// ========== ChillPay Transaction API (Request Void / Request Refund) ==========
const CHILLPAY_TRANSACTION_SANDBOX_BASE = 'https://sandbox-api-transaction.chillpay.co';
const CHILLPAY_TRANSACTION_PROD_BASE = 'https://api-transaction.chillpay.co';

function chillPayTransactionChecksum(concatStr, md5Key) {
  const s = (concatStr || '') + (md5Key ? String(md5Key).trim() : '');
  return crypto.createHash('md5').update(s, 'utf8').digest('hex').toLowerCase();
}

async function chillPayRequestVoid(transactionId, useSandbox) {
  const cfg = loadChillPayTransactionConfig();
  const cred = useSandbox ? cfg.sandbox : cfg.production;
  const base = useSandbox ? CHILLPAY_TRANSACTION_SANDBOX_BASE : CHILLPAY_TRANSACTION_PROD_BASE;
  if (!cred.mid || !cred.apiKey || !cred.md5) {
    return { success: false, error: 'ChillPay Transaction API 미설정 (mid/apiKey/md5)' };
  }
  const tid = String(transactionId).trim();
  if (!tid) return { success: false, error: 'TransactionId 없음' };
  const checksum = chillPayTransactionChecksum(tid, cred.md5);
  try {
    const res = await axios.post(
      base + '/api/v1/void/request',
      { TransactionId: Number(transactionId) || transactionId, Checksum: checksum },
      {
        headers: {
          'Content-Type': 'application/json',
          'CHILLPAY-MerchantCode': cred.mid,
          'CHILLPAY-ApiKey': cred.apiKey,
        },
        timeout: 15000,
      },
    );
    const data = res.data;
    if (data && (data.status === 200 || data.status === '200')) return { success: true, data: data.data || data };
    return { success: false, error: (data && data.message) || res.statusText || 'Void 요청 실패' };
  } catch (err) {
    const msg = err.response && err.response.data ? (err.response.data.message || JSON.stringify(err.response.data)) : err.message;
    return { success: false, error: msg };
  }
}

async function chillPayRequestRefund(transactionId, useSandbox) {
  const cfg = loadChillPayTransactionConfig();
  const cred = useSandbox ? cfg.sandbox : cfg.production;
  const base = useSandbox ? CHILLPAY_TRANSACTION_SANDBOX_BASE : CHILLPAY_TRANSACTION_PROD_BASE;
  if (!cred.mid || !cred.apiKey || !cred.md5) {
    return { success: false, error: 'ChillPay Transaction API 미설정 (mid/apiKey/md5)' };
  }
  const tid = String(transactionId).trim();
  if (!tid) return { success: false, error: 'TransactionId 없음' };
  const checksum = chillPayTransactionChecksum(tid, cred.md5);
  try {
    const res = await axios.post(
      base + '/api/v1/refund/request',
      { TransactionId: Number(transactionId) || transactionId, Checksum: checksum },
      {
        headers: {
          'Content-Type': 'application/json',
          'CHILLPAY-MerchantCode': cred.mid,
          'CHILLPAY-ApiKey': cred.apiKey,
        },
        timeout: 15000,
      },
    );
    const data = res.data;
    if (data && (data.status === 200 || data.status === '200')) return { success: true, data: data.data || data };
    return { success: false, error: (data && data.message) || res.statusText || 'Refund 요청 실패' };
  } catch (err) {
    const msg = err.response && err.response.data ? (err.response.data.message || JSON.stringify(err.response.data)) : err.message;
    return { success: false, error: msg };
  }
}

// ChillPay Search Void/Refund/Payment API용 날짜 형식: dd/MM/yyyy HH:mm:ss
// 서버별 타임존/Intl 지원 차이로 인한 오류를 피하기 위해, 단순 Date 객체 기준으로만 포맷팅한다.
// 가능한 한 보수적인 ES5 문법만 사용한다.
var formatChillPayTransactionDate = function (date, timePart) {
  var d = (date instanceof Date) ? date : new Date(date);
  if (isNaN(d.getTime())) return '';
  var day = ('0' + d.getDate()).slice(-2);
  var month = ('0' + (d.getMonth() + 1)).slice(-2);
  var year = d.getFullYear();
  return day + '/' + month + '/' + year + ' ' + (timePart || '00:00:00');
};

// Search Void Transaction: ChillPay에서 이미 무효 처리된 건 목록 조회 (수동 무효 포함)
// 문서: OrderBy + OrderDir + PageSize + PageNumber + SearchKeyword + MerchantCode + OrderNo + Status + TransactionDateFrom + TransactionDateTo + MD5 → Checksum
async function chillPaySearchVoid(useSandbox, params) {
  const cfg = loadChillPayTransactionConfig();
  const cred = useSandbox ? cfg.sandbox : cfg.production;
  const base = useSandbox ? CHILLPAY_TRANSACTION_SANDBOX_BASE : CHILLPAY_TRANSACTION_PROD_BASE;
  if (!cred.mid || !cred.apiKey || !cred.md5) {
    return { success: false, error: 'ChillPay Transaction API 미설정 (mid/apiKey/md5)', data: [] };
  }
  const orderBy = String((params.orderBy != null ? params.orderBy : '')).trim();
  const orderDir = String((params.orderDir != null ? params.orderDir : '')).trim();
  const pageSize = String((params.pageSize != null ? params.pageSize : '100')).trim();
  const pageNumber = String((params.pageNumber != null ? params.pageNumber : '1')).trim();
  const searchKeyword = String((params.searchKeyword != null ? params.searchKeyword : '')).trim();
  const merchantCode = String(cred.mid).trim();
  const orderNo = String((params.orderNo != null ? params.orderNo : '')).trim();
  const status = String((params.status != null ? params.status : '')).trim();
  const transactionDateFrom = String((params.transactionDateFrom != null ? params.transactionDateFrom : '')).trim();
  const transactionDateTo = String((params.transactionDateTo != null ? params.transactionDateTo : '')).trim();
  const concatStr = orderBy + orderDir + pageSize + pageNumber + searchKeyword + merchantCode + orderNo + status + transactionDateFrom + transactionDateTo;
  const checksum = chillPayTransactionChecksum(concatStr, cred.md5);
  try {
    const res = await axios.post(
      base + '/api/v1/void/search',
      {
        OrderBy: orderBy || undefined,
        OrderDir: orderDir || undefined,
        PageSize: pageSize ? parseInt(pageSize, 10) || 100 : 100,
        PageNumber: pageNumber ? parseInt(pageNumber, 10) || 1 : 1,
        SearchKeyword: searchKeyword || undefined,
        MerchantCode: merchantCode,
        OrderNo: orderNo || undefined,
        Status: status || undefined,
        TransactionDateFrom: transactionDateFrom || undefined,
        TransactionDateTo: transactionDateTo || undefined,
        Checksum: checksum,
      },
      {
        headers: {
          'Content-Type': 'application/json',
          'CHILLPAY-MerchantCode': cred.mid,
          'CHILLPAY-ApiKey': cred.apiKey,
        },
        timeout: 20000,
      },
    );
    const data = res.data;
    if (data && (data.status === 200 || data.status === '200')) {
      const list = (data.data && Array.isArray(data.data)) ? data.data : [];
      return { success: true, data: list };
    }
    return { success: false, error: (data && data.message) || res.statusText || 'Search Void 실패', data: [] };
  } catch (err) {
    const msg = err.response && err.response.data ? (err.response.data.message || JSON.stringify(err.response.data)) : err.message;
    return { success: false, error: msg, data: [] };
  }
}

// Search Refund Transaction: ChillPay에서 이미 환불 처리된 건 목록 조회 (수동 환불 포함). 체크섬 순서는 Search Void와 동일하게 가정.
async function chillPaySearchRefund(useSandbox, params) {
  const cfg = loadChillPayTransactionConfig();
  const cred = useSandbox ? cfg.sandbox : cfg.production;
  const base = useSandbox ? CHILLPAY_TRANSACTION_SANDBOX_BASE : CHILLPAY_TRANSACTION_PROD_BASE;
  if (!cred.mid || !cred.apiKey || !cred.md5) {
    return { success: false, error: 'ChillPay Transaction API 미설정 (mid/apiKey/md5)', data: [] };
  }
  const orderBy = String((params.orderBy != null ? params.orderBy : '')).trim();
  const orderDir = String((params.orderDir != null ? params.orderDir : '')).trim();
  const pageSize = String((params.pageSize != null ? params.pageSize : '100')).trim();
  const pageNumber = String((params.pageNumber != null ? params.pageNumber : '1')).trim();
  const searchKeyword = String((params.searchKeyword != null ? params.searchKeyword : '')).trim();
  const merchantCode = String(cred.mid).trim();
  const orderNo = String((params.orderNo != null ? params.orderNo : '')).trim();
  const status = String((params.status != null ? params.status : '')).trim();
  const transactionDateFrom = String((params.transactionDateFrom != null ? params.transactionDateFrom : '')).trim();
  const transactionDateTo = String((params.transactionDateTo != null ? params.transactionDateTo : '')).trim();
  const concatStr = orderBy + orderDir + pageSize + pageNumber + searchKeyword + merchantCode + orderNo + status + transactionDateFrom + transactionDateTo;
  const checksum = chillPayTransactionChecksum(concatStr, cred.md5);
  try {
    const res = await axios.post(
      base + '/api/v1/refund/search',
      {
        OrderBy: orderBy || undefined,
        OrderDir: orderDir || undefined,
        PageSize: pageSize ? parseInt(pageSize, 10) || 100 : 100,
        PageNumber: pageNumber ? parseInt(pageNumber, 10) || 1 : 1,
        SearchKeyword: searchKeyword || undefined,
        MerchantCode: merchantCode,
        OrderNo: orderNo || undefined,
        Status: status || undefined,
        TransactionDateFrom: transactionDateFrom || undefined,
        TransactionDateTo: transactionDateTo || undefined,
        Checksum: checksum,
      },
      {
        headers: {
          'Content-Type': 'application/json',
          'CHILLPAY-MerchantCode': cred.mid,
          'CHILLPAY-ApiKey': cred.apiKey,
        },
        timeout: 20000,
      },
    );
    const data = res.data;
    if (data && (data.status === 200 || data.status === '200')) {
      const list = (data.data && Array.isArray(data.data)) ? data.data : [];
      return { success: true, data: list };
    }
    return { success: false, error: (data && data.message) || res.statusText || 'Search Refund 실패', data: [] };
  } catch (err) {
    const msg = err.response && err.response.data ? (err.response.data.message || JSON.stringify(err.response.data)) : err.message;
    return { success: false, error: msg, data: [] };
  }
}

// Search Payment Transaction: ChillPay에서 결제 거래 목록 조회 (프로덕션/샌드박스 각각)
// 문서: OrderBy + OrderDir + PageSize + PageNumber + SearchKeyword + MerchantCode + PaymentChannel + RouteNo + OrderNo + Status + TransactionDateFrom + TransactionDateTo + PaymentDateFrom + PaymentDateTo + MD5 → Checksum
async function chillPaySearchPayment(useSandbox, params) {
  const cfg = loadChillPayTransactionConfig();
  const cred = useSandbox ? cfg.sandbox : cfg.production;
  const base = useSandbox ? CHILLPAY_TRANSACTION_SANDBOX_BASE : CHILLPAY_TRANSACTION_PROD_BASE;
  if (!cred.mid || !cred.apiKey || !cred.md5) {
    console.error('[PG transactions] chillPaySearchPayment 미설정 env=' + (useSandbox ? 'sandbox' : 'production'));
    setPgTransactionsLastError(useSandbox, 'Transaction API 자격증명(Mid/ApiKey/MD5) 미설정');
    return { success: false, error: 'ChillPay Transaction API 미설정 (mid/apiKey/md5)', data: [], totalRecord: 0, pageSize: 0, pageNumber: 0, filteredRecord: 0 };
  }
  const orderBy = String((params.orderBy != null ? params.orderBy : 'TransactionId')).trim();
  const orderDir = String((params.orderDir != null ? params.orderDir : 'DESC')).trim();
  const pageSize = String((params.pageSize != null ? params.pageSize : '50')).trim();
  const pageNumber = String((params.pageNumber != null ? params.pageNumber : '1')).trim();
  const searchKeyword = String((params.searchKeyword != null ? params.searchKeyword : '')).trim();
  const merchantCode = String((params.merchantCode != null ? params.merchantCode : cred.mid)).trim() || String(cred.mid).trim();
  const paymentChannel = String((params.paymentChannel != null ? params.paymentChannel : '')).trim();
  const routeNo = params.routeNo != null && params.routeNo !== '' ? String(params.routeNo).trim() : '';
  const orderNo = String((params.orderNo != null ? params.orderNo : '')).trim();
  const status = String((params.status != null ? params.status : '')).trim();
  const transactionDateFrom = String((params.transactionDateFrom != null ? params.transactionDateFrom : '')).trim();
  const transactionDateTo = String((params.transactionDateTo != null ? params.transactionDateTo : '')).trim();
  const paymentDateFrom = String((params.paymentDateFrom != null ? params.paymentDateFrom : '')).trim();
  const paymentDateTo = String((params.paymentDateTo != null ? params.paymentDateTo : '')).trim();
  // 문서: OrderBy+OrderDir+PageSize+PageNumber+SearchKeyword+MerchantCode+PaymentChannel+RouteNo+OrderNo+Status+TransactionDateFrom+TransactionDateTo+PaymentDateFrom+PaymentDateTo+MD5
  const concatStr = orderBy + orderDir + pageSize + pageNumber + searchKeyword + merchantCode + paymentChannel + routeNo + orderNo + status + transactionDateFrom + transactionDateTo + paymentDateFrom + paymentDateTo;
  const checksum = chillPayTransactionChecksum(concatStr, cred.md5);
  const body = {
    OrderBy: orderBy || null,
    OrderDir: orderDir || null,
    PageSize: pageSize ? parseInt(pageSize, 10) || 50 : 50,
    PageNumber: pageNumber ? parseInt(pageNumber, 10) || 1 : 1,
    SearchKeyword: searchKeyword || null,
    MerchantCode: merchantCode || null,
    PaymentChannel: paymentChannel || null,
    RouteNo: routeNo ? parseInt(routeNo, 10) : null,
    OrderNo: orderNo || null,
    Status: status || null,
    TransactionDateFrom: transactionDateFrom || null,
    TransactionDateTo: transactionDateTo || null,
    PaymentDateFrom: paymentDateFrom || null,
    PaymentDateTo: paymentDateTo || null,
    Checksum: checksum,
  };
  try {
    console.log('[PG transactions][DEBUG] chillPaySearchPayment request env=' + (useSandbox ? 'sandbox' : 'production') + ' body=' + JSON.stringify(body));
    const res = await axios.post(
      base + '/api/v1/payment/search',
      body,
      {
        headers: {
          'Content-Type': 'application/json',
          'CHILLPAY-MerchantCode': cred.mid,
          'CHILLPAY-ApiKey': cred.apiKey,
        },
        timeout: 20000,
      },
    );
    const data = res.data;
    const respCode = data && (data.Code != null ? data.Code : data.code);
    const isErrorCode = respCode != null && (respCode >= 1000 || respCode === '1004' || respCode === '2014' || respCode === '3001');
    if (data && (data.status === 200 || data.status === '200') && !isErrorCode) {
      const rawList = (data.Data && Array.isArray(data.Data)) ? data.Data : (data.data && Array.isArray(data.data)) ? data.data : [];
      const list = rawList.map((row) => normalizeChillPayTransactionRow(row));
      const totalRecord = (data.TotalRecord != null ? data.TotalRecord : (data.totalRecord != null ? data.totalRecord : list.length));
      const pageSizeVal = (data.PageSize != null ? data.PageSize : (data.pageSize != null ? data.pageSize : list.length));
      const pageNumberVal = (data.PageNumber != null ? data.PageNumber : (data.pageNumber != null ? data.pageNumber : 1));
      const filteredRecord = (data.FilteredRecord != null ? data.FilteredRecord : (data.filteredRecord != null ? data.filteredRecord : list.length));
      if (totalRecord === 0 && (transactionDateFrom || transactionDateTo)) {
        console.warn('[PG transactions] chillPaySearchPayment 0건 env=' + (useSandbox ? 'sandbox' : 'production') + ' from=' + transactionDateFrom + ' to=' + transactionDateTo + ' res.message=' + (data.message || data.Message));
      }
      console.log('[PG transactions][DEBUG] chillPaySearchPayment success env=' + (useSandbox ? 'sandbox' : 'production') + ' totalRecord=' + totalRecord + ' pageSize=' + pageSizeVal + ' pageNumber=' + pageNumberVal);
      setPgTransactionsLastError(useSandbox, null);
      return {
        success: true,
        data: list,
        totalRecord,
        pageSize: pageSizeVal,
        pageNumber: pageNumberVal,
        filteredRecord,
      };
    }
    const errMsg = (data && (data.Message != null ? data.Message : data.message)) || res.statusText || 'Search Payment 실패';
    const msgLower = (errMsg || '').toLowerCase();
    const isTxnNotFound = msgLower.indexOf('transaction not found') !== -1;
    console.error('[PG transactions][DEBUG] chillPaySearchPayment error response env=' + (useSandbox ? 'sandbox' : 'production') + ' status=' + (data && data.status) + ' Code=' + respCode + ' message=' + errMsg + ' raw=' + JSON.stringify(data));
    if (isTxnNotFound) {
      // Transaction Not Found 는 "해당 조건의 거래 없음"에 가까우므로, 치명적인 오류로 보지 않고
      // 경고만 남기고 UI의 오류 배너는 띄우지 않는다.
      console.warn('[PG transactions] chillPaySearchPayment Transaction Not Found → treat as empty result (env=' + (useSandbox ? 'sandbox' : 'production') + ')');
      setPgTransactionsLastError(useSandbox, null);
      return { success: true, error: null, data: [], totalRecord: 0, pageSize: 0, pageNumber: 0, filteredRecord: 0 };
    }
    setPgTransactionsLastError(useSandbox, errMsg || 'Search Payment 실패');
    return { success: false, error: errMsg, data: [], totalRecord: 0, pageSize: 0, pageNumber: 0, filteredRecord: 0 };
  } catch (err) {
    const raw = err && err.response && err.response.data ? JSON.stringify(err.response.data) : '';
    const msg = err.response && err.response.data ? (err.response.data.message || JSON.stringify(err.response.data)) : err.message;
    console.error('[PG transactions] chillPaySearchPayment error:', msg);
    if (raw) console.error('[PG transactions][DEBUG] chillPaySearchPayment error raw response:', raw);
    setPgTransactionsLastError(useSandbox, msg || 'Search Payment 예외');
    return { success: false, error: msg, data: [], totalRecord: 0, pageSize: 0, pageNumber: 0, filteredRecord: 0 };
  }
}

// 피지거래내역: 30분마다 ChillPay Search Payment로 조회·저장. 일자별 목록 노출용.
function loadPgTransactionStore() {
  try {
    if (!fs.existsSync(CHILLPAY_PG_TRANSACTIONS_PATH)) return { production: { lastFetchedAt: null, byDate: {} }, sandbox: { lastFetchedAt: null, byDate: {} } };
    const raw = fs.readFileSync(CHILLPAY_PG_TRANSACTIONS_PATH, 'utf8');
    const o = JSON.parse(raw || '{}');
    return {
      production: o.production && typeof o.production === 'object' ? { lastFetchedAt: o.production.lastFetchedAt || null, byDate: o.production.byDate && typeof o.production.byDate === 'object' ? o.production.byDate : {} } : { lastFetchedAt: null, byDate: {} },
      sandbox: o.sandbox && typeof o.sandbox === 'object' ? { lastFetchedAt: o.sandbox.lastFetchedAt || null, byDate: o.sandbox.byDate && typeof o.sandbox.byDate === 'object' ? o.sandbox.byDate : {} } : { lastFetchedAt: null, byDate: {} },
    };
  } catch {
    return { production: { lastFetchedAt: null, byDate: {} }, sandbox: { lastFetchedAt: null, byDate: {} } };
  }
}
function savePgTransactionStore(store) {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(CHILLPAY_PG_TRANSACTIONS_PATH, JSON.stringify(store, null, 2));
  } catch (e) {
    console.error('[PG transactions] savePgTransactionStore failed:', e && e.message);
  }
}

function loadPgTransactionBackups() {
  try {
    if (!fs.existsSync(CHILLPAY_PG_TRANSACTIONS_BACKUPS_PATH)) return { resetBackups: [], manualBackup: null };
    const raw = fs.readFileSync(CHILLPAY_PG_TRANSACTIONS_BACKUPS_PATH, 'utf8');
    const o = JSON.parse(raw || '{}');
    const resetBackups = Array.isArray(o.resetBackups) ? o.resetBackups.slice(0, PG_TRANSACTIONS_RESET_BACKUPS_MAX) : [];
    return { resetBackups, manualBackup: o.manualBackup && typeof o.manualBackup === 'object' ? o.manualBackup : null };
  } catch {
    return { resetBackups: [], manualBackup: null };
  }
}

function savePgTransactionBackups(backups) {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(CHILLPAY_PG_TRANSACTIONS_BACKUPS_PATH, JSON.stringify(backups, null, 2));
  } catch (_) {}
}

function pushResetBackup(store) {
  const backups = loadPgTransactionBackups();
  const entry = {
    savedAt: new Date().toISOString(),
    production: JSON.parse(JSON.stringify(store.production || { lastFetchedAt: null, byDate: {} })),
    sandbox: JSON.parse(JSON.stringify(store.sandbox || { lastFetchedAt: null, byDate: {} })),
  };
  backups.resetBackups = [entry, ...backups.resetBackups].slice(0, PG_TRANSACTIONS_RESET_BACKUPS_MAX);
  savePgTransactionBackups(backups);
}
// ChillPay API transactionDate "dd/MM/yyyy HH:mm:ss" → "YYYY-MM-DD"
function parseChillPayTransactionDateToYmd(str) {
  if (!str || typeof str !== 'string') return '';
  const m = str.trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (!m) return '';
  const [, d, mo, y] = m;
  return y + '-' + mo.padStart(2, '0') + '-' + d.padStart(2, '0');
}

// ChillPay API는 PascalCase 응답 → 목록/저장용 camelCase로 통일
function normalizeChillPayTransactionRow(row) {
  if (!row || typeof row !== 'object') return row;
  return Object.assign({}, row, {
    transactionId: (row.TransactionId != null ? row.TransactionId : row.transactionId),
    transactionDate: (row.TransactionDate != null ? row.TransactionDate : row.transactionDate),
    orderNo: (row.OrderNo != null ? row.OrderNo : row.orderNo),
    merchant: (row.Merchant != null ? row.Merchant : row.merchant),
    customer: (row.Customer != null ? row.Customer : row.customer),
    paymentChannel: (row.PaymentChannel != null ? row.PaymentChannel : row.paymentChannel),
    paymentDate: (row.PaymentDate != null ? row.PaymentDate : row.paymentDate),
    amount: (row.Amount != null ? row.Amount : row.amount),
    fee: (row.Fee != null ? row.Fee : row.fee),
    totalAmount: (row.TotalAmount != null ? row.TotalAmount : row.totalAmount),
    currency: (row.Currency != null ? row.Currency : row.currency),
    routeNo: (row.RouteNo != null ? row.RouteNo : row.routeNo),
    status: (row.Status != null ? row.Status : row.status),
    settled: (row.Settled != null ? row.Settled : row.settled),
  });
}
const PG_TRANSACTIONS_INCREMENTAL_DAYS = 2;
const PG_TRANSACTIONS_PAGE_SIZE = 100;
/** 최초 동기화 시 과거 거래를 가져올 때 한 번에 조회할 일수 */
const PG_TRANSACTIONS_INITIAL_CHUNK_DAYS = 30;

// Payment Search용 날짜도 Void/Refund Search와 동일 형식 사용 (dd/MM/yyyy HH:mm:ss, 설정 타임존 기준)
function formatPgTransactionDateForApi(d, timePart) {
  return formatChillPayTransactionDate(d, timePart || '00:00:00');
}

async function fetchChillPayPgTransactionsDateRange(useSandbox, transactionDateFrom, transactionDateTo, intoByDate) {
  let pageNumber = 1;
  let hasMore = true;
  // Payment Search는 TransactionDate, PaymentDate 둘 다 같은 범위로 보내 안정적으로 필터링
  const paymentDateFrom = transactionDateFrom;
  const paymentDateTo = transactionDateTo;
  while (hasMore) {
    const result = await chillPaySearchPayment(useSandbox, {
      orderBy: 'TransactionId',
      orderDir: 'DESC',
      pageSize: String(PG_TRANSACTIONS_PAGE_SIZE),
      pageNumber: String(pageNumber),
      transactionDateFrom,
      transactionDateTo,
      paymentDateFrom,
      paymentDateTo,
    });
    if (!result.success || !Array.isArray(result.data)) break;
    for (const row of result.data) {
      const normalized = normalizeChillPayTransactionRow(row);
      const ymd = parseChillPayTransactionDateToYmd(normalized.transactionDate);
      if (!ymd) continue;
      if (!intoByDate[ymd]) intoByDate[ymd] = [];
      const existing = intoByDate[ymd].find((r) => r.transactionId === normalized.transactionId);
      if (!existing) intoByDate[ymd].push(normalized);
    }
    const total = result.totalRecord != null ? result.totalRecord : 0;
    const fetched = (result.data || []).length;
    if (fetched === 0 || pageNumber * PG_TRANSACTIONS_PAGE_SIZE >= total) hasMore = false;
    else pageNumber += 1;
  }
  return intoByDate;
}

async function fetchChillPayPgTransactionsForEnv(useSandbox, options) {
  const { incremental = false, existingByDate = {} } = options || {};
  const cfg = loadChillPayTransactionConfig();
  const tz = cfg.timezone || 'Asia/Tokyo';
  // 서버 로컬 시간을 기준으로 사용하고, 실제 타임존 보정은 formatChillPayTransactionDate에서 처리한다.
  const now = new Date();
  const toDate = now;
  const byDate = incremental ? JSON.parse(JSON.stringify(existingByDate || {})) : {};

  if (incremental) {
    const fromDate = new Date(toDate);
    fromDate.setDate(fromDate.getDate() - (PG_TRANSACTIONS_INCREMENTAL_DAYS - 1));
    const transactionDateFrom = formatPgTransactionDateForApi(fromDate, '00:00:00');
    const transactionDateTo = formatPgTransactionDateForApi(toDate, '23:59:59');
    await fetchChillPayPgTransactionsDateRange(useSandbox, transactionDateFrom, transactionDateTo, byDate);
    return byDate;
  }

  const initialSyncMonths = Number.isFinite(cfg.pgTransactionInitialSyncMonths) && cfg.pgTransactionInitialSyncMonths > 0 ? Math.min(60, cfg.pgTransactionInitialSyncMonths) : DEFAULT_PG_TRANSACTION_INITIAL_SYNC_MONTHS;
  const maxDaysBack = initialSyncMonths * 31;
  const chunkEnd = new Date(toDate);
  chunkEnd.setHours(23, 59, 59, 999);
  let daysBack = 0;
  while (daysBack < maxDaysBack) {
    const chunkStart = new Date(chunkEnd);
    chunkStart.setDate(chunkStart.getDate() - PG_TRANSACTIONS_INITIAL_CHUNK_DAYS + 1);
    chunkStart.setHours(0, 0, 0, 0);
    const transactionDateFrom = formatPgTransactionDateForApi(chunkStart, '00:00:00');
    const transactionDateTo = formatPgTransactionDateForApi(chunkEnd, '23:59:59');
    const beforeCount = Object.keys(byDate).reduce((sum, k) => sum + (byDate[k] || []).length, 0);
    await fetchChillPayPgTransactionsDateRange(useSandbox, transactionDateFrom, transactionDateTo, byDate);
    const afterCount = Object.keys(byDate).reduce((sum, k) => sum + (byDate[k] || []).length, 0);
    const added = afterCount - beforeCount;
    daysBack += PG_TRANSACTIONS_INITIAL_CHUNK_DAYS;
    if (added === 0) break;
    chunkEnd.setDate(chunkEnd.getDate() - PG_TRANSACTIONS_INITIAL_CHUNK_DAYS);
  }
  return byDate;
}

function hasPgTransactionData(block) {
  return block && block.lastFetchedAt && block.byDate && typeof block.byDate === 'object' && Object.keys(block.byDate).length > 0;
}

async function runPgTransactionFetchAsync() {
  const store = loadPgTransactionStore();
  const cfg = loadChillPayTransactionConfig();

  // 운영(production) 환경 동기화
  const hasProdCred = !!(cfg && cfg.production && cfg.production.mid && cfg.production.apiKey && cfg.production.md5);
  if (!hasProdCred) {
    console.warn('[PG transactions] production 자격증명 미설정 → 동기화 스킵');
  } else {
    try {
      const isFullSync = !hasPgTransactionData(store.production);
      const newByDate = await fetchChillPayPgTransactionsForEnv(false, {
        incremental: !isFullSync,
        existingByDate: store.production.byDate || {},
      });
      const hasData = newByDate && typeof newByDate === 'object' && Object.keys(newByDate).length > 0;
      if (hasData || isFullSync === false) {
        store.production.byDate = newByDate;
      }
      store.production.lastFetchedAt = new Date().toISOString();
      if (!hasData && isFullSync) {
        console.error('[PG transactions] production: API 실패 또는 결과 없음. 기존 데이터 유지.');
      }
    } catch (e) {
      console.error('[PG transactions] production fetch error', e && e.message, e && e.stack);
    }
  }

  // 샌드박스(sandbox) 환경 동기화
  const hasSandboxCred = !!(cfg && cfg.sandbox && cfg.sandbox.mid && cfg.sandbox.apiKey && cfg.sandbox.md5);
  if (!hasSandboxCred) {
    console.warn('[PG transactions] sandbox 자격증명 미설정 → 동기화 스킵');
  } else {
    try {
      const isFullSyncSandbox = !hasPgTransactionData(store.sandbox);
      const newByDateSandbox = await fetchChillPayPgTransactionsForEnv(true, {
        incremental: !isFullSyncSandbox,
        existingByDate: store.sandbox.byDate || {},
      });
      const hasDataSandbox = newByDateSandbox && typeof newByDateSandbox === 'object' && Object.keys(newByDateSandbox).length > 0;
      if (hasDataSandbox || isFullSyncSandbox === false) {
        store.sandbox.byDate = newByDateSandbox;
      }
      store.sandbox.lastFetchedAt = new Date().toISOString();
      if (!hasDataSandbox && isFullSyncSandbox) {
        console.error('[PG transactions] sandbox: API 실패 또는 결과 없음. 기존 데이터 유지.');
      }
    } catch (e) {
      console.error('[PG transactions] sandbox fetch error', e && e.message, e && e.stack);
    }
  }

  savePgTransactionStore(store);
}

function runPgTransactionFetch() {
  runPgTransactionFetchAsync().catch((e) => console.error('[PG transactions] fetch error', e && e.message));
}
function startPgTransactionFetchInterval() {
  runPgTransactionFetch();
  const cfg = loadChillPayTransactionConfig();
  const minutes = Number.isFinite(cfg.pgTransactionSyncIntervalMinutes) && cfg.pgTransactionSyncIntervalMinutes > 0
    ? Math.min(1440, cfg.pgTransactionSyncIntervalMinutes) : DEFAULT_PG_TRANSACTION_SYNC_INTERVAL_MINUTES;
  const intervalMs = minutes * 60 * 1000;
  setInterval(runPgTransactionFetch, intervalMs);
}

// 무효 노티 전송 이력 (ChillPay 수동 무효 등 동기화 시 중복 전송 방지)
const VOID_NOTI_SENT_MAX = 10000;
function loadChillPayVoidNotiSent() {
  try {
    if (!fs.existsSync(CHILLPAY_VOID_NOTI_SENT_PATH)) return [];
    const raw = fs.readFileSync(CHILLPAY_VOID_NOTI_SENT_PATH, 'utf8');
    const arr = JSON.parse(raw || '[]');
    return Array.isArray(arr) ? arr.filter((id) => typeof id === 'string' && id.trim()) : [];
  } catch {
    return [];
  }
}
function saveChillPayVoidNotiSent(ids) {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    const list = Array.isArray(ids) ? ids.slice(-VOID_NOTI_SENT_MAX) : [];
    fs.writeFileSync(CHILLPAY_VOID_NOTI_SENT_PATH, JSON.stringify(list, null, 0));
  } catch (_) {}
}
function markVoidNotiSent(transactionId) {
  const tid = String(transactionId || '').trim();
  if (!tid) return;
  const list = loadChillPayVoidNotiSent();
  if (list.includes(tid)) return;
  list.push(tid);
  saveChillPayVoidNotiSent(list);
}
function hasVoidNotiSent(transactionId) {
  const tid = String(transactionId || '').trim();
  if (!tid) return false;
  return loadChillPayVoidNotiSent().includes(tid);
}

// 환불 노티 전송 이력 (ChillPay 수동 환불 등 동기화 시 중복 전송 방지)
const REFUND_NOTI_SENT_MAX = 10000;
function loadChillPayRefundNotiSent() {
  try {
    if (!fs.existsSync(CHILLPAY_REFUND_NOTI_SENT_PATH)) return [];
    const raw = fs.readFileSync(CHILLPAY_REFUND_NOTI_SENT_PATH, 'utf8');
    const arr = JSON.parse(raw || '[]');
    return Array.isArray(arr) ? arr.filter((id) => typeof id === 'string' && id.trim()) : [];
  } catch {
    return [];
  }
}
function saveChillPayRefundNotiSent(ids) {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    const list = Array.isArray(ids) ? ids.slice(-REFUND_NOTI_SENT_MAX) : [];
    fs.writeFileSync(CHILLPAY_REFUND_NOTI_SENT_PATH, JSON.stringify(list, null, 0));
  } catch (_) {}
}
function markRefundNotiSent(transactionId) {
  const tid = String(transactionId || '').trim();
  if (!tid) return;
  const list = loadChillPayRefundNotiSent();
  if (list.includes(tid)) return;
  list.push(tid);
  saveChillPayRefundNotiSent(list);
}
function hasRefundNotiSent(transactionId) {
  const tid = String(transactionId || '').trim();
  if (!tid) return false;
  return loadChillPayRefundNotiSent().includes(tid);
}

// 무효/환불 노티 발송 이력 (거래노티 페이지용). 발송 시마다 append, 조회 시 최근 30일만 로드
function appendVoidRefundNotiLog(entry) {
  const sentAtIso = new Date().toISOString();
  const log = { sentAtIso, ...entry };
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.appendFile(VOID_REFUND_NOTI_LOG_PATH, JSON.stringify(log) + '\n', () => {});
  } catch (_) {}
}
function loadVoidRefundNotiLog(days) {
  const limit = Math.min(Math.max(Number(days) || 30, 1), 90);
  const cutoff = Date.now() - limit * 24 * 60 * 60 * 1000;
  try {
    if (!fs.existsSync(VOID_REFUND_NOTI_LOG_PATH)) return [];
    const raw = fs.readFileSync(VOID_REFUND_NOTI_LOG_PATH, 'utf8');
    const lines = raw.split('\n').filter((l) => l.trim().length > 0);
    const entries = [];
    for (let i = lines.length - 1; i >= 0 && entries.length < 2000; i--) {
      try {
        const obj = JSON.parse(lines[i]);
        const t = Date.parse(obj.sentAtIso || obj.sentAt || '');
        if (!Number.isNaN(t) && t >= cutoff) entries.push(obj);
      } catch (_) {}
    }
    return entries.sort((a, b) => (Date.parse(b.sentAtIso || '') || 0) - (Date.parse(a.sentAtIso || '') || 0));
  } catch {
    return [];
  }
}
// transactionId별 최근 무효/환불 로그 맵 (거래내역 내역/사유 표시용)
function buildVoidRefundNotiMap(days) {
  const entries = loadVoidRefundNotiLog(days || 30);
  const map = {};
  for (const e of entries) {
    const tid = e.transactionId && String(e.transactionId).trim();
    if (tid) map[tid] = e;
  }
  return map;
}
// 무효/환불 노티 전송 시각 맵 (type별, 최신 1건만). 무효거래·환불거래 목록에서 "전송일/전송시각" 표시용
function buildVoidRefundNotiSentMaps(days) {
  const entries = loadVoidRefundNotiLog(days || 30);
  const voidMap = {};
  const refundMap = {};
  for (const e of entries) {
    const tid = e.transactionId && String(e.transactionId).trim();
    if (!tid) continue;
    if (e.type === 'void' && !voidMap[tid]) voidMap[tid] = e;
    if (e.type === 'refund' && !refundMap[tid]) refundMap[tid] = e;
  }
  return { voidMap, refundMap };
}
// 무효/환불 노티 전송 시각 맵 (type별, 최신 1건만). 무효거래·환불거래 목록에서 "전송일/전송시각" 표시용
function buildVoidRefundNotiSentMaps(days) {
  const entries = loadVoidRefundNotiLog(days || 30);
  const voidMap = {};
  const refundMap = {};
  for (const e of entries) {
    const tid = e.transactionId && String(e.transactionId).trim();
    if (!tid) continue;
    if (e.type === 'void' && !voidMap[tid]) voidMap[tid] = e;
    if (e.type === 'refund' && !refundMap[tid]) refundMap[tid] = e;
  }
  return { voidMap, refundMap };
}

// ChillPay에서 이미 무효 처리된 건 조회 후, 우리 로그와 매칭해 미전송 건만 무효 노티 전송 (수동 무효 동기화)
async function syncChillPayVoidNoti() {
  const cfg = loadChillPayTransactionConfig();
  const cred = cfg.production;
  if (!cred.mid || !cred.apiKey || !cred.md5) {
    return { success: false, error: 'ChillPay Transaction API(Production) 미설정', sent: 0, total: 0 };
  }
  const now = new Date();
  const toDate = new Date(now.getTime());
  const fromDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
  const transactionDateFrom = formatChillPayTransactionDate(fromDate, '00:00:00');
  const transactionDateTo = formatChillPayTransactionDate(toDate, '23:59:59');
  const searchResult = await chillPaySearchVoid(false, {
    transactionDateFrom,
    transactionDateTo,
    pageSize: '100',
    pageNumber: '1',
  });
  if (!searchResult.success) {
    return { success: false, error: searchResult.error || 'Search Void 실패', sent: 0, total: 0 };
  }
  const list = searchResult.data || [];
  let sent = 0;
  let alreadySent = 0;
  let noMatch = 0;
  const items = [];
  for (const item of list) {
    const txId = item.transactionId != null ? String(item.transactionId) : (item.TransactionId != null ? String(item.TransactionId) : '');
    const orderNo = item.orderNo != null ? String(item.orderNo) : (item.OrderNo != null ? String(item.OrderNo) : '');
    if (!txId) continue;
    // ChillPay 상태값이 실제로 "무효 처리된 건"인지 1차 필터 (Success 등은 노티 전송 대상 아님)
    const statusRaw = item.status != null ? String(item.status).trim() : (item.Status != null ? String(item.Status).trim() : '');
    const statusLower = statusRaw.toLowerCase();
    let isVoided = false;
    if (statusRaw) {
      const num = Number(statusRaw);
      if (num === 6 || num === 7) isVoided = true; // 6=Void Requested, 7=Voided
      const compact = statusLower.replace(/\s+/g, '');
      if (compact === 'voided' || compact === 'voidsuccess' || compact === 'voidrequested') isVoided = true;
    }
    if (!isVoided) {
      items.push({ transactionId: txId, orderNo, result: 'notVoided', status: statusRaw });
      continue;
    }
    if (hasVoidNotiSent(txId)) {
      alreadySent += 1;
      items.push({ transactionId: txId, orderNo, result: 'alreadySent' });
      continue;
    }
    // 1차: 이 TransactionId에 해당하는 모든 로그 후보 수집 (가맹점 등록된 것만)
    const candidates = NOTI_LOGS.filter((l) => {
      const body = l.body && typeof l.body === 'object' ? l.body : (typeof l.body === 'string'
        ? (() => { try { return JSON.parse(l.body); } catch { return {}; } })()
        : {});
      const logTxId = body.TransactionId != null ? String(body.TransactionId) : (body.transactionId != null ? String(body.transactionId) : '');
      if (logTxId !== txId) return false;
      return l.merchantId && MERCHANTS.get(l.merchantId);
    });
    // 2차: 가능하면 "결제 성공" 로그를 우선 사용, 없으면 취소 로그라도 사용 (무효 노티용 payload는 PaymentStatus를 새로 세팅함)
    let log = null;
    if (candidates.length > 0) {
      log =
        candidates.find((l) => {
          const body = l.body && typeof l.body === 'object' ? l.body : (typeof l.body === 'string'
            ? (() => { try { return JSON.parse(l.body); } catch (e) { return {}; } })()
            : {});
          const ps = body.PaymentStatus != null ? body.PaymentStatus : (body.paymentStatus != null ? body.paymentStatus : body.status);
          const isSuccess =
            ps === 1 || ps === '1' ||
            (body.paymentStatus && String(body.paymentStatus).toLowerCase() === 'success') ||
            body.status === 1;
          return isSuccess;
        }) || candidates[0];
    }
    if (!log) {
      noMatch += 1;
      const anyLog = NOTI_LOGS.find((l) => {
        const body = l.body && typeof l.body === 'object' ? l.body : (typeof l.body === 'string' ? (() => { try { return JSON.parse(l.body); } catch { return {}; } })() : {});
        const logTxId = body.TransactionId != null ? String(body.TransactionId) : (body.transactionId != null ? String(body.transactionId) : '');
        return logTxId === txId;
      });
      if (anyLog) {
        console.log('[무효 동기화 noMatch] txId=', txId, '원인: 가맹점 미등록 또는 결제 미성공. merchantId=', anyLog.merchantId, 'MERCHANTS.get=', anyLog.merchantId ? !!MERCHANTS.get(anyLog.merchantId) : false);
      } else {
        console.log('[무효 동기화 noMatch] txId=', txId, '원인: NOTI_LOGS에 해당 TransactionId 건 없음 (다른 서버에서 수신했거나 미수신)');
      }
      items.push({ transactionId: txId, orderNo, result: 'noMatch' });
      continue;
    }
    try {
      console.log('[무효 동기화] 무효 노티 전송 시도 txId=', txId, 'merchantId=', log.merchantId);
      await sendVoidOrRefundNoti(log, 'void');
      markVoidNotiSent(txId);
      sent += 1;
      items.push({ transactionId: txId, orderNo, result: 'sent' });
    } catch (_) {
      items.push({ transactionId: txId, orderNo, result: 'noMatch' });
    }
  }
  return { success: true, sent, total: list.length, alreadySent, noMatch, items, syncedAt: new Date().toISOString() };
}

// ChillPay에서 이미 환불 처리된 건 조회 후, 우리 로그와 매칭해 미전송 건만 환불 노티 전송 (수동 환불 동기화)
async function syncChillPayRefundNoti() {
  const cfg = loadChillPayTransactionConfig();
  const cred = cfg.production;
  if (!cred.mid || !cred.apiKey || !cred.md5) {
    return { success: false, error: 'ChillPay Transaction API(Production) 미설정', sent: 0, total: 0 };
  }
  const now = new Date();
  const toDate = new Date(now.getTime());
  const fromDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
  const transactionDateFrom = formatChillPayTransactionDate(fromDate, '00:00:00');
  const transactionDateTo = formatChillPayTransactionDate(toDate, '23:59:59');
  const searchResult = await chillPaySearchRefund(false, {
    transactionDateFrom,
    transactionDateTo,
    pageSize: '100',
    pageNumber: '1',
  });
  if (!searchResult.success) {
    return { success: false, error: searchResult.error || 'Search Refund 실패', sent: 0, total: 0 };
  }
  const list = searchResult.data || [];
  let sent = 0;
  let alreadySent = 0;
  let noMatch = 0;
  const items = [];
  for (const item of list) {
    const txId = item.transactionId != null ? String(item.transactionId) : (item.TransactionId != null ? String(item.TransactionId) : '');
    const orderNo = item.orderNo != null ? String(item.orderNo) : (item.OrderNo != null ? String(item.OrderNo) : '');
    if (!txId) continue;
    // ChillPay 상태값이 실제로 "환불 처리된 건"인지 1차 필터 (Success 등은 노티 전송 대상 아님)
    const statusRaw = item.status != null ? String(item.status).trim() : (item.Status != null ? String(item.Status).trim() : '');
    const statusLower = statusRaw.toLowerCase();
    let isRefunded = false;
    if (statusRaw) {
      const num = Number(statusRaw);
      if (num === 8 || num === 9) isRefunded = true; // 8=Refund Requested, 9=Refunded
      const compact = statusLower.replace(/\s+/g, '');
      if (compact === 'refunded' || compact === 'refundsuccess' || compact === 'refundrequested') isRefunded = true;
    }
    if (!isRefunded) {
      items.push({ transactionId: txId, orderNo, result: 'notRefunded', status: statusRaw });
      continue;
    }
    if (hasRefundNotiSent(txId)) {
      alreadySent += 1;
      items.push({ transactionId: txId, orderNo, result: 'alreadySent' });
      continue;
    }
    const candidates = NOTI_LOGS.filter((l) => {
      const body = l.body && typeof l.body === 'object' ? l.body : (typeof l.body === 'string'
        ? (() => { try { return JSON.parse(l.body); } catch { return {}; } })()
        : {});
      const logTxId = body.TransactionId != null ? String(body.TransactionId) : (body.transactionId != null ? String(body.transactionId) : '');
      if (logTxId !== txId) return false;
      return l.merchantId && MERCHANTS.get(l.merchantId);
    });
    let log = null;
    if (candidates.length > 0) {
      log =
        candidates.find((l) => {
          const body = l.body && typeof l.body === 'object' ? l.body : (typeof l.body === 'string'
            ? (() => { try { return JSON.parse(l.body); } catch (e) { return {}; } })()
            : {});
          const ps = body.PaymentStatus != null ? body.PaymentStatus : (body.paymentStatus != null ? body.paymentStatus : body.status);
          const isSuccess =
            ps === 1 || ps === '1' ||
            (body.paymentStatus && String(body.paymentStatus).toLowerCase() === 'success') ||
            body.status === 1;
          return isSuccess;
        }) || candidates[0];
    }
    if (!log) {
      noMatch += 1;
      items.push({ transactionId: txId, orderNo, result: 'noMatch' });
      continue;
    }
    try {
      await sendVoidOrRefundNoti(log, 'refund');
      markRefundNotiSent(txId);
      sent += 1;
      items.push({ transactionId: txId, orderNo, result: 'sent' });
    } catch (_) {
      items.push({ transactionId: txId, orderNo, result: 'noMatch' });
    }
  }
  return { success: true, sent, total: list.length, alreadySent, noMatch, items, syncedAt: new Date().toISOString() };
}

// 결제 시각 기준 자동화 구간: 'void_auto' | 'void_manual' | 'refund'
// 기준 (태국 시간 기준, 일본 시간은 +2시간):
// - 당일 00:01 ~ (자동 무효 마감 + 2분)  → void_auto (자동 무효 가능, 2분 오차 허용)
// - 그 이후 → void_manual (이메일로 환불/무효 요청 안내용)
// - 환불 가능 일자(결제 다음날 00:00부터 N일간)는 별도 isWithinRefundWindow에서 일자 기준으로 판정
function getVoidRefundWindow(paymentDateOrIso) {
  const cfg = loadChillPayTransactionConfig();
  const tz = cfg.timezone || DEFAULT_CHILLPAY_TIMEZONE;
  let date = null;
  const str = (paymentDateOrIso && String(paymentDateOrIso).trim()) || '';
  if (!str) return 'void_manual';
  if (/^\d{4}-\d{2}-\d{2}T/.test(str)) {
    date = new Date(str);
  } else if (/^\d{2}\/\d{2}\/\d{4}\s+\d{1,2}:\d{2}/.test(str)) {
    const [dpart, tpart] = str.split(/\s+/);
    const [dd, mm, yyyy] = dpart.split('/');
    const [h, min] = (tpart || '0:0').split(':');
    date = new Date(Number(yyyy), Number(mm) - 1, Number(dd), Number(h) || 0, Number(min) || 0, 0, 0);
  } else {
    date = new Date(str);
  }
  if (Number.isNaN(date.getTime())) return 'void_manual';
  // 태국 시간 기준 시각 추출
  const parts = new Intl.DateTimeFormat('en-CA', { timeZone: tz, hour: '2-digit', minute: '2-digit', hour12: false }).formatToParts(date);
  const get = (name) => (parts.find((p) => p.type === name) || {}).value || '0';
  const hour = parseInt(get('hour'), 10) || 0;
  const minute = parseInt(get('minute'), 10) || 0;
  const payMins = hour * 60 + minute;
  const cutoffMins = cfg.voidCutoffHour * 60 + cfg.voidCutoffMinute; // 기본 21:00
  const toleranceMins = 2; // TransactionDate / PaymentDate 1분 차이 등을 위한 여유
  const effectiveCutoff = Math.min(cutoffMins + toleranceMins, 23 * 60 + 59);
  // 00:01 이후 ~ (자동 무효 마감 + 2분)까지는 자동 무효로 인정
  if (payMins <= effectiveCutoff && payMins > 0) return 'void_auto';
  return 'void_manual';
}

// 환불 요청 버튼 활성화 여부: 결제 다음날 0시 기준, 환경설정 환불 가능 기간(일) 안인지 검사
function isWithinRefundWindow(paymentDateOrIso, nowIso) {
  const cfg = loadChillPayTransactionConfig();
  const tz = cfg.timezone || DEFAULT_CHILLPAY_TIMEZONE;
  const days = Number(cfg.refundWindowDays) > 0 ? cfg.refundWindowDays : DEFAULT_REFUND_WINDOW_DAYS;
  const src = (paymentDateOrIso && String(paymentDateOrIso).trim()) || '';
  if (!src) return false;
  let date = null;
  if (/^\d{4}-\d{2}-\d{2}T/.test(src)) {
    date = new Date(src);
  } else if (/^\d{2}\/\d{2}\/\d{4}\s+\d{1,2}:\d{2}/.test(src)) {
    const [dpart, tpart] = src.split(/\s+/);
    const [dd, mm, yyyy] = dpart.split('/');
    const [h, min] = (tpart || '0:0').split(':');
    date = new Date(Number(yyyy), Number(mm) - 1, Number(dd), Number(h) || 0, Number(min) || 0, 0, 0);
  } else {
    date = new Date(src);
  }
  if (Number.isNaN(date.getTime())) return false;
  const toYmdUtc = (d) => {
    const parts = new Intl.DateTimeFormat('en-CA', { timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit' }).formatToParts(d);
    const get = (name) => (parts.find((p) => p.type === name) || {}).value || '0';
    const y = parseInt(get('year'), 10) || 0;
    const m = parseInt(get('month'), 10) || 1;
    const da = parseInt(get('day'), 10) || 1;
    return Date.UTC(y, m - 1, da);
  };
  const payUtc = toYmdUtc(date);
  const nowDate = nowIso ? new Date(nowIso) : new Date();
  if (Number.isNaN(nowDate.getTime())) return false;
  const nowUtc = toYmdUtc(nowDate);
  const diffDays = Math.floor((nowUtc - payUtc) / (24 * 60 * 60 * 1000));
  // 결제 다음날(>=1일)부터 환불 가능, 설정된 환불 기간(days)까지만 허용
  return diffDays >= 1 && diffDays <= days;
}

// 현재 시각이 수동 무효 이메일 가능 시간대(설정 기준 void_manual 구간)인지 여부
function isCurrentTimeInVoidManualWindow() {
  return getVoidRefundWindow(new Date().toISOString()) === 'void_manual';
}

// 취소/무효/환불 목록·이메일용 Route 번호만 표시 (callback/16 → 16, 가맹점 routeNo 우선)
function getRouteNoDisplay(merchant, routeKey) {
  if (merchant && (merchant.routeNo != null && String(merchant.routeNo).trim() !== '')) return String(merchant.routeNo).trim();
  const s = String(routeKey || '');
  const m = s.match(/\d+/);
  return m ? m[0] : (s || '');
}

// 수동 무효용 이메일 본문 생성 (템플릿 치환). 반환: { subject, body } (body는 encodeURIComponent용). 본문에 Route No. 16 형식으로 노출
function buildVoidEmailContent(log) {
  const cfg = loadChillPayTransactionConfig();
  const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
  const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
  const mid = cfg.useSandbox ? (cfg.sandbox && cfg.sandbox.mid) : (cfg.production && cfg.production.mid);
  const transNo = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : '');
  const orderNo = body.OrderNo != null ? body.OrderNo : (body.orderNo != null ? body.orderNo : '');
  const amount = body.Amount != null ? body.Amount : (body.amount != null ? body.amount : '');
  const routeNo = getRouteNoDisplay(merchant, log.routeKey);
  const paymentDate = body.PaymentDate || body.paymentDate || log.receivedAtIso || log.receivedAt || '';
  const defaultTemplate = '아래 거래에 대해 취소를 요청합니다.\n\nTransactionId(transNo): {{transNo}}\nOrderNo: {{orderNo}}\nAmount: {{amount}}\nRoute No. {{routeNo}}\nPaymentDate: {{paymentDate}}\nMID: {{mid}}\n';
  const template = (cfg.emailBodyTemplate || '').trim() || defaultTemplate;
  const bodyText = template
    .replace(/\{\{transNo\}\}/g, String(transNo))
    .replace(/\{\{orderNo\}\}/g, String(orderNo))
    .replace(/\{\{amount\}\}/g, String(amount))
    .replace(/\{\{routeNo\}\}/g, String(routeNo))
    .replace(/\{\{paymentDate\}\}/g, String(paymentDate))
    .replace(/\{\{mid\}\}/g, String(mid || ''));
  return { subject: '취소 요청: ' + (transNo || orderNo || ''), body: bodyText };
}

// 무효 또는 환불 노티 전송 (가맹점 callback/result + 전산). log = NOTI_LOGS 항목, type = 'void' | 'refund'
// 전송하지 않은 경우(가맹점 없음 등)에도 노티 결과에 '미전송'으로 기록해, '안 보냄'과 '보냈는데 실패'를 구분할 수 있게 함.
async function sendVoidOrRefundNoti(log, type) {
  const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
  const bodyForId = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body || '{}'); } catch { return {}; } })() : {});
  const txIdForLog = bodyForId.TransactionId != null ? bodyForId.TransactionId : (bodyForId.transactionId != null ? bodyForId.transactionId : '');
  const orderNoForLog = bodyForId.OrderNo != null ? bodyForId.OrderNo : (bodyForId.orderNo != null ? bodyForId.orderNo : '');
  if (!merchant || !log.body) {
    const skipReason = !merchant ? 'no_merchant' : 'no_body';
    appendVoidRefundNotiLog({
      type,
      transactionId: txIdForLog,
      orderNo: orderNoForLog,
      merchantId: log.merchantId || '',
      routeNo: merchant ? (merchant.routeNo || '') : '',
      relayStatus: 'skip',
      internalStatus: 'skip',
      skipReason,
      env: log.env || 'live',
    });
    return { success: false, error: !merchant ? '가맹점 없음(미등록)' : '노티 본문 없음' };
  }
  const paymentStatus = type === 'refund' ? '9' : '2';
  const payload = typeof log.body === 'object' ? { ...log.body, PaymentStatus: paymentStatus } : { ...JSON.parse(log.body || '{}'), PaymentStatus: paymentStatus };
  console.log('[취소 노티 전송] type=', type, 'PaymentStatus=', paymentStatus, 'merchantId=', log.merchantId, 'TransactionId=', payload.TransactionId);
  const urls = [];
  if (merchant.callbackUrl) urls.push({ url: merchant.callbackUrl, kind: 'callback' });
  if (merchant.resultUrl && merchant.resultUrl !== merchant.callbackUrl) urls.push({ url: merchant.resultUrl, kind: 'result' });
  let relayOk = true;
  for (const { url } of urls) {
    try {
      const res = await relayToMerchant(url, payload, { contentType: 'application/json' });
      if (res.status < 200 || res.status >= 300) relayOk = false;
    } catch (e) {
      relayOk = false;
    }
  }
  let internalOk = false;
  const internalPayload = transformForInternal(payload, merchant);
  let internalUrl = merchant.internalTargetId ? findInternalTargetUrl(merchant.internalTargetId, 'callback') : null;
  if (!internalUrl && INTERNAL_NOTI_URL) internalUrl = INTERNAL_NOTI_URL;
  if (internalUrl) {
    try {
      console.log('[취소 노티 → 전산] url=', internalUrl, 'PaymentStatus=', internalPayload.PaymentStatus, 'TransactionId=', internalPayload.TransactionId);
      const internalRes = await sendToInternal(internalUrl, internalPayload);
      internalOk = internalRes.success;
      if (!internalOk) console.warn('[취소 노티 → 전산 실패] status=', internalRes.status);
    } catch (e) {
      console.warn('[취소 노티 → 전산 예외]', e.message);
    }
    appendInternalLog({
      storedAt: new Date().toISOString(),
      merchantId: log.merchantId,
      routeNo: merchant.routeNo || '',
      internalTargetId: merchant.internalTargetId || '',
      payload: internalPayload,
      internalTargetUrl: internalUrl,
      internalDeliveryStatus: internalOk ? 'ok' : 'fail',
    });
  }
  const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
  const internalSkipReason = !internalUrl ? 'no_internal_url' : null;
  appendVoidRefundNotiLog({
    type: type,
    transactionId: body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : ''),
    orderNo: body.OrderNo != null ? body.OrderNo : (body.orderNo != null ? body.orderNo : ''),
    merchantId: log.merchantId || '',
    routeNo: merchant ? (merchant.routeNo || '') : '',
    relayStatus: relayOk ? 'ok' : 'fail',
    internalStatus: internalUrl ? (internalOk ? 'ok' : 'fail') : 'skip',
    internalSkipReason: internalSkipReason || undefined,
    env: log.env || 'live',
  });
  return { success: relayOk, internalOk };
}

/**
 * PDF 4 규격: PG 오리지널 노티 → 전산 저장용 가공
 * - 오리지널 선택 시: PG 노티 그대로 전달 (가공 없음)
 * - 그 외: Amount(통화별 X100,/100,=), RouteNo(현재/삭제), CustomerId(가맹점설정/삭제) 적용
 */
function transformForInternal(original, merchant) {
  const currency = String(original.Currency || '392');
  const settings = loadInternalNotiSettingsFull();
  if (settings.original && settings.original[currency]) {
    return typeof original === 'object' && original !== null ? { ...original } : original;
  }

  const routeNo = (merchant && merchant.routeNo) || '7';
  const internalCustomerId = (merchant && merchant.internalCustomerId) || 'M035594';
  const routeNoMode = (settings.routeNoMode && settings.routeNoMode[currency]) || 'current';
  const customerIdMode = (settings.customerIdMode && settings.customerIdMode[currency]) || 'current';

  const amountRaw = Number(original.Amount) || 0;
  const processed = applyAmountRule(amountRaw, currency);
  const amount = Number.isInteger(processed) ? String(Math.round(processed)) : String(Number(processed).toFixed(2));

  const payload = {
    TransactionId: original.TransactionId,
    Amount: amount,
    OrderNo: original.OrderNo,
    BankCode: original.BankCode,
    PaymentDate: original.PaymentDate,
    PaymentStatus: original.PaymentStatus,
    BankRefCode: original.BankRefCode,
    CurrentDate: original.CurrentDate,
    CurrentTime: original.CurrentTime,
    PaymentDescription: original.PaymentDescription || '',
    CreditCardToken: original.CreditCardToken || '',
    Currency: original.Currency,
    CustomerName: original.CustomerName || '',
    CardNumber: '000000000000',
    CheckSum: original.CheckSum || '',
  };
  if (routeNoMode !== 'delete') payload.RouteNo = routeNo;
  if (customerIdMode !== 'delete') payload.CustomerId = internalCustomerId;

  return payload;
}

/**
 * 개발용 전산 노티 가공 (개발 노티 설정 기준)
 * - 구조는 transformForInternal 과 동일하지만, DEV_INTERNAL_NOTI_SETTINGS를 사용
 */
function transformForDevInternal(original, merchant) {
  const currency = String(original.Currency || '392');
  const settings = loadDevInternalNotiSettingsFull();
  if (settings.original && settings.original[currency]) {
    return typeof original === 'object' && original !== null ? { ...original } : original;
  }

  const routeNo = (merchant && merchant.routeNo) || '7';
  const internalCustomerId = (merchant && merchant.internalCustomerId) || 'M035594';
  const routeNoMode = (settings.routeNoMode && settings.routeNoMode[currency]) || 'current';
  const customerIdMode = (settings.customerIdMode && settings.customerIdMode[currency]) || 'current';

  const amountRaw = Number(original.Amount) || 0;
  const processed = applyDevAmountRule(amountRaw, currency);
  const amount = Number.isInteger(processed) ? String(Math.round(processed)) : String(Number(processed).toFixed(2));

  const payload = {
    TransactionId: original.TransactionId,
    Amount: amount,
    OrderNo: original.OrderNo,
    BankCode: original.BankCode,
    PaymentDate: original.PaymentDate,
    PaymentStatus: original.PaymentStatus,
    BankRefCode: original.BankRefCode,
    CurrentDate: original.CurrentDate,
    CurrentTime: original.CurrentTime,
    PaymentDescription: original.PaymentDescription || '',
    CreditCardToken: original.CreditCardToken || '',
    Currency: original.Currency,
    CustomerName: original.CustomerName || '',
    CardNumber: '000000000000',
    CheckSum: original.CheckSum || '',
  };
  if (routeNoMode !== 'delete') payload.RouteNo = routeNo;
  if (customerIdMode !== 'delete') payload.CustomerId = internalCustomerId;

  return payload;
}

/**
 * 가맹점으로 원본 그대로 릴레이
 * - options.rawBody 가 있으면 수신한 원문 그대로 전송 (가공 없음)
 * - 없으면 body 객체를 contentType에 맞게 직렬화해 전송
 */
async function relayToMerchant(callbackUrl, body, options = {}) {
  const contentType = (options.contentType || '').toString().trim() || 'application/json';
  const rawBody = options.rawBody;
  let data;
  let headers = { 'Content-Type': contentType };
  if (rawBody !== undefined && rawBody !== null) {
    data = typeof rawBody === 'string' ? rawBody : (Buffer.isBuffer(rawBody) ? rawBody : Buffer.from(String(rawBody)));
  } else if (body && typeof body === 'object' && !Buffer.isBuffer(body)) {
    const ct = contentType.toLowerCase();
    if (ct.includes('application/x-www-form-urlencoded')) {
      const params = new URLSearchParams();
      for (const [k, v] of Object.entries(body)) {
        params.append(k, v === null || v === undefined ? '' : String(v));
      }
      data = params.toString();
    } else {
      data = body;
      if (!headers['Content-Type'].toLowerCase().includes('application/json')) headers['Content-Type'] = 'application/json';
    }
  } else {
    data = body;
  }
  const res = await axios.post(callbackUrl, data, {
    headers,
    timeout: RELAY_TIMEOUT_MS,
    validateStatus: () => true,
  });
  return res;
}

/**
 * 전산 쪽으로 가공된 노티 전송
 * HTTP 2xx여도 응답 본문에 success: false / ok: false 가 있으면 실패로 처리 (전산에서 결제 미존재 등)
 */
async function sendToInternal(internalUrl, payload) {
  if (!internalUrl) return { success: false, status: 0 };
  try {
    const res = await axios.post(internalUrl, payload, {
      headers: { 'Content-Type': 'application/json' },
      timeout: INTERNAL_TIMEOUT_MS,
      validateStatus: () => true,
    });
    const statusOk = res.status >= 200 && res.status < 300;
    let bodyOk = true;
    if (statusOk && res.data != null && typeof res.data === 'object') {
      if (res.data.success === false || res.data.ok === false) {
        bodyOk = false;
        if (res.data.message || res.data.reason) console.warn('[전산 응답] success=false', res.data.message || res.data.reason);
      }
    }
    const ok = statusOk && bodyOk;
    return { success: ok, status: res.status };
  } catch (err) {
    return { success: false, status: 0, error: err.message };
  }
}

// routeKey(rount_c1, rount_r1 등)로 어떤 가맹점/타입(callback,result) 인지 찾기
function findMerchantByRouteKey(routeKey) {
  for (const [merchantId, m] of MERCHANTS.entries()) {
    if (!m) continue;
    if (m.routeCallbackKey === routeKey) {
      return {
        merchantId,
        merchant: m,
        kind: 'callback',
        targetUrl: m.callbackUrl,
      };
    }
    if (m.routeResultKey === routeKey) {
      return {
        merchantId,
        merchant: m,
        kind: 'result',
        targetUrl: m.resultUrl,
      };
    }
  }
  return null;
}

// 테스트 결제 환경 중 useTestResultPage 인 config 의 Result 노티용 routeKey: test_<configId>
function findTestResultByRouteKey(routeKey) {
  if (typeof routeKey !== 'string') return null;
  // 기존 URL 안내: /noti/result/test_<configId> → routeKey = "test_<configId>"
  let configId = null;
  if (routeKey.startsWith('test_')) {
    configId = routeKey.slice('test_'.length);
  } else if (routeKey.startsWith('result/test_')) {
    // 혹시나 과거 설정을 그대로 사용하는 경우도 함께 지원
    configId = routeKey.slice('result/test_'.length);
  }
  if (!configId) return null;
  const cfg = configId ? TEST_CONFIGS.get(configId) : null;
  if (!cfg || !cfg.useTestResultPage) return null;
  return { configId, config: cfg };
}

async function handleNotiRequest(routeKey, req, res) {
  const body = req.body;
  const rawBodyStr = req.rawBodyBuffer ? req.rawBodyBuffer.toString('utf8') : '';
  const incomingContentType = (req.get && req.get('Content-Type')) || (req.headers && req.headers['content-type']) || '';
  const refererOrOrigin = (req.get && req.get('Referer')) || (req.get && req.get('Origin')) || (req.headers && (req.headers['referer'] || req.headers['origin'])) || '';
  const host = (req.get && req.get('Host')) || (req.headers && req.headers['host']) || '';
  const apiKeyHeader = (req.get && req.get('ApiKey')) || (req.get && req.get('CHILLPAY-ApiKey')) || (req.headers && (req.headers['apikey'] || req.headers['chillpay-apikey'])) || '';
  const SANDBOX_APIKEY = 'fsL7o8du7F75fINcZ4yXxu8U9xHs33ieTWWJvXYdBkAVccuWX5w2CjUYc4o50L2N';
  const env =
    (String(refererOrOrigin) + ' ' + String(host)).toLowerCase().includes('sandbox') || String(apiKeyHeader).trim() === SANDBOX_APIKEY
      ? 'sandbox'
      : 'live';

  console.log('[수신] routeKey=', routeKey, 'Content-Type=', incomingContentType, 'body=', JSON.stringify(body));

  let match = findMerchantByRouteKey(routeKey);
  let testResultMatch = null;
  if (!match) {
    testResultMatch = findTestResultByRouteKey(routeKey);
    if (testResultMatch) {
      // 테스트 결제 Result 노티: 우리 테스트 결과 페이지로 릴레이 (가맹점 연동과 동일 방식)
      const baseUrl = req.protocol + '://' + (req.get('host') || req.hostname || '');
      const targetUrl = baseUrl + '/admin/test-pay/return';
      let relaySuccess = false;
      let relayFailReason = '';
      try {
        console.log('[포워딩 중] 테스트 결과 페이지로 릴레이:', targetUrl);
        const relayRes = await relayToMerchant(targetUrl, body, { contentType: incomingContentType, rawBody: rawBodyStr || undefined });
        const status = relayRes.status;
        // 2xx ~ 3xx(리다이렉트)까지는 성공으로 간주
        relaySuccess = status >= 200 && status < 400;
        if (relaySuccess) console.log('[포워딩 성공] 테스트 결과 페이지 status=', status);
        else relayFailReason = `HTTP ${status}`;
      } catch (err) {
        relayFailReason = err.code || err.message || String(err);
        console.error('[테스트 결과 페이지 릴레이 실패]', err.message);
      }
      const formatUsed = (incomingContentType || '').toLowerCase().includes('application/json') ? 'json' : 'raw';
      appendPgNotiLog({
        routeKey,
        merchantId: 'test_' + testResultMatch.configId,
        kind: 'result',
        body,
        rawBody: rawBodyStr || undefined,
        targetUrl,
        contentType: incomingContentType,
        env,
        relayStatus: relaySuccess ? 'ok' : 'fail',
        relayFailReason: relaySuccess ? '' : relayFailReason,
        relayFormatUsed: relaySuccess ? formatUsed : undefined,
      });
      return res.status(200).json({ ok: true, relay: relaySuccess });
    }
  }
  if (!match) {
    console.error('[에러] 등록되지 않은 routeKey:', routeKey);
    return res.status(404).json({ error: 'Route not found', routeKey });
  }

  const { merchantId, merchant, kind, targetUrl } = match;
  const enableRelay = merchant.enableRelay !== false;
  const enableInternal = merchant.enableInternal !== false;
  const enableDevInternal = merchant.enableDevInternal === true;

  let relaySuccess = false;
  let relayFailReason = '';
  const relayFormat = (merchant.relayFormat === 'json' || merchant.relayFormat === 'form') ? merchant.relayFormat : 'raw';
  let relayOpts;
  if (relayFormat === 'json') {
    relayOpts = { contentType: 'application/json', rawBody: undefined };
  } else if (relayFormat === 'form') {
    relayOpts = { contentType: 'application/x-www-form-urlencoded', rawBody: undefined };
  } else {
    relayOpts = { contentType: incomingContentType, rawBody: rawBodyStr || undefined };
  }
  if (enableRelay && targetUrl) {
    try {
      console.log('[포워딩 중] 가맹점으로 릴레이:', merchantId, kind, targetUrl, 'relayFormat=', relayFormat, 'Content-Type=', relayOpts.contentType);
      let relayRes = await relayToMerchant(targetUrl, body, relayOpts);
      relaySuccess = relayRes.status >= 200 && relayRes.status < 300;
      if (relaySuccess) {
        console.log('[포워딩 성공] status=', relayRes.status);
      } else {
        relayFailReason = `HTTP ${relayRes.status}` + (relayRes.data && typeof relayRes.data === 'string' ? ': ' + relayRes.data.slice(0, 200) : '');
        console.warn('[포워딩 실패] status=', relayRes.status, ' 1회 재시도 예정');
        await new Promise((r) => setTimeout(r, 2000));
        relayRes = await relayToMerchant(targetUrl, body, relayOpts);
        relaySuccess = relayRes.status >= 200 && relayRes.status < 300;
        if (relaySuccess) {
          console.log('[포워딩 재시도 성공] status=', relayRes.status);
          relayFailReason = '';
        } else {
          if (!relayFailReason) relayFailReason = `HTTP ${relayRes.status}`;
          console.warn('[포워딩 재시도 실패] status=', relayRes.status);
        }
      }
    } catch (err) {
      relayFailReason = err.code || err.message || String(err);
      console.error('[포워딩 실패]', err.message, ' 1회 재시도 예정');
      try {
        await new Promise((r) => setTimeout(r, 2000));
        const retryRes = await relayToMerchant(targetUrl, body, relayOpts);
        relaySuccess = retryRes.status >= 200 && retryRes.status < 300;
        if (relaySuccess) {
          console.log('[포워딩 재시도 성공]');
          relayFailReason = '';
        }
      } catch (err2) {
        if (!relayFailReason) relayFailReason = err2.code || err2.message || String(err2);
      }
    }
  } else {
    if (enableRelay && !targetUrl) relayFailReason = '가맹점 URL 없음';
    console.log('[포워딩 스킵] enableRelay=false 또는 targetUrl 없음');
  }

  // 로그 적재 (가맹점 수신 여부 포함, 최근 100건)
  // relayFormatUsed: 'raw' | 'json' | 'form' (성공 시에만 기록, 표시 색: 파란/녹/노랑)
  const formatUsed = enableRelay && relaySuccess ? relayFormat : undefined;
  appendPgNotiLog({
    routeKey,
    merchantId,
    kind,
    body,
    rawBody: rawBodyStr || undefined,
    targetUrl: enableRelay ? targetUrl || '' : '',
    contentType: incomingContentType,
    env,
    relayStatus: enableRelay ? (relaySuccess ? 'ok' : 'fail') : 'skip',
    relayFailReason: relaySuccess ? '' : (relayFailReason || ''),
    relayFormatUsed: formatUsed,
  });

  let internalDeliverySuccess = false;
  let internalTargetUrl = '';

  // 전산 전송 (실패해도 PG 응답에는 영향 없음)
  if (enableInternal) {
    try {
      const internalPayload = transformForInternal(body, merchant);
      let internalUrl = null;
      if (merchant.internalTargetId) {
        internalUrl = findInternalTargetUrl(merchant.internalTargetId, kind);
      }
      if (!internalUrl && INTERNAL_NOTI_URL) {
        internalUrl = INTERNAL_NOTI_URL;
      }

      if (!internalUrl) {
        console.log('[전산 전송 스킵] internalTargetId 또는 INTERNAL_NOTI_URL 미설정, merchant=', merchantId);
      } else {
        internalTargetUrl = internalUrl;
        console.log('[전산 전송 중] 내부 API로 가공 데이터 전송, merchant=', merchantId, 'url=', internalUrl);
        let internalRes = await sendToInternal(internalUrl, internalPayload);
        internalDeliverySuccess = internalRes.success;
        if (internalDeliverySuccess) {
          console.log('[전산 전송 완료]');
        } else {
          console.warn('[전산 전송 실패] status=', internalRes.status, ' 1회 재시도 예정');
          await new Promise((r) => setTimeout(r, 2000));
          internalRes = await sendToInternal(internalUrl, internalPayload);
          internalDeliverySuccess = internalRes.success;
          if (internalDeliverySuccess) console.log('[전산 전송 재시도 성공]');
          else console.warn('[전산 전송 재시도 실패]');
        }
      }
      appendInternalLog({
        storedAt: new Date().toISOString(),
        merchantId,
        routeNo: merchant.routeNo || '',
        internalTargetId: merchant.internalTargetId || '',
        payload: internalPayload,
        internalTargetUrl,
        internalDeliveryStatus: internalTargetUrl ? (internalDeliverySuccess ? 'ok' : 'fail') : 'skip',
      });
    } catch (err) {
      console.error('[전산 전송 실패]', err.message);
      let failUrl = internalTargetUrl;
      if (!failUrl && merchant.internalTargetId) failUrl = findInternalTargetUrl(merchant.internalTargetId, kind) || '';
      if (!failUrl && INTERNAL_NOTI_URL) failUrl = INTERNAL_NOTI_URL;
      let failPayload = body;
      try {
        failPayload = transformForInternal(body, merchant);
      } catch (_) {}
      appendInternalLog({
        storedAt: new Date().toISOString(),
        merchantId,
        routeNo: merchant.routeNo || '',
        internalTargetId: merchant.internalTargetId || '',
        payload: failPayload,
        internalTargetUrl: failUrl || '',
        internalDeliveryStatus: 'fail',
      });
    }
  } else {
    console.log('[전산 전송 스킵] enableInternal=false');
  }

  // 개발 전산 전송 (개발 노티 사용 시 별도 전송 및 로그)
  let devDeliverySuccess = false;
  let devInternalTargetUrl = '';

  if (enableDevInternal) {
    try {
      const devPayload = transformForDevInternal(body, merchant);
      let devUrl = null;
      if (merchant.internalTargetId) {
        devUrl = findInternalTargetUrl(merchant.internalTargetId, kind);
      }
      if (!devUrl && INTERNAL_NOTI_URL) {
        devUrl = INTERNAL_NOTI_URL;
      }

      if (!devUrl) {
        console.log('[개발 전산 전송 스킵] internalTargetId 또는 INTERNAL_NOTI_URL 미설정, merchant=', merchantId);
      } else {
        devInternalTargetUrl = devUrl;
        console.log('[개발 전산 전송 중] 개발 전산 시스템으로 가공 데이터 전송, merchant=', merchantId, 'url=', devUrl);
        let devRes = await sendToInternal(devUrl, devPayload);
        devDeliverySuccess = devRes.success;
        if (devDeliverySuccess) {
          console.log('[개발 전산 전송 완료]');
        } else {
          console.warn('[개발 전산 전송 실패] status=', devRes.status, ' 1회 재시도 예정');
          await new Promise((r) => setTimeout(r, 2000));
          devRes = await sendToInternal(devUrl, devPayload);
          devDeliverySuccess = devRes.success;
          if (devDeliverySuccess) console.log('[개발 전산 전송 재시도 성공]');
          else console.warn('[개발 전산 전송 재시도 실패]');
        }
      }
      appendDevInternalLog({
        storedAt: new Date().toISOString(),
        merchantId,
        routeNo: merchant.routeNo || '',
        internalTargetId: merchant.internalTargetId || '',
        payload: devPayload,
        internalTargetUrl: devInternalTargetUrl,
        internalDeliveryStatus: devInternalTargetUrl ? (devDeliverySuccess ? 'ok' : 'fail') : 'skip',
      });
    } catch (err) {
      console.error('[개발 전산 전송 실패]', err.message);
      let failUrl = devInternalTargetUrl;
      if (!failUrl && merchant.internalTargetId) failUrl = findInternalTargetUrl(merchant.internalTargetId, kind) || '';
      if (!failUrl && INTERNAL_NOTI_URL) failUrl = INTERNAL_NOTI_URL;
      let failPayload = body;
      try {
        failPayload = transformForDevInternal(body, merchant);
      } catch (_) {}
      appendDevInternalLog({
        storedAt: new Date().toISOString(),
        merchantId,
        routeNo: merchant.routeNo || '',
        internalTargetId: merchant.internalTargetId || '',
        payload: failPayload,
        internalTargetUrl: failUrl || '',
        internalDeliveryStatus: 'fail',
      });
    }
  } else {
    console.log('[개발 전산 전송 스킵] enableDevInternal=false');
  }

  res.status(200).json({ ok: true, relay: relaySuccess });
}

// ========== POST /noti/:routeKey (기존 형태 유지) ==========
// 예: /noti/rount_c1
app.post('/noti/:routeKey', async (req, res) => {
  const routeKey = req.params.routeKey;
  await handleNotiRequest(routeKey, req, res);
});

// ========== POST /noti/:kind/:no (신규: /noti/callback/1, /noti/result/1) ==========
app.post('/noti/:kind/:no', async (req, res) => {
  const { kind, no } = req.params;
  const routeKey = `${kind}/${no}`;
  await handleNotiRequest(routeKey, req, res);
});

// ========== 간단한 웹 관리 페이지 ==========

// 로그아웃
app.get('/admin/logout', (req, res) => {
  req.session.destroy(() => {});
  res.redirect('/admin/login');
});

// 언어 변경 (쿠키 저장 후 리다이렉트)
app.get('/admin/set-locale', (req, res) => {
  const lang = req.query.lang || 'ko';
  setLocaleCookie(res, lang);
  const back = req.query.back || req.get('Referer') || '/admin/login';
  res.redirect(back);
});

function getAdminSidebar(locale, adminUser, member, currentPath) {
  const site = loadSiteSettings();
  const pathMatch = (path) => currentPath && (currentPath === path || currentPath.startsWith(path + '?'));
  const link = (path, label) => {
    const cls = pathMatch(path) ? ' class="active"' : '';
    return `<a href="${path}"${cls}>${label}</a>`;
  };
  const langLinks = SUPPORTED_LOCALES.map((l) => {
    const label = l === 'zh' ? 'CH' : l.toUpperCase();
    return `<a href="/admin/set-locale?lang=${l}" style="color:#93c5fd;text-decoration:none;margin:0 2px;">${label}</a>`;
  }).join(' ');
  const role = member && member.role ? member.role : null;
  const canSeeMembers = role === ROLES.SUPER_ADMIN || role === ROLES.ADMIN;
  const perms = member && member.permissions ? member.permissions : PAGE_KEYS;
  const can = (key) => canSeeMembers || perms.includes(key);
  const sectionOpen = (paths) => paths.some((p) => pathMatch(p));
  // 왼쪽 메뉴: 섹션별로 접었다 펼치는 드롭다운 구조
  const navGroup = (sectionTitle, paths, itemsHtml) => {
    const open = sectionOpen(paths);
    return `<details class="nav-group"${open ? ' open' : ''}><summary class="nav-group-summary">${sectionTitle}</summary><div class="nav-group-items">${itemsHtml}</div></details>`;
  };
  const nav = [];
  if (can('merchants')) {
    nav.push(navGroup(t(locale, 'nav_merchant'), ['/admin/merchants'], link('/admin/merchants', t(locale, 'nav_merchant_settings'))));
  }
  if (can('pg_logs') || can('internal_logs') || can('dev_internal_logs') || can('pg_result') || can('internal_result') || can('dev_result') || can('traffic_analysis')) {
    const logPaths = ['/admin/logs-result', '/admin/internal-result', '/admin/dev-internal-result', '/admin/logs', '/admin/internal', '/admin/dev-internal', '/admin/traffic'];
    const logItems = [];
    if (can('pg_result')) logItems.push(link('/admin/logs-result', t(locale, 'nav_pg_result') || '피지 결과'));
    if (can('internal_result')) logItems.push(link('/admin/internal-result', t(locale, 'nav_internal_result') || '전산 결과'));
    if (can('dev_result')) logItems.push(link('/admin/dev-internal-result', t(locale, 'nav_dev_result') || '개발 결과'));
    if (can('pg_logs')) logItems.push(link('/admin/logs', t(locale, 'nav_pg_noti_log')));
    if (can('internal_logs')) logItems.push(link('/admin/internal', t(locale, 'nav_internal_noti_log')));
    if (can('dev_internal_logs')) logItems.push(link('/admin/dev-internal', t(locale, 'nav_dev_internal_noti_log') || '개발 노티로그'));
    if (can('traffic_analysis')) logItems.push(link('/admin/traffic', t(locale, 'nav_traffic_analysis') || '트래픽분석'));
    nav.push(navGroup(t(locale, 'nav_logs'), logPaths, logItems.join('')));
  }
  if (can('internal_targets') || can('internal_noti_settings') || can('dev_internal_noti_settings')) {
    const internalPaths = ['/admin/internal-targets', '/admin/internal-noti-settings', '/admin/dev-internal-noti-settings'];
    const internalItems = [];
    if (can('internal_targets')) internalItems.push(link('/admin/internal-targets', t(locale, 'nav_internal_targets')));
    if (can('internal_noti_settings')) internalItems.push(link('/admin/internal-noti-settings', t(locale, 'nav_internal_noti_settings')));
    if (can('dev_internal_noti_settings')) internalItems.push(link('/admin/dev-internal-noti-settings', t(locale, 'nav_dev_internal_noti_settings')));
    nav.push(navGroup(t(locale, 'nav_internal'), internalPaths, internalItems.join('')));
  }
  if (can('cancel_refund')) {
    const crPaths = ['/admin/transactions', '/admin/pg-transactions', '/admin/cancel-refund/cancel', '/admin/cancel-refund/void', '/admin/cancel-refund/force-void', '/admin/cancel-refund/refund', '/admin/cancel-refund/noti', '/admin/cancel-refund/void-deleted-list'];
    const crItems = [
      link('/admin/transactions', t(locale, 'nav_transaction_list')),
      // 피지거래내역은 기본 진입 시 항상 "당일"이 선택되도록 sort=today를 기본으로 붙인다.
      link('/admin/pg-transactions?sort=today', t(locale, 'nav_pg_transaction_list')),
      link('/admin/cancel-refund/cancel', t(locale, 'nav_cancel_refund_cancel')),
      link('/admin/cancel-refund/void', t(locale, 'nav_cancel_refund_void')),
      link('/admin/cancel-refund/force-void', t(locale, 'nav_cancel_refund_force_void')),
      link('/admin/cancel-refund/refund', t(locale, 'nav_cancel_refund_refund')),
      link('/admin/cancel-refund/noti', t(locale, 'nav_cancel_refund_noti')),
      link('/admin/cancel-refund/void-deleted-list', '삭제거래'),
    ];
    nav.push(navGroup(t(locale, 'nav_cancel_refund'), crPaths, crItems.join('')));
  }
  if (can('test_config') || can('test_run') || can('test_history')) {
    const testPaths = ['/admin/test-configs', '/admin/test-pay', '/admin/test-logs'];
    const testItems = [];
    if (can('test_config')) testItems.push(link('/admin/test-configs', t(locale, 'nav_test_config')));
    if (can('test_run')) testItems.push(link('/admin/test-pay', t(locale, 'nav_test_run')));
    if (can('test_history')) testItems.push(link('/admin/test-logs', t(locale, 'nav_test_history')));
    nav.push(navGroup(t(locale, 'nav_test'), testPaths, testItems.join('')));
  }
  if (canSeeMembers || can('settings') || can('account') || can('account_reset')) {
    const sysPaths = ['/admin/members', '/admin/account-reset', '/admin/settings', '/admin/account'];
    const sysItems = [];
    if (canSeeMembers) sysItems.push(link('/admin/members', t(locale, 'nav_account_manage')));
    if (can('account_reset')) sysItems.push(link('/admin/account-reset', t(locale, 'nav_account_reset')));
    if (can('settings')) sysItems.push(link('/admin/settings', t(locale, 'nav_settings')));
    if (can('account')) sysItems.push(link('/admin/account', t(locale, 'nav_account')));
    nav.push(navGroup(t(locale, 'nav_system'), sysPaths, sysItems.join('')));
  }
  const titleText = (site.sidebarTitle || '').replace(/</g, '&lt;').replace(/"/g, '&quot;') || DEFAULT_SIDEBAR_TITLE;
  const subText = (site.sidebarSub || '').replace(/</g, '&lt;').replace(/"/g, '&quot;') || DEFAULT_SIDEBAR_SUB;
  return `
    <aside class="sidebar">
      <a href="/admin/merchants" style="color:inherit;text-decoration:none;display:block;">
        <div class="sidebar-title" style="font-size:18px;">${titleText}</div>
        <div class="sidebar-sub" style="font-size:12px;">${subText}</div>
      </a>
      <div class="sidebar-user" style="font-size:13px;margin-top:18px;margin-bottom:18px;">${t(locale, 'user_label')}: ${adminUser || '-'}</div>
      <nav class="nav nav-github-style">${nav.join('')}</nav>
      <div style="margin-top:12px;font-size:12px;color:#9ca3af;">${t(locale, 'lang_switch')}: ${langLinks}</div>
    </aside>`;
}
function getAdminTopbar(locale, clientIp, nowLocal, nowTh, adminUser, currentPath) {
  const back = currentPath || '/admin/merchants';
  const langLinks = SUPPORTED_LOCALES.map((l) => {
    const label = l === 'zh' ? 'CH' : l.toUpperCase();
    return `<a href="/admin/set-locale?lang=${l}&back=${encodeURIComponent(back)}" style="color:#0369a1;text-decoration:none;margin:0 4px;">${label}</a>`;
  }).join(' ');
  const logoutLink = '<a href="/admin/logout" style="color:#0369a1;text-decoration:none;">로그아웃</a>';
  return `<div class="topbar">
    <span>${t(locale, 'topbar_ip')}: ${clientIp || '-'}</span>
    <span> ㅣ ${t(locale, 'topbar_time')}: ${nowLocal}</span>
    <span> ㅣ ${t(locale, 'topbar_time_th')}: ${nowTh}</span>
    <span style="margin-left:auto;">${t(locale, 'lang_switch')}: ${langLinks}</span>
    <span> ㅣ ${t(locale, 'user_label')}: ${adminUser || '-'}</span>
    <span> ㅣ ${logoutLink}</span>
  </div>`;
}

// 로그인 페이지 (다국어 + Google OTP 안내)
app.get('/admin/login', (req, res) => {
  const locale = getLocale(req);
  const err = String((req.query || {}).err || '');
  const u = String((req.query || {}).u || '');
  const langLinks = SUPPORTED_LOCALES.map((l) => {
    const label = l === 'zh' ? 'CH' : l.toUpperCase();
    return `<a href="/admin/set-locale?lang=${l}&back=/admin/login" style="color:#93c5fd;margin:0 4px;">${label}</a>`;
  }).join(' ');
  const escAttr = (s) => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const escJs = (s) => String(s == null ? '' : s).replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\r?\n/g, ' ');
  const remaining = Math.max(0, parseInt(String((req.query || {}).left || ''), 10) || 0);
  const remainingText = (left) => {
    if (!left) return '';
    if (locale === 'ja') return ` (残り${left}回)`;
    if (locale === 'en') return ` (${left} attempts left)`;
    if (locale === 'th') return ` (เหลือ ${left} ครั้ง)`;
    if (locale === 'zh') return `（剩余${left}次）`;
    return ` (${left}회 남음)`;
  };
  let errMsg = '';
  if (err === 'otp') errMsg = (t(locale, 'login_error_otp') || 'OTP 번호가 올바르지 않습니다.') + remainingText(remaining);
  if (err === 'otp_locked') errMsg = t(locale, 'login_error_otp_locked') || 'OTP가 5회 이상 틀려 초기화(잠금)되었습니다. 관리자에게 문의하세요.';
  if (err === 'cred') errMsg = t(locale, 'login_error_cred') || '아이디 또는 비밀번호가 올바르지 않습니다.';
  const alertScript = errMsg ? `<script>window.alert('${escJs(errMsg)}');</script>` : '';
  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'login_title')}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#111827; color:#f9fafb; }
    .container { max-width: 420px; margin: 80px auto; }
    .card { background:#111827; padding:22px 24px; border-radius:10px; box-shadow:0 10px 25px rgba(15,23,42,0.6); border:1px solid #1f2937; }
    h1 { margin-bottom: 8px; font-size:20px; color:#f9fafb; }
    label { display:block; margin-top:10px; font-size: 14px; color:#e5e7eb; }
    input[type="text"], input[type="password"] { width: 100%; padding: 8px 10px; margin-top:4px; box-sizing:border-box; border-radius:6px; border:1px solid #374151; background:#1f2937; color:#f9fafb; }
    input[type="text"]:focus, input[type="password"]:focus { outline:none; border-color:#60a5fa; box-shadow:0 0 0 1px #bfdbfe; }
    button { margin-top: 14px; padding: 8px 14px; background:#2563eb; color:#fff; border:none; border-radius:4px; cursor:pointer; font-size:14px; width:100%; }
    button:hover { background:#1d4ed8; }
    .hint { font-size:12px; color:#9ca3af; margin-top:6px; }
    .error { font-size:13px; color:#fca5a5; margin-bottom:6px; }
    .lang-bar { text-align:center; margin-bottom:12px; font-size:13px; color:#9ca3af; }
    .lang-bar a { text-decoration:none; }
  </style>
</head>
<body>
  <div class="container">
    <div class="lang-bar">${t(locale, 'lang_switch')}: ${langLinks}</div>
    <div class="card">
      <h1>${t(locale, 'login_title')}</h1>
      ${errMsg ? `<div class="error">${escAttr(errMsg)}</div>` : ''}
      <form method="post" action="/admin/login">
        <label>${t(locale, 'login_username')}<input type="text" name="username" value="${escAttr(u)}" required /></label>
        <label>${t(locale, 'login_password')}<input type="password" name="password" required /></label>
        <label>${t(locale, 'login_otp')}<input type="text" name="otp" maxlength="6" placeholder="000000" /></label>
        <div class="hint">${t(locale, 'login_otp_hint')}</div>
        <button type="submit">${t(locale, 'login_submit')}</button>
      </form>
      <p style="margin-top:14px;font-size:13px;"><a href="/admin/forgot" style="color:#93c5fd;">비밀번호 초기화 요청</a> &middot; <a href="/admin/forgot-id" style="color:#93c5fd;">아이디 찾기</a></p>
    </div>
  </div>
  ${alertScript}
</body>
</html>`);
});

// 아이디 찾기 (성명, 이메일, 국가 일치 시 아이디 안내 — 이메일 발송은 스텁)
app.get('/admin/forgot-id', (req, res) => {
  const locale = getLocale(req);
  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head><meta charset="UTF-8"/><title>아이디 찾기</title>
<style>body{font-family:system-ui;margin:0;background:#111827;color:#f9fafb;}.c{max-width:400px;margin:60px auto;padding:24px;}.card{background:#1f2937;padding:24px;border-radius:10px;}label{display:block;margin-top:12px;}input{width:100%;padding:10px;box-sizing:border-box;border-radius:6px;border:1px solid #4b5563;background:#111827;color:#f9fafb;}button{margin-top:16px;padding:10px;background:#2563eb;color:#fff;border:none;border-radius:6px;cursor:pointer;width:100%;}a{color:#93c5fd;}</style>
</head>
<body>
  <div class="c">
    <div class="card">
      <h1>아이디 찾기</h1>
      <p style="font-size:13px;color:#9ca3af;">가입 시 등록한 성명, 이메일, 국가가 일치하면 등록된 이메일로 아이디를 안내합니다.</p>
      <form method="post" action="/admin/forgot-id">
        <label>성명 <input type="text" name="name" required /></label>
        <label>이메일 <input type="email" name="email" required /></label>
        <label>국가 <input type="text" name="country" /></label>
        <button type="submit">확인</button>
      </form>
      <p style="margin-top:16px;"><a href="/admin/login">로그인</a></p>
    </div>
  </div>
</body>
</html>`);
});

app.post('/admin/forgot-id', (req, res) => {
  const { name, email, country } = req.body || {};
  MEMBERS = loadMembers();
  const mem = MEMBERS.find((m) => (m.name || '').trim() === (name || '').trim() && (m.email || '').trim().toLowerCase() === (email || '').trim().toLowerCase() && (m.country || '').trim() === (country || '').trim());
  if (!mem) {
    return res.status(400).send('일치하는 회원 정보가 없습니다. 성명, 이메일, 국가를 확인하세요. <a href="/admin/forgot-id">다시 시도</a>');
  }
  return res.send('등록된 이메일로 아이디를 발송했습니다. (테스트: 회원 아이디는 <strong>' + (mem.userId || '').replace(/</g, '&lt;') + '</strong> 입니다.) <a href="/admin/login">로그인</a>');
});

// 로그인 처리 (회원 기반: userId, password, OTP)
app.post('/admin/login', (req, res) => {
  const userId = (req.body.username || '').trim();
  const password = req.body.password || '';
  const otp = req.body.otp || '';
  const locale = getLocale(req);

  MEMBERS = loadMembers();
  const member = getMemberByUserId(userId);
  if (!member) {
    return res.redirect('/admin/login?err=cred&u=' + encodeURIComponent(userId));
  }

  const ok = bcrypt.compareSync(password, member.passwordHash);
  if (!ok) {
    return res.redirect('/admin/login?err=cred&u=' + encodeURIComponent(userId));
  }

  const isSuperAdmin = member.role === ROLES.SUPER_ADMIN;
  if (!isSuperAdmin && member.otpLocked) {
    return res.redirect('/admin/login?err=otp_locked&u=' + encodeURIComponent(userId));
  }

  if (member.otpRequired) {
    // OTP 필수인데 아직 secret이 없으면 로그인은 허용하되, 계정 설정에서 OTP를 먼저 등록하도록 강제
    if (!member.otpSecret) {
      OPERATOR_PERMISSIONS = loadOperatorPermissions();
      const permissions = member.role === ROLES.OPERATOR ? (OPERATOR_PERMISSIONS[member.userId] || []) : PAGE_KEYS;
      req.session.member = {
        id: member.id,
        role: member.role,
        userId: member.userId,
        name: member.name,
        canAssignPermission: member.canAssignPermission === true,
        permissions,
      };
      req.session.adminUser = member.userId;
      req.session.mustSetupOtp = true;
      return res.redirect('/admin/account?forceOtp=1');
    }

    if (!verifyOtp(member.otpSecret, otp)) {
      if (!isSuperAdmin) {
        MEMBERS = loadMembers();
        const m = getMemberByUserId(userId);
        if (m) {
          m.otpFailCount = Number.isFinite(m.otpFailCount) ? m.otpFailCount + 1 : 1;
          const left = Math.max(0, 5 - m.otpFailCount);
          if (m.otpFailCount >= 5) {
            m.otpLocked = true;
            // 자동 초기화(잠금): 기존 OTP 무효화
            m.otpSecret = '';
          }
          const idx = MEMBERS.findIndex((x) => x.id === m.id);
          if (idx >= 0) MEMBERS[idx] = m;
          saveMembers(MEMBERS);
          if (m.otpLocked) return res.redirect('/admin/login?err=otp_locked&u=' + encodeURIComponent(userId));
          return res.redirect('/admin/login?err=otp&left=' + left + '&u=' + encodeURIComponent(userId));
        }
      }
      return res.redirect('/admin/login?err=otp&left=0&u=' + encodeURIComponent(userId));
    }
  }

  // 로그인 성공 시 실패 카운트 초기화(슈퍼관리자 제외 규칙이지만, 슈퍼관리자는 별도 영향 없음)
  if (!isSuperAdmin && (member.otpFailCount || member.otpLocked)) {
    MEMBERS = loadMembers();
    const m = getMemberByUserId(userId);
    if (m) {
      m.otpFailCount = 0;
      m.otpLocked = false;
      const idx = MEMBERS.findIndex((x) => x.id === m.id);
      if (idx >= 0) MEMBERS[idx] = m;
      saveMembers(MEMBERS);
    }
  }

  OPERATOR_PERMISSIONS = loadOperatorPermissions();
  const permissions = member.role === ROLES.OPERATOR ? (OPERATOR_PERMISSIONS[member.userId] || []) : PAGE_KEYS;
  req.session.member = {
    id: member.id,
    role: member.role,
    userId: member.userId,
    name: member.name,
    canAssignPermission: member.canAssignPermission === true,
    permissions,
  };
  req.session.adminUser = member.userId;
  if (member.mustChangePassword) req.session.mustChangePassword = true;
  req.session.mustSetupOtp = false;

  if (member.mustChangePassword) {
    return res.redirect('/admin/change-password');
  }
  return res.redirect('/admin/merchants');
});

// 로그아웃
app.post('/admin/logout', (req, res) => {
  req.session.destroy(() => {
    res.redirect('/admin/login');
  });
});

// 최초 로그인 비밀번호 변경 (초기 비번 userId+1! 사용 후 새 비번 설정)
app.get('/admin/change-password', requireAuth, (req, res) => {
  const locale = getLocale(req);
  const member = req.session.member;
  if (!member || !req.session.mustChangePassword) {
    return res.redirect('/admin/merchants');
  }
  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'change_password_title')}</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 0; background:#111827; color:#f9fafb; }
    .container { max-width: 400px; margin: 60px auto; padding: 24px; }
    .card { background:#1f2937; padding: 24px; border-radius: 10px; border: 1px solid #374151; }
    h1 { font-size: 18px; margin-bottom: 16px; }
    label { display: block; margin-top: 12px; font-size: 14px; }
    input[type="password"] { width: 100%; padding: 10px; margin-top: 4px; box-sizing: border-box; border-radius: 6px; border: 1px solid #4b5563; background: #111827; color: #f9fafb; }
    button { margin-top: 16px; padding: 10px 16px; background: #2563eb; color: #fff; border: none; border-radius: 6px; cursor: pointer; width: 100%; font-size: 14px; }
    button:hover { background: #1d4ed8; }
    .error { color: #fca5a5; font-size: 13px; margin-top: 8px; }
  </style>
</head>
<body>
  <div class="container">
    <div class="card">
      <h1>${t(locale, 'change_password_title')}</h1>
      <p style="font-size:13px;color:#9ca3af;">${t(locale, 'change_password_desc')}</p>
      <form method="post" action="/admin/change-password">
        <label>${t(locale, 'change_password_new')}<input type="password" name="newPassword" required minlength="6" /></label>
        <label>${t(locale, 'change_password_confirm')}<input type="password" name="confirmPassword" required minlength="6" /></label>
        <button type="submit">${t(locale, 'change_password_submit')}</button>
      </form>
    </div>
  </div>
</body>
</html>`);
});

app.post('/admin/change-password', requireAuth, (req, res) => {
  const locale = getLocale(req);
  const { newPassword, confirmPassword } = req.body || {};
  const member = req.session.member;
  if (!member) return res.redirect('/admin/login');
  if (!req.session.mustChangePassword) return res.redirect('/admin/merchants');
  if (!newPassword || newPassword.length < 6) {
    return res.status(400).send(t(locale, 'change_password_too_short'));
  }
  if (newPassword !== confirmPassword) {
    return res.status(400).send(t(locale, 'change_password_mismatch'));
  }
  MEMBERS = loadMembers();
  const m = getMemberById(member.id);
  if (!m) return res.redirect('/admin/login');
  m.passwordHash = bcrypt.hashSync(newPassword, 10);
  m.mustChangePassword = false;
  const idx = MEMBERS.findIndex((x) => x.id === m.id);
  if (idx >= 0) MEMBERS[idx] = m;
  saveMembers(MEMBERS);
  req.session.mustChangePassword = false;
  return res.redirect('/admin/merchants');
});

// 로그인 성공 시 mustChangePassword 플래그 세션에 저장
function applyMemberSession(req, member) {
  OPERATOR_PERMISSIONS = loadOperatorPermissions();
  const permissions = member.role === ROLES.OPERATOR ? (OPERATOR_PERMISSIONS[member.userId] || []) : PAGE_KEYS;
  req.session.member = {
    id: member.id,
    role: member.role,
    userId: member.userId,
    name: member.name,
    canAssignPermission: member.canAssignPermission === true,
    permissions,
  };
  req.session.adminUser = member.userId;
  if (member.mustChangePassword) req.session.mustChangePassword = true;
}

// 관리자 계정 설정 페이지 (로그인한 회원 본인: 아이디/비밀번호/Google OTP)
app.get('/admin/account', requireAuth, requirePage('account'), async (req, res) => {
  const locale = getLocale(req);
  MEMBERS = loadMembers();
  const currentMember = getMemberById(req.session.member.id);
  if (!currentMember) return res.redirect('/admin/login');
  const hasOtp = !!(currentMember.otpSecret && currentMember.otpSecret.length > 0);
  const forceOtp = String((req.query || {}).forceOtp || '') === '1' || req.session.mustSetupOtp === true;
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });

  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'account_title')}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
    h1 { margin-bottom: 8px; }
    table { border-collapse: collapse; width: 100%; background:#ffffff; border-radius:8px; overflow:hidden; }
    th, td { border: 1px solid #e5e7eb; padding: 8px 10px; font-size: 14px; }
    th { background: #e5f0ff; text-align: center; color:#1f2937; }
    td { text-align: center; }
    tr:nth-child(even) { background:#f9fafb; }
    .actions-cell { text-align:center; }
    label { display:block; margin-top:10px; font-size: 14px; }
    input[type="text"], input[type="password"] { width: 100%; padding: 8px 10px; margin-top:4px; box-sizing:border-box; border-radius:6px; border:1px solid #d1d5db; background:#f9fafb; }
    input[type="text"]:focus, input[type="password"]:focus { outline:none; border-color:#60a5fa; box-shadow:0 0 0 1px #bfdbfe; background:#ffffff; }
    button { margin-top: 12px; padding: 9px 16px; background:#60a5fa; color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:14px; }
    button:hover { background:#3b82f6; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#ffffff; padding:18px 22px; border-radius:10px; box-shadow:0 10px 25px rgba(15,23,42,0.06); margin-bottom:8px; border:1px solid #e5e7eb; }
    .hint { font-size:12px; color:#6b7280; margin-top:6px; }
    .row { display:flex; justify-content:space-between; align-items:center; margin-top:10px; }
    .row span { font-size:13px; color:#374151; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card">
        <h1>${t(locale, 'account_title')}</h1>
        <p style="font-size:13px;color:#555;">${t(locale, 'account_desc')}</p>
        ${forceOtp ? `<div style="margin:10px 0 0;padding:10px 12px;border-radius:8px;background:#fef3c7;border:1px solid #f59e0b;color:#92400e;font-size:13px;">OTP 등록이 필요합니다. 아래에서 <strong>OTP 새로 생성</strong>을 체크한 뒤 저장하고, QR 코드를 Google OTP 앱에 등록하세요.</div>` : ''}
        <form method="post" action="/admin/account" onsubmit="return confirm('${(t(locale, 'merchants_confirm_save') || '저장(적용)하시겠습니까?').replace(/'/g, "\\'")}');">
          <label>${t(locale, 'account_new_username')}<input type="text" name="username" value="${(currentMember.userId || '').replace(/"/g, '&quot;')}" required /></label>
          <label>${t(locale, 'account_new_password')}<input type="password" name="password" /></label>
          <div class="row">
            <span>${t(locale, 'account_otp_status')}: ${hasOtp ? t(locale, 'account_otp_set') : t(locale, 'account_otp_not_set')}</span>
            <label style="margin:0;font-size:13px;"><input type="checkbox" name="resetOtp" />${t(locale, 'account_otp_regenerate')}</label>
          </div>
          <div class="hint">${t(locale, 'account_otp_hint')}</div>
          <button type="submit">${t(locale, 'account_save')}</button>
        </form>
      </div>
    </main>
  </div>
</body>
</html>`);
});

app.post('/admin/account', requireAuth, requirePage('account'), async (req, res) => {
  const locale = getLocale(req);
  const { username, password, resetOtp } = req.body;
  const actor = req.session.adminUser || 'unknown';
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  MEMBERS = loadMembers();
  const m = getMemberById(req.session.member.id);
  if (!m) return res.redirect('/admin/login');
  const before = { userId: m.userId, hasOtp: !!m.otpSecret };

  if (!username || !username.trim()) {
    return res.status(400).send(t(locale, 'account_username_required'));
  }

  m.userId = username.trim();
  if (password && password.trim().length > 0) {
    m.passwordHash = bcrypt.hashSync(password, 10);
  }

  if (resetOtp === 'on') {
    const secret = speakeasy.generateSecret({
      name: `Noti Admin (${m.userId})`,
    });
    m.otpSecret = secret.base32;
    m.otpFailCount = 0;
    m.otpLocked = false;
    const idx = MEMBERS.findIndex((x) => x.id === m.id);
    if (idx >= 0) MEMBERS[idx] = m;
    saveMembers(MEMBERS);

    const dataUrl = await QRCode.toDataURL(secret.otpauth_url);
    appendConfigChangeLog({
      type: 'admin_account_update',
      actor,
      clientIp,
      before,
      after: { userId: m.userId, hasOtp: true },
      resetOtp: true,
    });

    req.session.adminUser = m.userId;
    if (req.session.member) {
      req.session.member.userId = m.userId;
    }
    req.session.mustSetupOtp = false;
    return res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'account_otp_register_title')}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 24px; background:#f5f5f5; }
    .container { max-width: 520px; margin: 40px auto; text-align:center; }
    .card { background:#fff; padding:20px 22px; border-radius:8px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:24px; }
    h1 { margin-bottom: 8px; font-size:20px; }
    p { font-size:13px; color:#555; }
    code { background:#f3f4f6; padding:3px 6px; border-radius:4px; }
    button { margin-top: 14px; padding: 8px 14px; background:#2563eb; color:#fff; border:none; border-radius:4px; cursor:pointer; font-size:14px; }
    button:hover { background:#1d4ed8; }
  </style>
</head>
<body>
  <div class="container">
    <div class="card">
      <h1>${t(locale, 'account_otp_register_title')}</h1>
      <p>${t(locale, 'account_otp_register_desc')}</p>
      <img src="${dataUrl}" alt="OTP QR" />
      <p>${t(locale, 'account_otp_secret_label')}: <code>${m.otpSecret}</code></p>
      <form method="get" action="/admin/merchants">
        <button type="submit">${t(locale, 'account_go_merchants')}</button>
      </form>
    </div>
  </div>
</body>
</html>`);
  }

  const idx = MEMBERS.findIndex((x) => x.id === m.id);
  if (idx >= 0) MEMBERS[idx] = m;
  saveMembers(MEMBERS);
  appendConfigChangeLog({
    type: 'admin_account_update',
    actor,
    clientIp,
    before,
    after: { userId: m.userId, changedPassword: !!(password && password.trim()) },
    resetOtp: false,
  });
  req.session.adminUser = m.userId;
  // OTP 미설정 상태(otpRequired=true, otpSecret='')라면 mustSetupOtp 유지, 설정 완료 시 해제
  if (m.otpRequired && m.otpSecret) req.session.mustSetupOtp = false;
  return res.redirect('/admin/account');
});

// ----- 환경설정 (왼쪽 상단 노출 이름 / 추가 노출 문구) -----
app.get('/admin/settings', requireAuth, requirePage('settings'), (req, res) => {
  const locale = getLocale(req);
  const clientIp = (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() || req.ip || '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });
  const site = loadSiteSettings();
  const titleVal = (site.sidebarTitle || '').replace(/"/g, '&quot;');
  const subVal = (site.sidebarSub || '').replace(/"/g, '&quot;');
  const pageTitleVal = (site.pageTitle || '').replace(/"/g, '&quot;');
  const faviconVal = (site.favicon || '').replace(/"/g, '&quot;');
  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${(site.pageTitle || t(locale, 'nav_settings')).replace(/</g, '&lt;')}</title>
  ${site.favicon ? `<link rel="icon" href="${String(site.favicon).replace(/"/g, '&quot;')}" />` : ''}
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; color:#111827; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
    .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; margin-bottom:0; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:18px 22px; border-radius:10px; box-shadow:0 10px 25px rgba(15,23,42,0.06); border:1px solid #e5e7eb; }
    label { display:block; margin-top:12px; font-size:14px; }
    input[type="text"] { width:100%; max-width:400px; padding:10px 12px; margin-top:4px; box-sizing:border-box; border-radius:6px; border:1px solid #d1d5db; }
    button { margin-top:16px; padding:10px 18px; background:#2563eb; color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:14px; }
    button:hover { background:#1d4ed8; }
    .hint { font-size:12px; color:#6b7280; margin-top:6px; }
    .card-chillpay { margin-top: 18px; padding: 14px 18px; }
    .card-chillpay h2 { margin-bottom: 5px; font-size: 1.1rem; }
    .chillpay-desc { margin-bottom: 11px; color: #4b5563; font-size: 12px; line-height: 1.35; }
    .chillpay-section { margin-bottom: 18px; padding-bottom: 16px; border-bottom: 1px solid #e5e7eb; }
    .chillpay-section:last-of-type { border-bottom: none; }
    .chillpay-section h3 { margin: 0 0 5px 0; font-size: 0.95rem; color: #374151; }
    .chillpay-hint { font-size: 11px; color: #6b7280; margin: 0 0 7px 0; line-height: 1.35; }
    .chillpay-row { display: flex; flex-wrap: wrap; gap: 9px; align-items: flex-end; margin-bottom: 0; }
    .chillpay-row-md5 { margin-top: 8px; }
    .chillpay-cell { display: flex; flex-direction: column; margin-top: 0; }
    .chillpay-cell input { max-width: none; padding: 6px 8px; font-size: 13px; }
    .chillpay-cell-mid { flex: 0 0 auto; width: 10em; min-width: 8em; }
    .chillpay-cell-mid input { width: 100%; }
    .chillpay-cell-api { flex: 1; min-width: 120px; }
    .chillpay-cell-md5 { flex: 1; min-width: 100px; width: 100%; }
    .chillpay-time-th { color: #2563eb; font-weight: 600; }
    .chillpay-label { font-size: 11px; font-weight: 600; color: #374151; margin-bottom: 2px; }
    .chillpay-time-grid { display: flex; flex-wrap: wrap; gap: 16px 22px; margin-bottom: 0; }
    .chillpay-time-grid .chillpay-time-row { flex: 1; min-width: 260px; margin-bottom: 0; }
    .chillpay-time-grid .chillpay-time-row-duo { display: flex; gap: 16px; flex-wrap: wrap; flex: 1 1 100%; min-width: 0; }
    .chillpay-time-row-duo .chillpay-time-field { flex: 1; min-width: 260px; }
    .chillpay-time-row { margin-bottom: 9px; }
    .chillpay-time-field { display: flex; flex-wrap: wrap; align-items: baseline; gap: 5px 9px; }
    .chillpay-time-field .chillpay-label { display: block; width: 100%; margin-bottom: 2px; }
    .chillpay-time-inputs input { width: 52px; padding: 4px 6px; margin: 0 2px; box-sizing: border-box; font-size: 13px; }
    .chillpay-time-desc { font-size: 10px; color: #6b7280; line-height: 1.3; }
    .chillpay-time-table { width: 100%; border-collapse: collapse; margin-top: 6px; font-size: 12px; }
    .chillpay-time-table th, .chillpay-time-table td { padding: 8px 10px; text-align: center; vertical-align: middle; border: 1px solid #e5e7eb; }
    .chillpay-time-table th { background:#f9fafb; font-weight:600; }
    .chillpay-time-table .time-desc-cell { font-size:11px; color:#6b7280; text-align:center; line-height:1.5; }
    .chillpay-sandbox-check { margin-bottom: 12px; }
    .chillpay-checkbox-wrap { display: inline-flex; align-items: center; gap: 7px; cursor: pointer; font-weight: 600; font-size: 13px; }
    .chillpay-checkbox-desc { font-size: 11px; color: #6b7280; margin: 8px 0 0 0; padding: 7px 9px; background: #f3f4f6; border-radius: 6px; line-height: 1.35; }
    .chillpay-email-fixed-note { margin-bottom: 9px; }
    .chillpay-email-sender-row { display: flex; flex-wrap: wrap; gap: 11px; align-items: flex-end; margin-bottom: 9px; }
    .chillpay-email-sender-cell { flex: 0 0 auto; width: 220px; }
    .chillpay-email-sender-cell .chillpay-label { margin-bottom: 3px; font-size: 12px; }
    .chillpay-email-sender-cell input { width: 100%; box-sizing: border-box; padding: 5px 8px; border-radius: 6px; border: 1px solid #d1d5db; font-size: 13px; }
    .chillpay-email-input-same { width: 220px; max-width: 100%; box-sizing: border-box; padding: 5px 8px; border-radius: 6px; border: 1px solid #d1d5db; font-size: 13px; }
    .chillpay-email-to-row { margin-bottom: 8px; }
    .chillpay-email-to-row .chillpay-label { margin-bottom: 3px; font-size: 12px; }
    .chillpay-email-body-row { margin-top: 2px; }
    .chillpay-email-body-row .chillpay-label { margin-bottom: 3px; font-size: 12px; }
    .chillpay-email-body-row textarea { padding: 6px 8px; font-size: 13px; line-height: 1.35; }
    .chillpay-submit { margin-top: 6px; }
    .chillpay-submit button { padding: 7px 15px; font-size: 13px; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card">
        <h1>${t(locale, 'nav_settings')}</h1>
        <p class="hint">왼쪽 상단 노출 이름, 추가 문구, 브라우저 탭 제목, 파비콘(ico) URL을 설정합니다.</p>
        <form method="post" action="/admin/settings" onsubmit="return confirm('${(t(locale, 'merchants_confirm_save') || '저장(적용)하시겠습니까?').replace(/'/g, "\\'")}');">
          <label>왼쪽 상단 노출 이름 <input type="text" name="sidebarTitle" value="${titleVal}" placeholder="예: PG 노티 관리자" /></label>
          <label>추가 노출 문구 (한 줄) <input type="text" name="sidebarSub" value="${subVal}" placeholder="예: Webhooks &amp; Internal Notices" /></label>
          <label>브라우저 탭 제목 <input type="text" name="pageTitle" value="${pageTitleVal}" placeholder="접속 시 브라우저 상단에 표시되는 이름" /></label>
          <label>파비콘(ico) URL <input type="text" name="favicon" value="${faviconVal}" placeholder="예: https://example.com/favicon.ico" /></label>
          <button type="submit">저장</button>
        </form>
      </div>
      ${(function() {
        const c = loadChillPayTransactionConfig();
        const q = (v) => (v != null && typeof v === 'string' ? String(v).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '');
        // 수동 이메일 가능 시간(태국/일본) 자동 계산: 자동 무효 마감 시각 + 1분 ~ 다음날 23:59
        const pad2 = (n) => String(n).padStart(2, '0');
        const startMinTh = ((c.voidCutoffMinute ?? DEFAULT_VOID_CUTOFF_MINUTE) + 1) % 60;
        const carryHour = ((c.voidCutoffMinute ?? DEFAULT_VOID_CUTOFF_MINUTE) + 1) >= 60 ? 1 : 0;
        const startHourTh = (((c.voidCutoffHour ?? DEFAULT_VOID_CUTOFF_HOUR) + carryHour) + 24) % 24;
        const startLabelTh = pad2(startHourTh) + ':' + pad2(startMinTh);
        const startHourJp = (startHourTh + 2) % 24;
        const startLabelJp = pad2(startHourJp) + ':' + pad2(startMinTh);
        const endLabelTh = '23:59';
        const endLabelJp = '01:59';
        return '<div class="card card-chillpay"><h2>ChillPay 환경 설정</h2><p class="hint chillpay-desc">무효·환불 API 호출에 사용할 Sandbox / Production 키입니다. 수정 시 수정 버튼을 누른 뒤 변경하고, 저장 시 확인 후 저장됩니다. 본 데이터는 빈 값으로 덮어쓰지 않습니다.</p>'
          + '<form id="chillpay-cred-form" method="post" action="/admin/settings/chillpay-credentials" onsubmit="return confirm(\'저장하시겠습니까?\');">'
          + '<section class="chillpay-section"><h3>Sandbox (테스트 환경)</h3><p class="chillpay-hint">1줄: Mid · ApiKey / 2줄: MD5</p>'
          + '<div class="chillpay-row"><label class="chillpay-cell chillpay-cell-mid"><span class="chillpay-label">Mid</span><input type="text" name="sandboxMid" data-initial="' + q(c.sandbox.mid) + '" value="' + q(c.sandbox.mid) + '" placeholder="MerchantCode" maxlength="20" readonly /></label>'
          + '<label class="chillpay-cell chillpay-cell-api"><span class="chillpay-label">ApiKey</span><input type="text" name="sandboxApiKey" data-initial="' + q(c.sandbox.apiKey) + '" value="' + q(c.sandbox.apiKey) + '" placeholder="ApiKey" readonly /></label></div>'
          + '<div class="chillpay-row chillpay-row-md5"><label class="chillpay-cell chillpay-cell-md5"><span class="chillpay-label">MD5</span><input type="text" name="sandboxMd5" data-initial="' + q(c.sandbox.md5) + '" value="' + q(c.sandbox.md5) + '" placeholder="MD5 Secret Key" readonly /></label></div></section>'
          + '<section class="chillpay-section"><h3>Production (운영 환경)</h3><p class="chillpay-hint">1줄: Mid · ApiKey / 2줄: MD5</p>'
          + '<div class="chillpay-row"><label class="chillpay-cell chillpay-cell-mid"><span class="chillpay-label">Mid</span><input type="text" name="productionMid" data-initial="' + q(c.production.mid) + '" value="' + q(c.production.mid) + '" placeholder="MerchantCode" maxlength="20" readonly /></label>'
          + '<label class="chillpay-cell chillpay-cell-api"><span class="chillpay-label">ApiKey</span><input type="text" name="productionApiKey" data-initial="' + q(c.production.apiKey) + '" value="' + q(c.production.apiKey) + '" placeholder="ApiKey" readonly /></label></div>'
          + '<div class="chillpay-row chillpay-row-md5"><label class="chillpay-cell chillpay-cell-md5"><span class="chillpay-label">MD5</span><input type="text" name="productionMd5" data-initial="' + q(c.production.md5) + '" value="' + q(c.production.md5) + '" placeholder="MD5 Secret Key" readonly /></label></div></section>'
          + '<div class="chillpay-submit chillpay-cred-btns">'
          + '<button type="button" id="chillpay-cred-edit-btn" style="margin-right:8px;background:#eab308;color:#1f2937;border:none;padding:6px 14px;border-radius:6px;cursor:pointer;font-weight:600;">수정</button>'
          + '<button type="submit" id="chillpay-cred-save-btn" style="display:none;margin-right:8px;">저장</button>'
          + '<button type="button" id="chillpay-cred-cancel-btn" style="display:none;">취소</button>'
          + '</div></form>'
          + '<script>(function(){ var f=document.getElementById("chillpay-cred-form"); if(!f) return; var inps=f.querySelectorAll("input[type=\'text\']"); var editBtn=document.getElementById("chillpay-cred-edit-btn"); var saveBtn=document.getElementById("chillpay-cred-save-btn"); var cancelBtn=document.getElementById("chillpay-cred-cancel-btn"); editBtn.onclick=function(){ if(!confirm("수정하시겠습니까?")) return; inps.forEach(function(i){ i.removeAttribute("readonly"); }); editBtn.style.display="none"; saveBtn.style.display="inline-block"; cancelBtn.style.display="inline-block"; }; cancelBtn.onclick=function(){ inps.forEach(function(i){ var v=i.getAttribute("data-initial"); if(v!==null) i.value=v; i.setAttribute("readonly","readonly"); }); editBtn.style.display="inline-block"; saveBtn.style.display="none"; cancelBtn.style.display="none"; }; })();</script></div>'
          + '<div class="card card-chillpay"><h2>시간 및 동기화 설정</h2><p class="hint chillpay-desc">무효·환불 가능 시간(우리가 ChillPay API로 무효/환불 요청할 수 있는 구간)과 피지거래내역 동기화 주기입니다. 취소는 노티 수신만 있으므로 이 설정과 무관합니다.</p>'
          + '<form method="post" action="/admin/settings/chillpay-time" onsubmit="return confirm(\'시간 및 동기화 설정을 저장하시겠습니까?\');">'
          + '<section class="chillpay-section"><h3>무효/환불 가능 시간 (기준: 태국 시간)</h3><p class="chillpay-hint">무효/타임아웃 관련 구간은 <strong>TransactionDate</strong> 기준, 정산·환불 관련 기간은 <strong>PaymentDate</strong> 기준으로 판정됩니다. 모든 시각은 <strong>태국 시간(ICT)</strong> 기준이며, 화면에는 일본 시간(JST)도 함께 표기됩니다. (일본은 태국보다 2시간 빠릅니다.) 취소는 ChillPay 노티로만 수신되며, 우리가 API로 처리하는 것은 무효·환불뿐입니다.</p>'
          + '<table class="chillpay-time-table"><thead><tr><th>자동 무효 마감 (당일)</th><th>수동 이메일 가능 (당일)</th><th>환불 구간 시작 (다음날)</th><th>금액 표시 계산식</th></tr></thead><tbody>'
          + '<tr>'
          + '<td><input type="number" name="voidCutoffHour" min="0" max="23" value="' + c.voidCutoffHour + '" /> 시 <input type="number" name="voidCutoffMinute" min="0" max="59" value="' + c.voidCutoffMinute + '" /> 분</td>'
          + '<td><input type="text" value="23시 59분" readonly style="width:100%;padding:4px 6px;border:1px solid #e5e7eb;background:#f9fafb;color:#6b7280;font-size:12px;box-sizing:border-box;cursor:not-allowed;text-align:center;" /></td>'
          + '<td><input type="number" name="refundStartHour" min="0" max="23" value="' + c.refundStartHour + '" /> 시 <input type="number" name="refundStartMinute" min="0" max="59" value="' + c.refundStartMinute + '" /> 분</td>'
          + '<td><span class="chillpay-time-inputs"><select name="amountDisplayOp"><option value="*" ' + (c.amountDisplayOp === '*' ? 'selected' : '') + '>*</option><option value="/" ' + (c.amountDisplayOp === '/' ? 'selected' : '') + '>/</option><option value="+" ' + (c.amountDisplayOp === '+' ? 'selected' : '') + '>+</option><option value="-" ' + (c.amountDisplayOp === '-' ? 'selected' : '') + '>-</option></select>'
          + ' <input type="number" step="0.01" name="amountDisplayValue" value="' + (Number.isFinite(c.amountDisplayValue) ? c.amountDisplayValue : DEFAULT_AMOUNT_DISPLAY_VALUE) + '" /></span></td>'
          + '</tr>'
          + '<tr>'
          + '<td class="time-desc-cell">태국 기준. 기본 21:00<br />(일본 기준 23:00)</td>'
          + '<td class="time-desc-cell">태국 기준 23:59까지<br />(자동 계산, 수정 불가)<br />일본 기준 01:59</td>'
          + '<td class="time-desc-cell">태국 기준. 기본 00:00<br />(결제(PaymentDate) 다음날 0시부터<br />환불 가능 일자 계산 시작)</td>'
          + '<td class="time-desc-cell">Amount 원시 값에 계산식을 적용한 결과를<br />「금액」 컬럼에 표시합니다.<br />예: / 100 → 1000 → 10.00</td>'
          + '</tr>'
          + '</tbody></table>'
          + '<table class="chillpay-time-table" style="margin-top:10px;"><thead><tr>'
          + '<th>피지거래내역 동기화 주기 (분)</th><th>동기화 결과 상세 표시 시간 (분)</th><th>환불 가능 기간 (일)</th><th>피지거래내역 초기화 동기화 기간 (개월)</th>'
          + '</tr></thead><tbody><tr>'
          + '<td><input type="number" name="pgTransactionSyncIntervalMinutes" min="1" max="1440" value="' + c.pgTransactionSyncIntervalMinutes + '" /> 분</td>'
          + '<td><input type="number" name="syncResultDisplayMinutes" min="1" max="1440" value="' + c.syncResultDisplayMinutes + '" /> 분</td>'
          + '<td><input type="number" name="refundWindowDays" min="1" max="365" value="' + (c.refundWindowDays || '7') + '" /> 일</td>'
          + '<td><input type="number" name="pgTransactionInitialSyncMonths" min="1" max="60" value="' + c.pgTransactionInitialSyncMonths + '" /> 개월</td>'
          + '</tr><tr>'
          + '<td class="time-desc-cell">거래 내역 자동 조회 주기. 서버 재시작 시 적용</td>'
          + '<td class="time-desc-cell">무효/환불 동기화 후 상단 상세 표시 시간. 기본 30분</td>'
          + '<td class="time-desc-cell">결제(PaymentDate) 다음날 0시 기준, 이 기간 안에서만 환불 요청 버튼이 활성화됩니다. 기본 7일</td>'
          + '<td class="time-desc-cell">동기화 초기화 시 과거 몇 개월치. 기본 3개월</td>'
          + '</tr></tbody></table>'
          + '</section>'
          + '<input type="hidden" name="timezone" value="Asia/Bangkok" />'
          + '<div class="chillpay-submit"><button type="submit">저장</button></div></form></div>'
          + '<div class="card card-chillpay"><h2>자동화 이메일 설정</h2><p class="hint chillpay-desc">수동 무효 구간에서 ChillPay에 무효 요청을 위한 이메일 발송에 사용합니다. (취소는 노티 수신만 있으며, 무효·환불만 우리가 API/이메일로 요청 가능합니다.)</p>'
          + '<form method="post" action="/admin/settings/chillpay-email" onsubmit="return confirm(\'자동화 이메일 설정을 저장하시겠습니까?\');">'
          + '<section class="chillpay-section"><div class="chillpay-row chillpay-email-sender-row">'
          + '<label class="chillpay-cell chillpay-email-sender-cell"><span class="chillpay-label">보내는 사람 주소</span><input type="email" name="emailFrom" value="' + q(c.emailFrom) + '" placeholder="발신 이메일" /></label>'
          + '<label class="chillpay-cell chillpay-email-sender-cell"><span class="chillpay-label">회사명</span><input type="text" name="companyName" value="' + q(c.companyName) + '" placeholder="회사명" /></label>'
          + '<label class="chillpay-cell chillpay-email-sender-cell"><span class="chillpay-label">담당자 성명</span><input type="text" name="contactName" value="' + q(c.contactName) + '" placeholder="담당자 성명" /></label></div>'
          + '<div class="chillpay-email-to-row"><label class="chillpay-cell"><span class="chillpay-label">받는 분 (수신 이메일)</span><input type="email" name="emailTo" class="chillpay-email-input-same" value="' + q(c.emailTo || 'help@chillpay.co.th') + '" placeholder="help@chillpay.co.th" /></label></div>'
          + '<div class="chillpay-email-body-row"><label class="chillpay-cell" style="width:100%;"><span class="chillpay-label">내용 ({{transNo}}, {{orderNo}}, {{amount}}, {{routeNo}}, {{paymentDate}}, {{mid}} 자동 치환)</span>'
          + '<textarea name="emailBodyTemplate" rows="5" style="width:100%;max-width:600px;padding:8px;border-radius:6px;border:1px solid #d1d5db;box-sizing:border-box;">' + (c.emailBodyTemplate || '아래 거래에 대해 취소를 요청합니다.\n\nTransactionId(transNo): {{transNo}}\nOrderNo: {{orderNo}}\nAmount: {{amount}}\nRoute No. {{routeNo}}\nPaymentDate: {{paymentDate}}\nMID: {{mid}}\n').replace(/</g, '&lt;').replace(/"/g, '&quot;') + '</textarea></label></div></section>'
          + '<section class="chillpay-section chillpay-sandbox-check"><label class="chillpay-checkbox-wrap"><input type="checkbox" name="useSandbox" ' + (c.useSandbox ? 'checked' : '') + ' /><span>API 호출 시 Sandbox 사용 (테스트 시 체크)</span></label>'
          + '<p class="chillpay-checkbox-desc">체크 시 무효/환불 요청이 Sandbox로 전송됩니다. 해제 시 Production으로 전송됩니다.</p></section>'
          + '<div class="chillpay-submit"><button type="submit">저장</button></div></form></div>';
      })()}
    </main>
  </div>
</body>
</html>`);
});

// 빈 값으로 덮어쓰지 않음: 제출값이 비어 있으면 기존 값 유지
function keepIfNotEmpty(currentVal, submittedVal) {
  const cur = currentVal != null && typeof currentVal === 'string' ? currentVal : '';
  const sub = submittedVal != null ? String(submittedVal).trim() : '';
  return sub === '' ? cur : sub;
}

app.post('/admin/settings/chillpay-credentials', requireAuth, requirePage('settings'), (req, res) => {
  const cur = loadChillPayTransactionConfig();
  const sandboxMid = keepIfNotEmpty(cur.sandbox.mid, req.body && req.body.sandboxMid);
  const sandboxApiKey = keepIfNotEmpty(cur.sandbox.apiKey, req.body && req.body.sandboxApiKey);
  const sandboxMd5 = keepIfNotEmpty(cur.sandbox.md5, req.body && req.body.sandboxMd5);
  const productionMid = keepIfNotEmpty(cur.production.mid, req.body && req.body.productionMid);
  const productionApiKey = keepIfNotEmpty(cur.production.apiKey, req.body && req.body.productionApiKey);
  const productionMd5 = keepIfNotEmpty(cur.production.md5, req.body && req.body.productionMd5);
  saveChillPayTransactionConfig({
    sandbox: { mid: sandboxMid, apiKey: sandboxApiKey, md5: sandboxMd5 },
    production: { mid: productionMid, apiKey: productionApiKey, md5: productionMd5 },
  });
  return res.redirect('/admin/settings');
});

app.post('/admin/settings/chillpay-time', requireAuth, requirePage('settings'), (req, res) => {
  const voidCutoffHour = parseInt(req.body.voidCutoffHour, 10);
  const voidCutoffMinute = parseInt(req.body.voidCutoffMinute, 10);
  const refundStartHour = parseInt(req.body.refundStartHour, 10);
  const refundStartMinute = parseInt(req.body.refundStartMinute, 10);
  const syncResultDisplayMinutes = parseInt(req.body.syncResultDisplayMinutes, 10);
  const pgTransactionSyncIntervalMinutes = parseInt(req.body.pgTransactionSyncIntervalMinutes, 10);
  const pgTransactionInitialSyncMonths = parseInt(req.body.pgTransactionInitialSyncMonths, 10);
  const refundWindowDays = parseInt(req.body.refundWindowDays, 10);
  saveChillPayTransactionConfig({
    voidCutoffHour: Number.isFinite(voidCutoffHour) ? voidCutoffHour : DEFAULT_VOID_CUTOFF_HOUR,
    voidCutoffMinute: Number.isFinite(voidCutoffMinute) ? voidCutoffMinute : DEFAULT_VOID_CUTOFF_MINUTE,
    refundStartHour: Number.isFinite(refundStartHour) ? refundStartHour : DEFAULT_REFUND_START_HOUR,
    refundStartMinute: Number.isFinite(refundStartMinute) ? refundStartMinute : DEFAULT_REFUND_START_MINUTE,
    refundWindowDays: Number.isFinite(refundWindowDays) && refundWindowDays > 0 ? refundWindowDays : DEFAULT_REFUND_WINDOW_DAYS,
    syncResultDisplayMinutes: Number.isFinite(syncResultDisplayMinutes) && syncResultDisplayMinutes > 0 ? syncResultDisplayMinutes : 30,
    pgTransactionSyncIntervalMinutes: Number.isFinite(pgTransactionSyncIntervalMinutes) && pgTransactionSyncIntervalMinutes > 0 ? Math.min(1440, pgTransactionSyncIntervalMinutes) : 30,
    pgTransactionInitialSyncMonths: Number.isFinite(pgTransactionInitialSyncMonths) && pgTransactionInitialSyncMonths > 0 ? Math.min(60, pgTransactionInitialSyncMonths) : 3,
    timezone: (req.body && req.body.timezone != null) ? String(req.body.timezone).trim() : 'Asia/Tokyo',
  });
  return res.redirect('/admin/settings');
});

app.post('/admin/settings/chillpay-email', requireAuth, requirePage('settings'), (req, res) => {
  const useSandbox = req.body.useSandbox === 'on' || req.body.useSandbox === true;
  const emailFrom = (req.body && req.body.emailFrom != null) ? String(req.body.emailFrom).trim() : '';
  const companyName = (req.body && req.body.companyName != null) ? String(req.body.companyName).trim() : '';
  const contactName = (req.body && req.body.contactName != null) ? String(req.body.contactName).trim() : '';
  const emailTo = (req.body && req.body.emailTo != null) ? String(req.body.emailTo).trim() : 'help@chillpay.co.th';
  const emailBodyTemplate = (req.body && req.body.emailBodyTemplate != null) ? String(req.body.emailBodyTemplate).trim() : '';
  saveChillPayTransactionConfig({
    useSandbox,
    emailFrom,
    companyName,
    contactName,
    emailTo,
    emailBodyTemplate,
  });
  return res.redirect('/admin/settings');
});

app.post('/admin/settings', requireAuth, requirePage('settings'), (req, res) => {
  const sidebarTitle = (req.body && req.body.sidebarTitle != null) ? String(req.body.sidebarTitle).trim() : '';
  const sidebarSub = (req.body && req.body.sidebarSub != null) ? String(req.body.sidebarSub).trim() : '';
  const pageTitle = (req.body && req.body.pageTitle != null) ? String(req.body.pageTitle).trim() : '';
  const favicon = (req.body && req.body.favicon != null) ? String(req.body.favicon).trim() : '';
  saveSiteSettings({ sidebarTitle: sidebarTitle || undefined, sidebarSub, pageTitle, favicon });
  if (typeof appendConfigChangeLog === 'function') {
    appendConfigChangeLog({
      action: 'site_settings',
      detail: '환경설정(상단 노출/탭 제목/파비콘) 변경',
      actor: req.session.adminUser || 'unknown',
      clientIp: (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() || req.ip || '',
      payload: { sidebarTitle: sidebarTitle || loadSiteSettings().sidebarTitle, sidebarSub, pageTitle, favicon },
    });
  }
  return res.redirect('/admin/settings');
});

// ----- 회원 관리 (SUPER_ADMIN, ADMIN만 접근) -----
const requireMemberManage = [requireAuth, requireRole([ROLES.SUPER_ADMIN, ROLES.ADMIN])];

const PAGE_KEY_LABELS = {
  merchants: '가맹점추가',
  pg_logs: '피지 노티',
  internal_logs: '전산 노티',
  dev_internal_logs: '개발 노티',
  pg_result: '피지 결과',
  internal_result: '전산 결과',
  dev_result: '개발 결과',
  traffic_analysis: '트래픽분석',
  internal_targets: '노티 추가등록',
  internal_noti_settings: '노티 환경설정',
  dev_internal_noti_settings: '개발 환경설정',
  test_config: '테스트 설정',
  test_run: '테스트 실행',
  test_history: '테스트 내역',
  account: '계정 설정',
  settings: '환경 설정',
  account_reset: '계정초기화',
  cancel_refund: '종합거래',
};

function getRoleLabel(role, locale) {
  if (role === ROLES.SUPER_ADMIN) return t(locale, 'role_super_admin');
  if (role === ROLES.ADMIN) return t(locale, 'role_admin');
  if (role === ROLES.OPERATOR) return t(locale, 'role_operator');
  return role || '';
}

// 페이지 번호(1~18) 설명 문구 (상단 안내용) - 컴팩트 그리드
const PAGE_NUM_LEGEND = PAGE_KEYS.map((k, i) => `${i + 1}:${(PAGE_KEY_LABELS[k] || k).replace(/\s+/g, '')}`).join(' ');

app.get('/admin/members', requireMemberManage[0], requireMemberManage[1], (req, res) => {
  const locale = getLocale(req);
  const clientIp = (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() || req.ip || '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });
  MEMBERS = loadMembers();
  OPERATOR_PERMISSIONS = loadOperatorPermissions();
  const cur = req.session.member;
  const isSuper = cur && cur.role === ROLES.SUPER_ADMIN;
  const list = isSuper ? MEMBERS : MEMBERS.filter((x) => x.role === ROLES.OPERATOR);
  const addFormRoleBlock = isSuper ? `<label>역할 <select name="role" id="m-role"><option value="${ROLES.OPERATOR}">${t(locale, 'role_operator')}</option><option value="${ROLES.ADMIN}">${t(locale, 'role_admin')}</option></select></label>` : '<input type="hidden" name="role" id="m-role" value="OPERATOR" />';
  const addFormPermSlots = PAGE_KEYS.map((k) => {
    const name = (PAGE_KEY_LABELS[k] || k).replace(/</g, '&lt;').replace(/"/g, '&quot;');
    return `<div class="perm-slot"><span class="perm-slot-name" title="${name}">${name}</span><label class="perm-slot-cb"><input type="checkbox" name="perm_${k}" /></label></div>`;
  }).join('');
  const addFormPermBlock = `<div class="perm-grid-2x9">${addFormPermSlots}</div>`;
  const permHeaderCell = '<th class="perm-header-col">페이지 접근 권한 (OPERATOR)</th>';
  const confirmDel = (t(locale, 'members_confirm_delete') || '삭제하시겠습니까?').replace(/'/g, "\\'");
  const confirmPw = '비밀번호를 초기(아이디+1!)로 초기화합니다. 진행할까요?'.replace(/'/g, "\\'");
  const confirmOtp = 'OTP를 초기화합니다. 해당 계정은 재등록 후 사용 가능합니다. 진행할까요?'.replace(/'/g, "\\'");
  const rows = list
    .map((mem) => {
      const canEditInfo = isSuper || cur.role === ROLES.ADMIN;
      const canEditPerm = isSuper || (cur.role === ROLES.ADMIN && cur.canAssignPermission && mem.role === ROLES.OPERATOR);
      const canDelete = (isSuper || mem.role === ROLES.OPERATOR) && mem.id !== cur.id;
      const canReset = (isSuper || (mem.role === ROLES.OPERATOR && cur.role === ROLES.ADMIN)) && mem.id !== cur.id;
      const opPerms = mem.role === ROLES.OPERATOR ? (OPERATOR_PERMISSIONS[mem.userId] || []) : [];
      const permGridCells = PAGE_KEYS.map((k) => {
        const checked = opPerms.includes(k);
        const name = (PAGE_KEY_LABELS[k] || k).replace(/</g, '&lt;').replace(/"/g, '&quot;');
        if (mem.role === ROLES.OPERATOR) {
          if (canEditPerm) {
            return `<div class="perm-slot"><span class="perm-slot-name">${name}</span><label class="perm-slot-cb"><input type="checkbox" name="perm_${k}" ${checked ? 'checked' : ''} form="perm-form-${mem.id}" /></label></div>`;
          }
          return `<div class="perm-slot"><span class="perm-slot-name">${name}</span><span class="perm-slot-cb">${checked ? '●' : '-'}</span></div>`;
        }
        return `<div class="perm-slot perm-slot-full"><span class="perm-slot-name">${name}</span><span class="perm-slot-cb">●</span></div>`;
      }).join('');
      const permCell = mem.role === ROLES.OPERATOR
        ? `<td class="perm-td"><div class="perm-grid-2x9">${permGridCells}</div></td>`
        : `<td class="perm-td perm-td-full">전체</td>`;
      const confirmPerm = (t(locale, 'members_confirm_update_permissions') || '페이지 접근 권한을 수정하시겠습니까?').replace(/'/g, "\\'");
      const permForm = mem.role === ROLES.OPERATOR && canEditPerm
        ? `<form id="perm-form-${mem.id}" method="post" action="/admin/members/update-permissions" style="display:inline;" onsubmit="return confirm('${confirmPerm}');"><input type="hidden" name="id" value="${mem.id}" /><button type="submit" class="btn-update-perm">권한</button></form>`
        : '';
      const resetPwBtn = canReset ? `<form method="post" action="/admin/members/reset-password" style="display:inline;" onsubmit="return confirm('${confirmPw}');"><input type="hidden" name="id" value="${mem.id}" /><button type="submit" class="btn-reset-pw">비번</button></form>` : '-';
      const resetOtpBtn = canReset ? `<form method="post" action="/admin/members/reset-otp" style="display:inline;" onsubmit="return confirm('${confirmOtp}');"><input type="hidden" name="id" value="${mem.id}" /><button type="submit" class="btn-reset-otp">OTP</button></form>` : '-';
      const initCell = canReset ? `<td class="init-cell">${resetPwBtn} ${resetOtpBtn}</td>` : '<td class="init-cell">-</td>';
      const memberDataAttr = canEditInfo ? (() => {
        const o = { id: mem.id, name: mem.name || '', country: mem.country || '', userId: mem.userId || '', email: mem.email || '', birthDate: mem.birthDate || '', role: mem.role || '', perms: mem.role === ROLES.OPERATOR ? (OPERATOR_PERMISSIONS[mem.userId] || []) : [], otpRequired: !!mem.otpRequired, canAssignPermission: !!mem.canAssignPermission };
        return JSON.stringify(o).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      })() : '';
      const editInfoBtn = canEditInfo ? `<button type="button" class="btn-edit-info" data-member="${memberDataAttr}" title="상단 폼에 불러와 수정">정보</button>` : '-';
      const delBtn = canDelete ? `<form method="post" action="/admin/members/delete" style="display:inline;" onsubmit="return confirm('${confirmDel}');"><input type="hidden" name="id" value="${mem.id}" /><button type="submit" class="btn-del">${t(locale, 'delete_member')}</button></form>` : '-';
      return `<tr>
        <td>${(mem.name || '').replace(/</g, '&lt;')}</td>
        <td>${(mem.country || '').replace(/</g, '&lt;')}</td>
        <td>${(mem.userId || '').replace(/</g, '&lt;')}</td>
        <td>${(mem.email || '').replace(/</g, '&lt;')}</td>
        <td>${(mem.birthDate || '').replace(/</g, '&lt;')}</td>
        <td>${getRoleLabel(mem.role, locale)}</td>
        ${permCell}
        ${initCell}
        <td class="manage-cell">${permForm} ${editInfoBtn} ${delBtn}</td>
      </tr>`;
    })
    .join('');
  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'account_manage_title')}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; color:#111827; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
    .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; margin-bottom:0; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:18px 22px; border-radius:10px; box-shadow:0 10px 25px rgba(15,23,42,0.06); border:1px solid #e5e7eb; }
    h1 { margin-bottom:8px; }
    h2 { margin-top:20px; margin-bottom:12px; font-size:16px; }
    .members-table-wrap { overflow-x:auto; margin-bottom:8px; }
    table { border-collapse:collapse; width:100%; min-width:800px; background:#fff; border-radius:8px; }
    th, td { border:1px solid #e5e7eb; padding:6px 8px; font-size:13px; text-align:center; }
    th { background:#e5f0ff; color:#1f2937; }
    th:nth-child(1), td:nth-child(1) { min-width:70px; }
    th:nth-child(2), td:nth-child(2) { min-width:50px; }
    th:nth-child(3), td:nth-child(3) { min-width:80px; }
    th:nth-child(4), td:nth-child(4) { min-width:100px; }
    th:nth-child(5), td:nth-child(5) { min-width:80px; }
    th:nth-child(6), td:nth-child(6) { min-width:52px; }
    tr:nth-child(even) { background:#f9fafb; }
    .perm-header-col { min-width:320px; font-size:12px; padding:6px 8px; white-space:nowrap; }
    .perm-td { padding:6px 8px; vertical-align:top; min-width:320px; }
    .perm-td-full { font-size:12px; color:#6b7280; }
    .perm-grid-2x9 { display:grid; grid-template-columns:repeat(9, 1fr); gap:6px 10px; }
    .perm-slot { display:flex; flex-direction:column; align-items:center; padding:4px 2px; border:1px solid #e5e7eb; border-radius:6px; background:#f9fafb; font-size:11px; }
    .perm-slot-name { margin:2px 0; line-height:1.2; color:#6b7280; text-align:center; word-break:keep-all; }
    .perm-slot-cb input { margin:0; cursor:pointer; width:14px; height:14px; }
    .perm-slot-full .perm-slot-cb { font-size:12px; color:#059669; }
    .manage-cell { white-space:nowrap; }
    .manage-cell form { display:inline; margin-right:6px; }
    .init-cell { white-space:nowrap; }
    .init-cell form { display:inline; margin-right:4px; }
    .btn-update-perm { padding:4px 10px; font-size:12px; background:#2563eb; color:#fff; border:none; border-radius:6px; cursor:pointer; }
    .btn-update-perm:hover { background:#1d4ed8; }
    .btn-edit { display:inline-block; padding:4px 10px; font-size:12px; background:#eab308; color:#111827; border:none; border-radius:4px; cursor:pointer; text-decoration:none; margin-right:4px; }
    .btn-edit:hover { background:#ca8a04; color:#fff; }
    .btn-edit-info { padding:4px 10px; font-size:12px; background:#eab308; color:#111827; border:none; border-radius:4px; cursor:pointer; margin-right:4px; }
    .btn-edit-info:hover { background:#ca8a04; color:#fff; }
    .btn-del { padding:4px 8px; font-size:12px; background:#dc2626; color:#fff; border:none; border-radius:4px; cursor:pointer; }
    .btn-reset-pw { padding:4px 8px; font-size:12px; background:#6b7280; color:#fff; border:none; border-radius:4px; cursor:pointer; }
    .btn-reset-otp { padding:4px 8px; font-size:12px; background:#7c3aed; color:#fff; border:none; border-radius:4px; cursor:pointer; }
    label { display:block; margin-top:10px; font-size:14px; }
    .perm-legend { font-size:11px; color:#6b7280; background:#f9fafb; border:1px solid #e5e7eb; border-radius:6px; padding:6px 10px; margin-bottom:10px; line-height:1.5; }
    .perm-legend-inner { display:grid; grid-template-columns:repeat(auto-fill, minmax(100px, 1fr)); gap:2px 10px; }
    .add-form-grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(200px, 1fr)); gap:12px 20px; align-items:end; }
    .add-form-grid label { margin-top:0; }
    input[type="text"], input[type="email"], input[type="date"], select { width:100%; max-width:280px; padding:8px 10px; border-radius:6px; border:1px solid #d1d5db; box-sizing:border-box; }
    #add-form-perms .perm-grid-2x9 { margin-top:8px; max-width:100%; }
    #add-form-perms .perm-slot { min-width:0; }
    button[type="submit"].btn-add { padding:10px 18px; background:#2563eb; color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:14px; margin-top:12px; }
    button[type="submit"].btn-add:hover { background:#1d4ed8; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card">
        <h1>${t(locale, 'account_manage_title')}</h1>
        <p style="font-size:13px;color:#555;">${t(locale, 'account_list_desc')}</p>
        ${(isSuper || cur.role === ROLES.ADMIN) ? `
        <h2 id="form-title" data-initial-title="${(t(locale, 'add_member') || '회원 추가').replace(/"/g, '&quot;')}">${t(locale, 'add_member')}</h2>
        <form id="member-form" method="post" action="/admin/members/save" class="add-form-grid" onsubmit="return confirm('${(t(locale, 'merchants_confirm_save') || '저장(적용)하시겠습니까?').replace(/'/g, "\\'")}');">
          <input type="hidden" name="editId" id="editId" value="" />
          <label>${t(locale, 'member_name')} <input type="text" name="name" id="m-name" required /></label>
          <label>${t(locale, 'member_country')} <input type="text" name="country" id="m-country" /></label>
          <label>${t(locale, 'member_user_id')} <input type="text" name="userId" id="m-userId" required /></label>
          <label>${t(locale, 'member_email')} <input type="email" name="email" id="m-email" /></label>
          <label>${t(locale, 'member_birth_date')} <input type="date" name="birthDate" id="m-birthDate" /></label>
          ${addFormRoleBlock}
          ${isSuper ? '<label><input type="checkbox" name="canAssignPermission" id="m-canAssignPermission" /> 운영자 권한 부여 가능 (ADMIN용)</label><label><input type="checkbox" name="otpRequired" id="m-otpRequired" /> OTP 로그인 필수 지정</label>' : ''}
          <label style="grid-column:1/-1;">${t(locale, 'page_permissions')} (OPERATOR만 해당) <div id="add-form-perms">${addFormPermBlock}</div></label>
          <label style="grid-column:1/-1;"><button type="submit" class="btn-add">${t(locale, 'members_save')}</button> <button type="button" id="btn-cancel-edit" style="display:none;padding:10px 18px;background:#6b7280;color:#fff;border:none;border-radius:6px;cursor:pointer;font-size:14px;margin-left:8px;">취소(새로 등록)</button></label>
        </form>
        ` : ''}
      </div>
      <div class="card">
        <h2>${t(locale, 'registered_accounts')}</h2>
        <div class="members-table-wrap"><table>
          <thead><tr><th>${t(locale, 'member_name')}</th><th>${t(locale, 'member_country')}</th><th>${t(locale, 'member_user_id')}</th><th>${t(locale, 'member_email')}</th><th>${t(locale, 'member_birth_date')}</th><th>역할</th>${permHeaderCell}<th>초기화</th><th>관리</th></tr></thead>
          <tbody>${rows}</tbody>
        </table></div>
      </div>
    </main>
  </div>
  <script>
    (function(){
      var form = document.getElementById('member-form');
      var editId = document.getElementById('editId');
      var btnCancel = document.getElementById('btn-cancel-edit');
      var formTitle = document.getElementById('form-title');
      if (!form) return;
      var permKeys = ['merchants','pg_logs','internal_logs','dev_internal_logs','pg_result','internal_result','dev_result','traffic_analysis','internal_targets','internal_noti_settings','dev_internal_noti_settings','test_config','test_run','test_history','account','settings','account_reset','cancel_refund'];
      function parseDataAttr(s) {
        if (!s) return null;
        try { return JSON.parse(s.replace(/&quot;/g, '"').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')); } catch(e) { return null; }
      }
      function fillForm(m) {
        var nameEl = document.getElementById('m-name');
        var countryEl = document.getElementById('m-country');
        var userIdEl = document.getElementById('m-userId');
        var emailEl = document.getElementById('m-email');
        var birthEl = document.getElementById('m-birthDate');
        var roleEl = document.getElementById('m-role');
        if (nameEl) nameEl.value = m.name || '';
        if (countryEl) countryEl.value = m.country || '';
        if (userIdEl) userIdEl.value = m.userId || '';
        if (emailEl) emailEl.value = m.email || '';
        if (birthEl) birthEl.value = m.birthDate || '';
        if (roleEl) { roleEl.value = m.role || ''; if (roleEl.tagName === 'SELECT') roleEl.selectedIndex = m.role === 'ADMIN' ? 1 : 0; }
        var otpEl = document.getElementById('m-otpRequired');
        var assignEl = document.getElementById('m-canAssignPermission');
        if (otpEl) otpEl.checked = !!m.otpRequired;
        if (assignEl) assignEl.checked = !!m.canAssignPermission;
        if (editId) editId.value = m.id || '';
        var perms = m.perms || [];
        permKeys.forEach(function(k){
          var cb = form.querySelector('[name="perm_'+k+'"]');
          if (cb) cb.checked = perms.indexOf(k) !== -1;
        });
        if (btnCancel) btnCancel.style.display = 'inline-block';
        if (formTitle) formTitle.textContent = '계정 수정: ' + (m.userId || '');
        form.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
      function clearEditMode() {
        if (editId) editId.value = '';
        if (btnCancel) btnCancel.style.display = 'none';
        if (formTitle) formTitle.textContent = formTitle.getAttribute('data-initial-title') || '회원 추가';
        form.reset();
        permKeys.forEach(function(k){ var cb = form.querySelector('[name="perm_'+k+'"]'); if (cb) cb.checked = false; });
      }
      document.querySelectorAll('.btn-edit-info').forEach(function(btn){
        btn.addEventListener('click', function(){
          if (!confirm('수정 하시겠습니까?')) return;
          var m = parseDataAttr(this.getAttribute('data-member'));
          if (m) fillForm(m);
        });
      });
      if (btnCancel) btnCancel.addEventListener('click', clearEditMode);
    })();
  </script>
</body>
</html>`);
});

app.get('/admin/members/add', requireMemberManage[0], requireMemberManage[1], (req, res) => {
  return res.redirect('/admin/members');
});

app.post('/admin/members/add', requireMemberManage[0], requireMemberManage[1], (req, res) => {
  const locale = getLocale(req);
  const { name, country, userId, email, birthDate, role } = req.body || {};
  const cur = req.session.member;
  const isSuper = cur && cur.role === ROLES.SUPER_ADMIN;
  const assignRole = (role === ROLES.ADMIN && isSuper) ? ROLES.ADMIN : ROLES.OPERATOR;
  if (!userId || !userId.trim()) return res.status(400).send('userId required');
  if (getMemberByUserId(userId.trim())) return res.status(400).send('Already exists: ' + userId.trim());
  const initialPassword = userId.trim() + INITIAL_PASSWORD_SUFFIX;
  const member = {
    id: 'member-' + Date.now(),
    role: assignRole,
    name: (name || '').trim(),
    country: (country || '').trim(),
    userId: userId.trim(),
    email: (email || '').trim(),
    birthDate: (birthDate || '').trim(),
    passwordHash: bcrypt.hashSync(initialPassword, 10),
    otpSecret: '',
    otpRequired: false,
    canAssignPermission: false,
    mustChangePassword: true,
    createdAt: new Date().toISOString(),
  };
  MEMBERS = loadMembers();
  MEMBERS.push(member);
  saveMembers(MEMBERS);
  const perms = [];
  PAGE_KEYS.forEach((k) => {
    if (req.body['perm_' + k] === 'on') perms.push(k);
  });
  if (assignRole === ROLES.OPERATOR && perms.length) {
    OPERATOR_PERMISSIONS = loadOperatorPermissions();
    OPERATOR_PERMISSIONS[member.userId] = perms;
    saveOperatorPermissions(OPERATOR_PERMISSIONS);
  }
  return res.redirect('/admin/members');
});

app.post('/admin/members/save', requireMemberManage[0], requireMemberManage[1], (req, res) => {
  const locale = getLocale(req);
  const body = req.body || {};
  const editId = (body.editId || '').trim();
  const cur = req.session.member;
  const isSuper = cur && cur.role === ROLES.SUPER_ADMIN;
  if (editId) {
    MEMBERS = loadMembers();
    const mem = getMemberById(editId);
    if (!mem) return res.status(404).send('Not found');
    if (!isSuper && mem.role !== ROLES.OPERATOR) return res.status(403).send('Forbidden');
    const { name, country, userId, email, birthDate, canAssignPermission } = body;
    mem.name = (name || '').trim();
    mem.country = (country || '').trim();
    mem.userId = (userId || '').trim();
    mem.email = (email || '').trim();
    mem.birthDate = (birthDate || '').trim();
    if (isSuper && mem.role === ROLES.ADMIN && body.hasOwnProperty('canAssignPermission')) mem.canAssignPermission = canAssignPermission === 'on';
    if (isSuper && body.hasOwnProperty('otpRequired')) mem.otpRequired = body.otpRequired === 'on';
    const idx = MEMBERS.findIndex((x) => x.id === mem.id);
    if (idx >= 0) MEMBERS[idx] = mem;
    saveMembers(MEMBERS);
    const perms = [];
    PAGE_KEYS.forEach((k) => { if (body['perm_' + k] === 'on') perms.push(k); });
    if (mem.role === ROLES.OPERATOR) {
      OPERATOR_PERMISSIONS = loadOperatorPermissions();
      OPERATOR_PERMISSIONS[mem.userId] = perms;
      saveOperatorPermissions(OPERATOR_PERMISSIONS);
    }
    return res.redirect('/admin/members');
  }
  const { name, country, userId, email, birthDate, role } = body;
  const assignRole = (role === ROLES.ADMIN && isSuper) ? ROLES.ADMIN : ROLES.OPERATOR;
  if (!userId || !userId.trim()) return res.status(400).send('userId required');
  if (getMemberByUserId(userId.trim())) return res.status(400).send('Already exists: ' + userId.trim());
  const initialPassword = userId.trim() + INITIAL_PASSWORD_SUFFIX;
  const member = {
    id: 'member-' + Date.now(),
    role: assignRole,
    name: (name || '').trim(),
    country: (country || '').trim(),
    userId: userId.trim(),
    email: (email || '').trim(),
    birthDate: (birthDate || '').trim(),
    passwordHash: bcrypt.hashSync(initialPassword, 10),
    otpSecret: '',
    otpRequired: false,
    otpFailCount: 0,
    otpLocked: false,
    canAssignPermission: false,
    mustChangePassword: true,
    createdAt: new Date().toISOString(),
  };
  MEMBERS = loadMembers();
  MEMBERS.push(member);
  saveMembers(MEMBERS);
  const perms = [];
  PAGE_KEYS.forEach((k) => { if (body['perm_' + k] === 'on') perms.push(k); });
  if (assignRole === ROLES.OPERATOR && perms.length) {
    OPERATOR_PERMISSIONS = loadOperatorPermissions();
    OPERATOR_PERMISSIONS[member.userId] = perms;
    saveOperatorPermissions(OPERATOR_PERMISSIONS);
  }
  return res.redirect('/admin/members');
});

app.get('/admin/members/edit/:id', requireMemberManage[0], requireMemberManage[1], (req, res) => {
  return res.redirect('/admin/members');
});

app.post('/admin/members/reset-password', requireMemberManage[0], requireMemberManage[1], (req, res) => {
  const id = (req.body || {}).id;
  if (!id) return res.status(400).send('id required');
  MEMBERS = loadMembers();
  const mem = getMemberById(id);
  if (!mem) return res.status(404).send('Not found');
  const cur = req.session.member;
  const isSuper = cur.role === ROLES.SUPER_ADMIN;
  if (!isSuper && mem.role !== ROLES.OPERATOR) return res.status(403).send('Forbidden');
  mem.passwordHash = bcrypt.hashSync(mem.userId + INITIAL_PASSWORD_SUFFIX, 10);
  mem.mustChangePassword = true;
  const idx = MEMBERS.findIndex((x) => x.id === mem.id);
  if (idx >= 0) MEMBERS[idx] = mem;
  saveMembers(MEMBERS);
  return res.redirect('/admin/members');
});

app.post('/admin/members/reset-otp', requireMemberManage[0], requireMemberManage[1], (req, res) => {
  const id = (req.body || {}).id;
  if (!id) return res.status(400).send('id required');
  MEMBERS = loadMembers();
  const mem = getMemberById(id);
  if (!mem) return res.status(404).send('Not found');
  const cur = req.session.member;
  const isSuper = cur.role === ROLES.SUPER_ADMIN;
  if (!isSuper && mem.role !== ROLES.OPERATOR) return res.status(403).send('Forbidden');
  mem.otpSecret = '';
  mem.otpFailCount = 0;
  mem.otpLocked = false;
  const idx = MEMBERS.findIndex((x) => x.id === mem.id);
  if (idx >= 0) MEMBERS[idx] = mem;
  saveMembers(MEMBERS);
  return res.redirect('/admin/members');
});

app.post('/admin/members/update-permissions', requireMemberManage[0], requireMemberManage[1], (req, res) => {
  const id = (req.body || {}).id;
  if (!id) return res.status(400).send('id required');
  MEMBERS = loadMembers();
  const mem = getMemberById(id);
  if (!mem) return res.status(404).send('Not found');
  const cur = req.session.member;
  const isSuper = cur.role === ROLES.SUPER_ADMIN;
  const canAssign = isSuper || (cur.role === ROLES.ADMIN && cur.canAssignPermission === true);
  if (!canAssign || mem.role !== ROLES.OPERATOR) return res.status(403).send('Forbidden');
  if (mem.role !== ROLES.OPERATOR) return res.redirect('/admin/members');
  const perms = [];
  PAGE_KEYS.forEach((k) => {
    if (req.body['perm_' + k] === 'on') perms.push(k);
  });
  OPERATOR_PERMISSIONS = loadOperatorPermissions();
  OPERATOR_PERMISSIONS[mem.userId] = perms;
  saveOperatorPermissions(OPERATOR_PERMISSIONS);
  return res.redirect('/admin/members');
});

app.post('/admin/members/edit/:id', requireMemberManage[0], requireMemberManage[1], (req, res) => {
  const id = req.params.id;
  MEMBERS = loadMembers();
  const mem = getMemberById(id);
  if (!mem) return res.status(404).send('Not found');
  const cur = req.session.member;
  const isSuper = cur.role === ROLES.SUPER_ADMIN;
  if (!isSuper && mem.role !== ROLES.OPERATOR) return res.status(403).send('Forbidden');
  const { name, country, userId, email, birthDate, canAssignPermission } = req.body || {};
  mem.name = (name || '').trim();
  mem.country = (country || '').trim();
  mem.userId = (userId || '').trim();
  mem.email = (email || '').trim();
  mem.birthDate = (birthDate || '').trim();
  if (isSuper && mem.role === ROLES.ADMIN) mem.canAssignPermission = canAssignPermission === 'on';
  if (isSuper) mem.otpRequired = (req.body || {}).otpRequired === 'on';
  const idx = MEMBERS.findIndex((x) => x.id === mem.id);
  if (idx >= 0) MEMBERS[idx] = mem;
  saveMembers(MEMBERS);
  const perms = [];
  PAGE_KEYS.forEach((k) => {
    if (req.body['perm_' + k] === 'on') perms.push(k);
  });
  if (mem.role === ROLES.OPERATOR) {
    OPERATOR_PERMISSIONS = loadOperatorPermissions();
    OPERATOR_PERMISSIONS[mem.userId] = perms;
    saveOperatorPermissions(OPERATOR_PERMISSIONS);
  }
  return res.redirect('/admin/members');
});

app.post('/admin/members/delete', requireMemberManage[0], requireMemberManage[1], (req, res) => {
  const id = (req.body || {}).id;
  if (!id) return res.status(400).send('id required');
  MEMBERS = loadMembers();
  const mem = getMemberById(id);
  if (!mem) return res.status(404).send('Not found');
  const cur = req.session.member;
  if (cur.id === mem.id) return res.status(400).send('Cannot delete self');
  const isSuper = cur.role === ROLES.SUPER_ADMIN;
  if (!isSuper && mem.role !== ROLES.OPERATOR) return res.status(403).send('Forbidden');
  MEMBERS = MEMBERS.filter((x) => x.id !== id);
  saveMembers(MEMBERS);
  OPERATOR_PERMISSIONS = loadOperatorPermissions();
  delete OPERATOR_PERMISSIONS[mem.userId];
  saveOperatorPermissions(OPERATOR_PERMISSIONS);
  return res.redirect('/admin/members');
});

// 기존 URL 리다이렉트
app.get('/admin/password-reset-requests', requireAuth, (req, res) => res.redirect('/admin/account-reset'));

// 계정초기화 (비번/OTP 초기화 요청 처리, 권한 account_reset 필요)
app.get('/admin/account-reset', requireAuth, requirePage('account_reset'), (req, res) => {
  const locale = getLocale(req);
  const clientIp = (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() || req.ip || '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });
  const requests = loadPasswordResetRequests();
  const pwRequests = requests.filter((r) => (r.type || 'password') === 'password');
  const otpRequests = requests.filter((r) => r.type === 'otp');
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const confirmApprove = (t(locale, 'account_reset_confirm_approve') || '승인하시겠습니까?').replace(/'/g, "\\'");
  const pwRows = pwRequests.map((r) => {
    const mem = getMemberByUserId(r.userId);
    const name = mem ? mem.name : '-';
    return `<tr><td>${esc(r.userId)}</td><td>${esc(name)}</td><td>${esc(r.requestedAt)}</td><td><form method="post" action="/admin/account-reset/approve" style="display:inline;" onsubmit="return confirm('${confirmApprove}');"><input type="hidden" name="userId" value="${esc(r.userId).replace(/"/g, '&quot;')}" /><input type="hidden" name="type" value="password" /><button type="submit" class="btn-approve">승인</button></form></td></tr>`;
  }).join('');
  const otpRows = otpRequests.map((r) => {
    const mem = getMemberByUserId(r.userId);
    const name = mem ? mem.name : '-';
    return `<tr><td>${esc(r.userId)}</td><td>${esc(name)}</td><td>${esc(r.requestedAt)}</td><td><form method="post" action="/admin/account-reset/approve" style="display:inline;" onsubmit="return confirm('${confirmApprove}');"><input type="hidden" name="userId" value="${esc(r.userId).replace(/"/g, '&quot;')}" /><input type="hidden" name="type" value="otp" /><button type="submit" class="btn-approve">승인</button></form></td></tr>`;
  }).join('');
  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'nav_account_reset')}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; color:#111827; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; margin-bottom:16px; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:18px 22px; border-radius:10px; box-shadow:0 10px 25px rgba(15,23,42,0.06); border:1px solid #e5e7eb; }
    h1 { margin-bottom:8px; }
    h2 { margin-top:20px; margin-bottom:12px; font-size:16px; }
    table { border-collapse:collapse; width:100%; background:#fff; border-radius:8px; overflow:hidden; }
    th, td { border:1px solid #e5e7eb; padding:8px 10px; font-size:14px; text-align:center; }
    th { background:#e5f0ff; color:#1f2937; }
    tr:nth-child(even) { background:#f9fafb; }
    .btn-approve { padding:6px 12px; background:#2563eb; color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:13px; }
    .btn-approve:hover { background:#1d4ed8; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card">
        <h1>${t(locale, 'nav_account_reset')}</h1>
        <p style="font-size:13px;color:#555;">비번 초기화·OTP 초기화 요청을 승인할 수 있습니다.</p>
      </div>
      <div class="card">
        <h2>비번 초기화 요청</h2>
        ${pwRequests.length === 0 ? '<p>요청이 없습니다.</p>' : `<table><thead><tr><th>아이디</th><th>성명</th><th>요청 시각</th><th>작업</th></tr></thead><tbody>${pwRows}</tbody></table>`}
      </div>
      <div class="card">
        <h2>OTP 초기화 요청</h2>
        ${otpRequests.length === 0 ? '<p>요청이 없습니다.</p>' : `<table><thead><tr><th>아이디</th><th>성명</th><th>요청 시각</th><th>작업</th></tr></thead><tbody>${otpRows}</tbody></table>`}
      </div>
    </main>
  </div>
</body>
</html>`);
});

app.post('/admin/account-reset/approve', requireAuth, requirePage('account_reset'), (req, res) => {
  const userId = (req.body || {}).userId;
  const type = (req.body || {}).type || 'password';
  if (!userId) return res.status(400).send('userId required');
  const mem = getMemberByUserId(userId);
  if (!mem) return res.status(404).send('Member not found');
  MEMBERS = loadMembers();
  const m = MEMBERS.find((x) => x.id === mem.id);
  if (!m) return res.status(404).send('Member not found');
  if (type === 'otp') {
    m.otpSecret = '';
    m.otpFailCount = 0;
    m.otpLocked = false;
  } else {
    m.passwordHash = bcrypt.hashSync(mem.userId + INITIAL_PASSWORD_SUFFIX, 10);
    m.mustChangePassword = true;
  }
  const idx = MEMBERS.findIndex((x) => x.id === mem.id);
  if (idx >= 0) MEMBERS[idx] = m;
  saveMembers(MEMBERS);
  let reqList = loadPasswordResetRequests();
  reqList = reqList.filter((r) => !(r.userId === userId && (r.type || 'password') === type));
  savePasswordResetRequests(reqList);
  return res.redirect('/admin/account-reset');
});

// 비밀번호 초기화 요청 (비로그인): 이메일·성명·생년월일로 요청
app.get('/admin/forgot', (req, res) => {
  const locale = getLocale(req);
  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head><meta charset="UTF-8"/><title>비밀번호 초기화 요청</title>
<style>body{font-family:system-ui;margin:0;background:#111827;color:#f9fafb;}.c{max-width:400px;margin:60px auto;padding:24px;}.card{background:#1f2937;padding:24px;border-radius:10px;}label{display:block;margin-top:12px;}input{width:100%;padding:10px;box-sizing:border-box;border-radius:6px;border:1px solid #4b5563;background:#111827;color:#f9fafb;}button{margin-top:16px;padding:10px;background:#2563eb;color:#fff;border:none;border-radius:6px;cursor:pointer;width:100%;}a{color:#93c5fd;}</style>
</head>
<body>
  <div class="c">
    <div class="card">
      <h1>비밀번호 초기화 요청</h1>
      <p style="font-size:13px;color:#9ca3af;">가입 시 등록한 이메일, 성명, 생년월일이 일치하면 요청이 접수됩니다. 관리자 승인 후 초기 비밀번호(아이디+1!)로 로그인하여 새 비밀번호를 설정하세요.</p>
      <form method="post" action="/admin/forgot">
        <label>이메일 <input type="email" name="email" required /></label>
        <label>성명 <input type="text" name="name" required /></label>
        <label>생년월일 <input type="date" name="birthDate" /></label>
        <button type="submit">요청</button>
      </form>
      <p style="margin-top:16px;"><a href="/admin/login">로그인</a></p>
    </div>
  </div>
</body>
</html>`);
});

app.post('/admin/forgot', (req, res) => {
  const { email, name, birthDate } = req.body || {};
  MEMBERS = loadMembers();
  const mem = MEMBERS.find((m) => (m.email || '').trim().toLowerCase() === (email || '').trim().toLowerCase() && (m.name || '').trim() === (name || '').trim() && (m.birthDate || '').trim() === (birthDate || '').trim());
  if (!mem) {
    return res.status(400).send('일치하는 회원 정보가 없습니다. 이메일, 성명, 생년월일을 확인하세요.');
  }
  let reqList = loadPasswordResetRequests();
  if (!reqList.find((r) => r.userId === mem.userId && (r.type || 'password') === 'password')) {
    reqList.push({ userId: mem.userId, requestedAt: new Date().toISOString(), type: 'password' });
    savePasswordResetRequests(reqList);
  }
  return res.send('요청이 접수되었습니다. 관리자 승인 후 초기 비밀번호(아이디+1!)로 로그인하여 새 비밀번호를 설정하세요. <a href="/admin/login">로그인</a>');
});

// 가맹점 목록 + 등록 폼
app.get('/admin/merchants', requireAuth, requirePage('merchants'), (req, res) => {
  const locale = getLocale(req);
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });
  const internalOptions = Array.from(INTERNAL_TARGETS.values())
    .map(
      (target) =>
        `<option value="${target.id}">${target.id} - ${target.name}</option>`,
    )
    .join('');

  const confirmSaveMsg = (t(locale, 'merchants_confirm_save') || '저장(적용)하시겠습니까?').replace(/'/g, "\\'");
  const callbackNumberOptions = Array.from({ length: 50 }, (_, i) => {
    const n = i + 1;
    const value = `callback/${n}`;
    return `<option value="${value}">${n} (/noti/callback/${n})</option>`;
  }).join('');

  const resultNumberOptions = Array.from({ length: 50 }, (_, i) => {
    const n = i + 1;
    const value = `result/${n}`;
    return `<option value="${value}">${n} (/noti/result/${n})</option>`;
  }).join('');

  const sortType = (req.query.sort || 'recent').toString();
  const sortedEntries = getSortedMerchantEntries(sortType);

  const rows = Array.from(sortedEntries)
    .map(([id, m]) => {
      const cbKey = m.routeCallbackKey || '';
      const rsKey = m.routeResultKey || '';
      const notiBase = 'https://noti.icopay.net/noti/';
      let cbDisplay = '';
      let rsDisplay = '';
      if (cbKey) {
        const cbPath = cbKey.includes('/') ? cbKey : `callback/${cbKey}`;
        cbDisplay = notiBase + cbPath;
      }
      if (rsKey) {
        const rsPath = rsKey.includes('/') ? rsKey : `result/${rsKey}`;
        rsDisplay = notiBase + rsPath;
      }
      const cbUrl = m.callbackUrl || '';
      const rsUrl = m.resultUrl || '';
      const relay = m.enableRelay === false ? 'N' : 'Y';
      const internal = m.enableInternal === false ? 'N' : 'Y';
      const devInternal = m.enableDevInternal === true ? 'Y' : 'N';
      const internalTargetId = m.internalTargetId || '';
      const confirmDel = (t(locale, 'merchants_confirm_delete') || '삭제하시겠습니까?').replace(/'/g, "\\'");
      const confirmDel2 = (t(locale, 'merchants_confirm_delete_second') || '정말 삭제하시겠습니까? 이 가맹점의 노티 설정과 Route 설정이 모두 제거됩니다.').replace(/'/g, "\\'");
      return `<tr>
        <td>${id}</td>
        <td class="cell-url">${cbDisplay}</td>
        <td class="cell-url">${rsDisplay}</td>
        <td class="cell-url">${cbUrl}</td>
        <td class="cell-url">${rsUrl}</td>
        <td>${m.routeNo || ''}</td>
        <td>${m.internalCustomerId || ''}</td>
        <td>${internalTargetId}</td>
        <td>${relay}</td>
        <td>${internal}</td>
        <td>${devInternal}</td>
        <td class="actions-cell">
          <div style="display:flex;flex-direction:column;gap:4px;align-items:center;">
            <button
              type="button"
              class="edit-merchant"
              data-merchant-id="${id}"
              data-internal-target-id="${internalTargetId}"
              data-route-callback-key="${cbKey}"
              data-route-result-key="${rsKey}"
              data-callback-url="${cbUrl}"
              data-result-url="${rsUrl}"
              data-route-no="${m.routeNo || ''}"
              data-internal-customer-id="${m.internalCustomerId || ''}"
              data-enable-relay="${relay}"
              data-enable-internal="${internal}"
              data-enable-dev-internal="${devInternal}"
              data-relay-format="${m.relayFormat || 'raw'}"
              style="padding:4px 8px;font-size:12px;background:#facc15;color:#111827;border:none;border-radius:4px;cursor:pointer;"
            >${t(locale, 'merchants_edit')}</button>
            <form method="post" action="/admin/merchants/delete" onsubmit="return confirm('${confirmDel}') && confirm('${confirmDel2}');" style="margin:0;">
              <input type="hidden" name="merchantId" value="${id}" />
              <button type="submit" style="padding:4px 8px;font-size:12px;background:#dc2626;color:#fff;border:none;border-radius:4px;cursor:pointer;">${t(locale, 'merchants_delete')}</button>
            </form>
          </div>
        </td>
      </tr>`;
    })
    .join('');

  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'merchants_title')}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; color:#111827; }
    h1 { margin-bottom: 8px; }
    h2 { margin-top: 32px; }
    table { border-collapse: collapse; width: 100%; background:#ffffff; border-radius:8px; overflow:hidden; }
    th, td { border: 1px solid #e5e7eb; padding: 8px 10px; font-size: 14px; text-align: center; }
    th { background: #e5f0ff; color:#1f2937; }
    tr:nth-child(even) { background:#f9fafb; }
    label { display:block; margin-top:10px; font-size: 14px; }
    input[type="text"], select { width: 100%; padding: 8px 10px; margin-top:4px; box-sizing:border-box; border-radius:6px; border:1px solid #d1d5db; background:#f9fafb; }
    input[type="text"]:focus, select:focus { outline:none; border-color:#60a5fa; box-shadow:0 0 0 1px #bfdbfe; background:#ffffff; }
    button { margin-top: 12px; padding: 9px 16px; background:#60a5fa; color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:14px; }
    button:hover { background:#3b82f6; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .content { display:flex; flex-direction:column; gap:16px; }
    .card { background:#ffffff; padding:18px 22px; border-radius:10px; box-shadow:0 10px 25px rgba(15,23,42,0.06); margin-bottom:8px; border:1px solid #e5e7eb; }
    .cell-url { word-break: break-all; overflow-wrap: break-word; white-space: normal; max-width: 200px; text-align: center; }
    .merchants-table-wrap { overflow-x: auto; }
    .merchants-table-wrap table { min-width: 900px; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="content">
    <div class="card">
      <h1>${t(locale, 'merchants_title')}</h1>
      <p style="font-size:13px;color:#555;">${t(locale, 'merchants_desc')}</p>
      <h2>${t(locale, 'merchants_register')}</h2>
      <form id="merchant-form" method="post" action="/admin/merchants" onsubmit="return confirm('${confirmSaveMsg}');">
        <input type="hidden" name="originalMerchantId" id="originalMerchantId" value="" />
        <label>
          가맹점 ID (<code>merchantId</code>)
          <input type="text" name="merchantId" required />
        </label>
        <label>
          전산 노티 대상 (우리 전산에서 받을 노티 주소)
          <select name="internalTargetId">
            <option value="">-- 선택 안 함 --</option>
            ${internalOptions}
          </select>
        </label>
        <label>
          PG callback용 번호 (1~50, /noti/callback/번호)
          <div style="display:flex;gap:6px;align-items:center;margin-top:4px;">
            <select name="routeCallbackKey" style="width:140px;">
              <option value="">번호 선택 (1~50)</option>
              ${callbackNumberOptions}
            </select>
            <input type="text" id="callback-url-preview" readonly style="flex:1;padding:6px 8px;border-radius:6px;border:1px solid #d1d5db;background:#f9fafb;font-size:12px;" placeholder="선택 시 전체 주소가 표시됩니다." />
            <button type="button" id="copy-callback-url" style="padding:6px 10px;font-size:12px;background:#6b7280;color:#fff;border:none;border-radius:6px;cursor:pointer;white-space:nowrap;">복사</button>
          </div>
        </label>
        <label>
          PG result용 번호 (1~50, /noti/result/번호)
          <div style="display:flex;gap:6px;align-items:center;margin-top:4px;">
            <select name="routeResultKey" style="width:140px;">
              <option value="">번호 선택 (1~50)</option>
              ${resultNumberOptions}
            </select>
            <input type="text" id="result-url-preview" readonly style="flex:1;padding:6px 8px;border-radius:6px;border:1px solid #d1d5db;background:#f9fafb;font-size:12px;" placeholder="선택 시 전체 주소가 표시됩니다." />
            <button type="button" id="copy-result-url" style="padding:6px 10px;font-size:12px;background:#6b7280;color:#fff;border:none;border-radius:6px;cursor:pointer;white-space:nowrap;">복사</button>
          </div>
        </label>
        <label>
          가맹점 callback URL
          <input type="text" name="callbackUrl" required />
        </label>
        <label>
          가맹점 result URL
          <div style="display:flex;align-items:center;gap:8px;">
            <input type="text" name="resultUrl" style="flex:1;" />
            <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:#374151;white-space:nowrap;">
              <input type="checkbox" id="use-test-result-url" />
              테스트결과페이지 넣기
            </label>
          </div>
          <div style="margin-top:4px;font-size:11px;color:#6b7280;">체크 시 result URL 입력란에 <code style="background:#f3f4f6;padding:1px 4px;border-radius:4px;">https://noti.icopay.net/admin/test-pay/return</code> 이 자동으로 들어갑니다.</div>
        </label>
        <label>
          RouteNo (전산용 루트번호, 예: 7)
          <input type="text" name="routeNo" />
        </label>
        <label>
          전산용 CustomerId (예: M035594)
          <input type="text" name="internalCustomerId" />
        </label>
        <div id="route-warning" style="margin-top:10px;font-size:12px;color:#b91c1c;display:none;background:#fef2f2;border:1px solid #fecaca;padding:8px 10px;border-radius:6px;"></div>
        <label>
          <input type="checkbox" name="enableRelay" checked />
          전달 노티 사용 (가맹점으로 포워딩)
        </label>
        <label>
          노티 방식 (가맹점 수신 형식)
          <select name="relayFormat">
            <option value="raw">일반 (피지 원문 그대로)</option>
            <option value="json">JSON</option>
            <option value="form">FORM (application/x-www-form-urlencoded)</option>
          </select>
          <span style="font-size:12px;color:#6b7280;display:block;margin-top:4px;">선택하지 않으면 일반 형태로 자동 구성됩니다.</span>
        </label>
        <label>
          <input type="checkbox" name="enableInternal" checked />
          전산 노티 사용 (내부 전산 시스템으로 전송)
        </label>
        <label>
          <input type="checkbox" name="enableDevInternal" />
          개발 노티 사용 (개발 전산 시스템으로 전송)
        </label>
        <button type="submit">${t(locale, 'merchants_save')}</button>
      </form>
    </div>
    <div class="card merchants-table-wrap">
      <h2>${t(locale, 'merchants_list')}</h2>
      <div style="display:flex;flex-wrap:wrap;align-items:center;gap:12px;margin-bottom:12px;">
        <span style="font-size:13px;color:#374151;">정렬:</span>
        <a href="/admin/merchants?sort=recent" style="padding:6px 12px;font-size:13px;border-radius:6px;text-decoration:none;background:${sortType === 'recent' ? '#2563eb' : '#e5e7eb'};color:${sortType === 'recent' ? '#fff' : '#374151'};" title="최근 등록순">최신등록 (최근)</a>
        <a href="/admin/merchants?sort=past" style="padding:6px 12px;font-size:13px;border-radius:6px;text-decoration:none;background:${sortType === 'past' ? '#2563eb' : '#e5e7eb'};color:${sortType === 'past' ? '#fff' : '#374151'};" title="과거 등록순">최신등록 (과거)</a>
        <a href="/admin/merchants?sort=route_asc" style="padding:6px 12px;font-size:13px;border-radius:6px;text-decoration:none;background:${sortType === 'route_asc' ? '#2563eb' : '#e5e7eb'};color:${sortType === 'route_asc' ? '#fff' : '#374151'};" title="Route 번호 오름차순">Route 번호 (작은수)</a>
        <a href="/admin/merchants?sort=route_desc" style="padding:6px 12px;font-size:13px;border-radius:6px;text-decoration:none;background:${sortType === 'route_desc' ? '#2563eb' : '#e5e7eb'};color:${sortType === 'route_desc' ? '#fff' : '#374151'};" title="Route 번호 내림차순">Route 번호 (큰수)</a>
        <a href="/admin/merchants?sort=target" style="padding:6px 12px;font-size:13px;border-radius:6px;text-decoration:none;background:${sortType === 'target' ? '#2563eb' : '#e5e7eb'};color:${sortType === 'target' ? '#fff' : '#374151'};" title="동일 등록대상끼리 묶어서 표시">등록대상</a>
        <a href="/admin/merchants/export?sort=${sortType}" style="padding:6px 12px;font-size:13px;border-radius:6px;text-decoration:none;background:#166534;color:#fff;margin-left:8px;" download>Excel 내보내기</a>
      </div>
      <table>
        <thead>
          <tr>
            <th style="width:90px;">가맹점</th>
            <th class="cell-url">PG Callurl</th>
            <th class="cell-url">PG Reurl</th>
            <th class="cell-url">Origin CBurl</th>
            <th class="cell-url">Origin Reurl</th>
            <th style="width:70px;">Route</th>
            <th style="width:90px;">CustomerId</th>
            <th style="width:100px;">등록대상</th>
            <th style="width:50px;">PG</th>
            <th style="width:50px;">전산</th>
            <th style="width:50px;">개발</th>
            <th class="actions-cell" style="width:80px;">관리</th>
          </tr>
        </thead>
        <tbody>
          ${
            rows ||
            `<tr><td colspan="12" style="text-align:center;color:#777;">${t(locale, 'merchants_empty')}</td></tr>`
          }
        </tbody>
      </table>
      </div>
    </main>
  </div>
  <script>
    (function () {
      var cbSelect = document.querySelector('select[name="routeCallbackKey"]');
      var rsSelect = document.querySelector('select[name="routeResultKey"]');
      var cbPreview = document.getElementById('callback-url-preview');
      var rsPreview = document.getElementById('result-url-preview');
      var cbCopyBtn = document.getElementById('copy-callback-url');
      var rsCopyBtn = document.getElementById('copy-result-url');
      var warningBox = document.getElementById('route-warning');
      var form = document.getElementById('merchant-form');
      var editButtons = document.querySelectorAll('.edit-merchant');

      // 항상 이 도메인 기준으로 전체 주소 생성
      var baseUrl = 'https://noti.icopay.net';

      function updatePreviews() {
        var cb = cbSelect && cbSelect.value; // 예: "callback/8"
        var rs = rsSelect && rsSelect.value; // 예: "result/8"

        if (cbPreview) {
          cbPreview.value = cb ? baseUrl + '/noti/' + cb : '';
        }
        if (rsPreview) {
          rsPreview.value = rs ? baseUrl + '/noti/' + rs : '';
        }

        // 경고 박스는 간단히만 사용: 둘 다 비어 있으면 숨김
        if (warningBox) {
          if (!cb && !rs) {
            warningBox.style.display = 'none';
            warningBox.innerHTML = '';
          } else {
            warningBox.style.display = 'none';
            warningBox.innerHTML = '';
          }
        }
      }

      function copyToClipboard(el) {
        if (!el || !el.value) return;
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(el.value);
        } else {
          el.select();
          document.execCommand('copy');
        }
        alert('복사되었습니다.');
      }

      if (cbSelect) cbSelect.addEventListener('change', updatePreviews);
      if (rsSelect) rsSelect.addEventListener('change', updatePreviews);

      if (cbCopyBtn && cbPreview) {
        cbCopyBtn.addEventListener('click', function () {
          copyToClipboard(cbPreview);
        });
      }
      if (rsCopyBtn && rsPreview) {
        rsCopyBtn.addEventListener('click', function () {
          copyToClipboard(rsPreview);
        });
      }

      // 가맹점 result URL → 테스트결과페이지 넣기 체크 시 고정 URL 자동 세팅
      var rsUrlInput = form ? form.querySelector('input[name="resultUrl"]') : null;
      var useTestResultCheckbox = form ? document.getElementById('use-test-result-url') : null;
      var TEST_RESULT_URL = 'https://noti.icopay.net/admin/test-pay/return';
      if (useTestResultCheckbox && rsUrlInput) {
        useTestResultCheckbox.addEventListener('change', function () {
          if (this.checked) {
            rsUrlInput.value = TEST_RESULT_URL;
          }
        });
      }

      if (form) {
        form.addEventListener('submit', function (e) {
          var ok = window.confirm('설정을 저장하시겠습니까?');
          if (!ok) {
            e.preventDefault();
          }
        });
      }

      function applyEdit(button) {
        if (!button || !form) return;
        var merchantIdInput = form.querySelector('input[name="merchantId"]');
        var internalTargetSelect = form.querySelector('select[name="internalTargetId"]');
        var cbUrlInput = form.querySelector('input[name="callbackUrl"]');
        var rsUrlInput = form.querySelector('input[name="resultUrl"]');
        var routeNoInput = form.querySelector('input[name="routeNo"]');
        var internalCustomerInput = form.querySelector('input[name="internalCustomerId"]');
        var relayCheckbox = form.querySelector('input[name="enableRelay"]');
        var internalCheckbox = form.querySelector('input[name="enableInternal"]');
        var devInternalCheckbox = form.querySelector('input[name="enableDevInternal"]');

        if (merchantIdInput) {
          merchantIdInput.value = button.dataset.merchantId || '';
          merchantIdInput.readOnly = false;
        }
        var originalInput = form.querySelector('input[name="originalMerchantId"]');
        if (originalInput) originalInput.value = button.dataset.merchantId || '';
        if (internalTargetSelect) {
          internalTargetSelect.value = button.dataset.internalTargetId || '';
        }
        if (cbUrlInput) cbUrlInput.value = button.dataset.callbackUrl || '';
        if (rsUrlInput) rsUrlInput.value = button.dataset.resultUrl || '';
        if (routeNoInput) routeNoInput.value = button.dataset.routeNo || '';
        if (internalCustomerInput)
          internalCustomerInput.value = button.dataset.internalCustomerId || '';

        if (relayCheckbox) {
          relayCheckbox.checked = (button.dataset.enableRelay || 'Y') === 'Y';
        }
        if (internalCheckbox) {
          internalCheckbox.checked = (button.dataset.enableInternal || 'Y') === 'Y';
        }
        if (devInternalCheckbox) {
          devInternalCheckbox.checked = (button.dataset.enableDevInternal || 'N') === 'Y';
        }
        var relayFormatSelect = form.querySelector('select[name="relayFormat"]');
        if (relayFormatSelect) relayFormatSelect.value = button.dataset.relayFormat || 'raw';

        if (cbSelect) cbSelect.value = button.dataset.routeCallbackKey || '';
        if (rsSelect) rsSelect.value = button.dataset.routeResultKey || '';

        updatePreviews();
      }

      if (editButtons && editButtons.length > 0) {
        Array.prototype.forEach.call(editButtons, function (btn) {
          btn.addEventListener('click', function () {
            if (!confirm('수정 하시겠습니까?')) return;
            applyEdit(btn);
          });
        });
      }

      // 페이지 처음 로드될 때도 한 번 계산
      updatePreviews();
    })();
  </script>
</body>
</html>`);
});

// 가맹점 목록 Excel(CSV) 내보내기 (현재 정렬 기준)
function getSortedMerchantEntries(sortType) {
  let entries = [...MERCHANTS.entries()];
  if (sortType === 'recent') entries.reverse();
  else if (sortType === 'route_asc') {
    entries.sort((a, b) => {
      const na = Number(a[1].routeNo) || 0;
      const nb = Number(b[1].routeNo) || 0;
      return na - nb || String(a[0]).localeCompare(b[0]);
    });
  } else if (sortType === 'route_desc') {
    entries.sort((a, b) => {
      const na = Number(a[1].routeNo) || 0;
      const nb = Number(b[1].routeNo) || 0;
      return nb - na || String(a[0]).localeCompare(b[0]);
    });
  } else if (sortType === 'target') {
    // 등록대상(internalTargetId) 기준으로 묶어서 리스트: 동일 등록대상끼리 인접
    entries.sort((a, b) => {
      const ta = String(a[1].internalTargetId || '').trim();
      const tb = String(b[1].internalTargetId || '').trim();
      const cmp = ta.localeCompare(tb, undefined, { sensitivity: 'base' });
      return cmp !== 0 ? cmp : String(a[0]).localeCompare(b[0]);
    });
  }
  return entries;
}

app.get('/admin/merchants/export', requireAuth, requirePage('merchants'), (req, res) => {
  const sortType = (req.query.sort || 'recent').toString();
  const sortedEntries = getSortedMerchantEntries(sortType);
  const notiBase = 'https://noti.icopay.net/noti/';
  const escapeCsv = (v) => {
    const s = String(v == null ? '' : v);
    if (/[",\r\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
    return s;
  };
  const header = [
    '가맹점 ID',
    'PG Callback URL',
    'PG Result URL',
    'Callback URL',
    'Result URL',
    'Route No',
    'Internal CustomerId',
    '등록대상',
    '릴레이',
    '전산',
    '개발',
  ];
  const lines = ['\uFEFF' + header.map(escapeCsv).join(',')];
  for (const [id, m] of sortedEntries) {
    const cbKey = m.routeCallbackKey || '';
    const rsKey = m.routeResultKey || '';
    const cbPath = cbKey.includes('/') ? cbKey : `callback/${cbKey}`;
    const rsPath = rsKey.includes('/') ? rsKey : `result/${rsKey}`;
    const cbDisplay = cbKey ? notiBase + cbPath : '';
    const rsDisplay = rsKey ? notiBase + rsPath : '';
    const relay = m.enableRelay === false ? 'N' : 'Y';
    const internal = m.enableInternal === false ? 'N' : 'Y';
    const devInternal = m.enableDevInternal === true ? 'Y' : 'N';
    lines.push(
      [
        id,
        cbDisplay,
        rsDisplay,
        m.callbackUrl || '',
        m.resultUrl || '',
        m.routeNo || '',
        m.internalCustomerId || '',
        m.internalTargetId || '',
        relay,
        internal,
        devInternal,
      ].map(escapeCsv).join(','),
    );
  }
  const csv = lines.join('\r\n');
  const filename = `merchants_${sortType}_${new Date().toISOString().slice(0, 10)}.csv`;
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.send(csv);
});

// 가맹점 등록/수정 처리
app.post('/admin/merchants', requireAuth, requirePage('merchants'), (req, res) => {
  const {
    merchantId: rawMerchantId,
    originalMerchantId,
    routeCallbackKey,
    routeResultKey,
    callbackUrl,
    resultUrl,
    routeNo,
    internalCustomerId,
    internalTargetId,
    enableRelay,
    enableInternal,
    enableDevInternal,
    relayFormat,
  } = req.body;

  const merchantId = (rawMerchantId || '').trim();
  const origId = (originalMerchantId || '').trim();

  const actor = req.session.adminUser || 'unknown';
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const before = MERCHANTS.get(origId || merchantId) || null;

  if (!merchantId || !routeCallbackKey || !callbackUrl) {
    return res
      .status(400)
      .send('merchantId, routeCallbackKey, callbackUrl 은 필수입니다.');
  }

  if (origId && origId !== merchantId) {
    MERCHANTS.delete(origId);
  }

  const fmt = (relayFormat === 'json' || relayFormat === 'form') ? relayFormat : 'raw';
  MERCHANTS.set(merchantId, {
    merchantId,
    routeCallbackKey: routeCallbackKey || '',
    routeResultKey: routeResultKey || '',
    callbackUrl,
    resultUrl: resultUrl || '',
    routeNo: routeNo || '7',
    internalCustomerId: internalCustomerId || 'M035594',
    internalTargetId: internalTargetId || '',
    enableRelay: enableRelay === 'on',
    enableInternal: enableInternal === 'on',
    enableDevInternal: enableDevInternal === 'on',
    relayFormat: fmt,
  });

  console.log('[관리자] 가맹점 저장:', merchantId, MERCHANTS.get(merchantId));
  saveMerchants();
  appendConfigChangeLog({
    type: 'merchant_update',
    actor,
    clientIp,
    merchantId,
    before,
    after: MERCHANTS.get(merchantId),
  });
  res.redirect('/admin/merchants');
});

// 가맹점 삭제 처리
app.post('/admin/merchants/delete', requireAuth, requirePage('merchants'), (req, res) => {
  const { merchantId } = req.body;
  if (!merchantId) {
    return res.status(400).send('merchantId 는 필수입니다.');
  }

  const actor = req.session.adminUser || 'unknown';
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const before = MERCHANTS.get(merchantId) || null;

  if (!before) {
    return res.redirect('/admin/merchants');
  }

  MERCHANTS.delete(merchantId);
  saveMerchants();

  appendConfigChangeLog({
    type: 'merchant_delete',
    actor,
    clientIp,
    merchantId,
    before,
  });

  return res.redirect('/admin/merchants');
});

// PG 노티 로그 페이지 (수신시간 4타임존, 1건 1줄, Json/callback·json/result)
app.get('/admin/logs', requireAuth, requirePage('pg_logs'), (req, res) => {
  const locale = getLocale(req);
  const q = req.query || {};
  const escQ = (s) => (s ? String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '');
  const resendKind = (q.resendKind || 'payment').toString().toLowerCase();
  const resendOkLabel = resendKind === 'cancel' ? '취소 재전송이 완료되었습니다.' : '결제 재전송이 완료되었습니다.';
  const resendFailLabel = resendKind === 'cancel' ? '취소 재전송 실패' : '결제 재전송 실패';
  const resendMsg =
    q.resend === 'ok'
      ? '<div class="alert alert-ok">' + resendOkLabel + '</div>'
      : q.resend === 'fail' && q.reason
      ? '<div class="alert alert-fail">' + resendFailLabel + ': ' + escQ(q.reason) + '</div>'
      : q.void === 'ok'
      ? '<div class="alert alert-ok">무효 요청이 완료되었습니다.' + (q.noti === 'partial' ? ' (노티 일부 실패)' : '') + '</div>'
      : q.void === 'fail' && q.reason
      ? '<div class="alert alert-fail">무효 요청 실패: ' + escQ(q.reason) + '</div>'
      : q.refund === 'ok'
      ? '<div class="alert alert-ok">환불 요청이 완료되었습니다.' + (q.noti === 'partial' ? ' (노티 일부 실패)' : '') + '</div>'
      : q.refund === 'fail' && q.reason
      ? '<div class="alert alert-fail">환불 요청 실패: ' + escQ(q.reason) + '</div>'
      : q.err === 'invalid'
      ? '<div class="alert alert-fail">잘못된 요청입니다.</div>'
      : q.err === 'no_target' || q.err === 'no_body'
      ? '<div class="alert alert-fail">' + (q.reason ? escQ(q.reason) : (q.err === 'no_target' ? '가맹점 URL 없음' : '노티 본문 없음')) + '</div>'
      : '';
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });

  const liveLogs = NOTI_LOGS.filter((log) => (log.env || '') !== 'sandbox');
  const reversed = [...liveLogs].slice().reverse();
  const rows = reversed
    .map((log, i) => {
      const realIndex = NOTI_LOGS.indexOf(log);
      const dt = formatDateAndTimeTHJP(log.receivedAtIso || log.receivedAt);
      const jsonCallback = log.kind === 'callback' ? JSON.stringify(log.body, null, 2) : '';
      const jsonResult = log.kind === 'result' ? JSON.stringify(log.body, null, 2) : '';
      const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      const relayStatus = log.relayStatus || '-';
      const relayHasTarget = !!log.targetUrl;
      const relayFailReason = (log.relayFailReason || '').trim();
      const relayLabel =
        relayStatus === 'ok'
          ? '성공'
          : relayStatus === 'fail'
          ? '실패'
          : relayStatus === 'skip' && !relayHasTarget
          ? '노티없음'
          : relayStatus === 'skip'
          ? '미전송'
          : relayStatus;
      // 성공 시: JSON=녹색, FORM=노란색, 일반(raw)=파란색 (relayFormatUsed 또는 구 relaySentAsJson 호환)
      const formatUsed = log.relayFormatUsed || (log.relaySentAsJson === true ? 'json' : 'raw');
      const relayClass =
        relayStatus === 'ok'
          ? (formatUsed === 'json' ? 'status-ok' : formatUsed === 'form' ? 'status-ok-form' : 'status-ok-normal')
          : relayStatus === 'fail'
          ? 'status-fail'
          : relayStatus === 'skip' && !relayHasTarget
          ? 'status-none'
          : '';
      const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
      const canResend = (relayStatus === 'fail' || relayStatus === 'ok') && relayHasTarget && (log.body || log.rawBody);
      const resendKindVal = isCancelNotiBody(body) ? 'cancel' : 'payment';
      const resendKindLabel = resendKindVal === 'cancel' ? '취소' : '결제';
      const resendBtn = canResend
        ? `<div class="resend-wrap"><span class="resend-kind-label" style="font-size:11px;color:#6b7280;margin-right:4px;">${resendKindLabel}</span><form method="post" action="/admin/logs/resend" style="display:inline;" onsubmit="return confirm('일반(원문) 형식으로 다시 전송하시겠습니까?');"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="resendKind" value="${resendKindVal}" /><button type="submit" class="btn-resend">일반</button></form><form method="post" action="/admin/logs/resend" style="display:inline;" onsubmit="return confirm('JSON 형식으로 다시 전송하시겠습니까?');"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="resendKind" value="${resendKindVal}" /><input type="hidden" name="resendAsJson" value="1" /><button type="submit" class="btn-resend-json">JSON</button></form><form method="post" action="/admin/logs/resend" style="display:inline;" onsubmit="return confirm('FORM 형식으로 다시 전송하시겠습니까?');"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="resendKind" value="${resendKindVal}" /><input type="hidden" name="resendAsForm" value="1" /><button type="submit" class="btn-resend-form">FORM</button></form></div>`
        : !relayHasTarget
        ? '<span class="label-none">노티없음</span>'
        : '-';
      const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : null);
      const isSuccess = body.PaymentStatus === 0 || body.PaymentStatus === '0' || body.PaymentStatus === 1 || body.PaymentStatus === '1'
        || (body.paymentStatus && String(body.paymentStatus).toLowerCase() === 'success')
        || body.status === 1;
      const baseDate = body.TransactionDate || body.transactionDate || body.PaymentDate || body.paymentDate || log.receivedAtIso || log.receivedAt;
      const windowType = txId && isSuccess && log.merchantId && MERCHANTS.get(log.merchantId) ? getVoidRefundWindow(baseDate) : null;
      const cfg = loadChillPayTransactionConfig();
      const useSandbox = cfg.useSandbox;
      const voidRefundBtns = windowType === 'void_auto'
        ? `<form method="post" action="/admin/logs/void-request" style="display:inline;" onsubmit="return confirm('ChillPay에 무효 요청 후 가맹점/전산에 무효 노티를 보냅니다. 진행할까요?');"><input type="hidden" name="index" value="${realIndex}" /><button type="submit" class="btn-void">무효 요청</button></form>`
        : windowType === 'refund'
        ? `<form method="post" action="/admin/logs/refund-request" style="display:inline;" onsubmit="return confirm('ChillPay에 환불 요청 후 가맹점/전산에 환불 노티를 보냅니다. 진행할까요?');"><input type="hidden" name="index" value="${realIndex}" /><button type="submit" class="btn-refund">환불 요청</button></form>`
        : windowType === 'void_manual'
        ? '<span class="label-manual">수동만</span>'
        : '';
      return `<tr>
        <td class="col-date">${esc(dt.date)}</td>
        <td class="col-time">TH: ${esc(dt.timeTh)}<br><span class="time-jp">JP: ${esc(dt.timeJp)}</span></td>
        <td class="col-narrow">${esc(log.routeKey || '')}</td>
        <td class="col-narrow">${esc(log.merchantId || '')}</td>
        <td class="col-status"><span class="${relayClass}">${esc(relayLabel)}</span>${relayStatus === 'fail' && relayFailReason ? `<br /><span class="relay-fail-reason" title="${esc(relayFailReason)}">${esc(relayFailReason)}</span>` : ''}</td>
        <td class="col-json"><pre>${esc(jsonCallback) || '-'}</pre></td>
        <td class="col-json"><pre>${esc(jsonResult) || '-'}</pre></td>
        <td class="col-action">${resendBtn}</td>
        <td class="col-void-refund">${voidRefundBtns}</td>
      </tr>`;
    })
    .join('');

  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'pg_logs_title')}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
    h1 { margin-bottom: 8px; }
    table { border-collapse: collapse; width: 100%; background:#fff; table-layout: fixed; }
    th, td { border: 1px solid #e5e7eb; padding: 8px 10px; font-size: 13px; vertical-align: middle; text-align: center; }
    th { background: #e5f0ff; }
    tr:nth-child(even) { background:#f9fafb; }
    .col-date { width: 8%; min-width: 70px; }
    .col-time, .col-narrow { width: 9%; min-width: 75px; }
    .col-status { width: 8%; min-width: 70px; }
    .col-json { width: 26%; text-align: left; }
    .col-action { width: 8%; min-width: 70px; }
    .col-action .resend-wrap { display: inline-flex; flex-direction: column; align-items: center; justify-content: center; gap: 6px; margin: 6px 0; min-height: 48px; }
    .col-action form { margin: 4px 0; }
    .time-jp { color: #2563eb; }
    .status-ok { color: #059669; font-weight: 600; }
    .status-ok-normal { color: #2563eb; font-weight: 600; }
    .status-ok-form { color: #ca8a04; font-weight: 600; }
    .status-fail { color: #dc2626; font-weight: 600; }
    .relay-fail-reason { font-size: 11px; color: #b91c1c; word-break: break-all; display: block; margin-top: 2px; text-align: center; }
    .status-none { color:#b91c1c; font-weight:700; }
    .label-none { color:#b91c1c; font-weight:700; }
    .btn-resend { padding: 4px 10px; font-size: 12px; background: #2563eb; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
    .btn-resend:hover { background: #1d4ed8; }
    .btn-resend-json { padding: 4px 10px; font-size: 12px; background: #059669; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
    .btn-resend-json:hover { background: #047857; }
    .btn-resend-form { padding: 4px 10px; font-size: 12px; background: #ca8a04; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
    .btn-resend-form:hover { background: #a16207; }
    .col-void-refund { width: 9%; min-width: 90px; }
    .btn-void { padding: 4px 10px; font-size: 12px; background: #7c3aed; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
    .btn-void:hover { background: #6d28d9; }
    .btn-refund { padding: 4px 10px; font-size: 12px; background: #0d9488; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
    .btn-refund:hover { background: #0f766e; }
    .label-manual { font-size: 11px; color: #6b7280; }
    .alert { padding: 10px 14px; border-radius: 8px; margin-bottom: 12px; font-size: 13px; }
    .alert-ok { background: #d1fae5; color: #065f46; border: 1px solid #6ee7b7; }
    .alert-fail { background: #fee2e2; color: #991b1b; border: 1px solid #fca5a5; }
    .col-json pre { margin:0; white-space:pre-wrap; font-size:12px; text-align: left; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:8px; border:1px solid #e5e7eb; }
    pre { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card">
      ${resendMsg}
      <h1>${t(locale, 'pg_logs_title')} (${liveLogs.length})</h1>
      <p style="font-size:13px;color:#555;">PG에서 우리 서버로 들어온 노티 전문을 그대로 확인할 수 있습니다. ㅣ 재전송: 일반 · JSON · FORM (한 줄). 성공 <span style="color:#2563eb;font-weight:600;">파란색</span>=일반, <span style="color:#059669;font-weight:600;">녹색</span>=JSON, <span style="color:#ca8a04;font-weight:600;">노란색</span>=FORM</p>
      <table>
        <colgroup><col class="col-date" /><col class="col-time" /><col class="col-narrow" /><col class="col-narrow" /><col class="col-status" /><col class="col-json" /><col class="col-json" /><col class="col-action" /><col class="col-void-refund" /></colgroup>
        <thead>
          <tr>
            <th>수신일</th>
            <th>수신시각</th>
            <th>${t(locale, 'pg_logs_route_key')}</th>
            <th>${t(locale, 'pg_logs_merchant_id')}</th>
            <th>가맹점 수신</th>
            <th>${t(locale, 'pg_logs_json_callback')}</th>
            <th>${t(locale, 'pg_logs_json_result')}</th>
            <th>재전송</th>
            <th>무효/환불</th>
          </tr>
        </thead>
        <tbody>
          ${rows || `<tr><td colspan="9" style="text-align:center;color:#777;">${t(locale, 'pg_logs_empty')}</td></tr>`}
        </tbody>
      </table>
      </div>
    </main>
  </div>
</body>
</html>`);
});

// PG 노티 재전송 (가맹점으로 수동 재전송)
// - 재전송: 원문(rawBody)이 있으면 ChillPay 수신 원문 그대로 전송(Content-Type 유지). 없으면 body를 원래 contentType으로 전송.
// - 재전송(JSON): 항상 application/json으로 body 객체를 JSON 직렬화해 전송. 가맹점이 JSON만 수신할 때 사용.
app.post('/admin/logs/void-request', requireAuth, requirePageAny(['pg_logs', 'pg_result']), async (req, res) => {
  const index = parseInt(req.body.index, 10);
  if (Number.isNaN(index) || index < 0 || index >= NOTI_LOGS.length) {
    return res.redirect('/admin/logs?void=fail&reason=' + encodeURIComponent('잘못된 인덱스'));
  }
  const log = NOTI_LOGS[index];
  const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
  const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : null);
  if (!txId) return res.redirect('/admin/logs?void=fail&reason=' + encodeURIComponent('TransactionId 없음'));
  const cfg = loadChillPayTransactionConfig();
  const result = await chillPayRequestVoid(txId, false);
  if (!result.success) return res.redirect('/admin/logs?void=fail&reason=' + encodeURIComponent(result.error || '무효 요청 실패'));
  const sendResult = await sendVoidOrRefundNoti(log, 'void');
  markVoidNotiSent(txId);
  return res.redirect('/admin/logs?void=ok' + (sendResult.success ? '' : '&noti=partial'));
});

app.post('/admin/logs/refund-request', requireAuth, requirePageAny(['pg_logs', 'pg_result']), async (req, res) => {
  const index = parseInt(req.body.index, 10);
  if (Number.isNaN(index) || index < 0 || index >= NOTI_LOGS.length) {
    return res.redirect('/admin/logs?refund=fail&reason=' + encodeURIComponent('잘못된 인덱스'));
  }
  const log = NOTI_LOGS[index];
  const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
  const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : null);
  if (!txId) return res.redirect('/admin/logs?refund=fail&reason=' + encodeURIComponent('TransactionId 없음'));
  const cfg = loadChillPayTransactionConfig();
  const result = await chillPayRequestRefund(txId, false);
  if (!result.success) return res.redirect('/admin/logs?refund=fail&reason=' + encodeURIComponent(result.error || '환불 요청 실패'));
  const sendResult = await sendVoidOrRefundNoti(log, 'refund');
  markRefundNotiSent(txId);
  return res.redirect('/admin/logs?refund=ok' + (sendResult.success ? '' : '&noti=partial'));
});

// ----- 종합거래 전용 메뉴 (거래내역 / 취소내역 / 무효거래 / 환불거래 / 거래노티) -----
const cancelRefundLayoutCss = `
  body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
  h1 { margin-bottom: 8px; }
  table { border-collapse: collapse; width: 100%; background:#fff; table-layout: fixed; }
  th, td { border: 1px solid #e5e7eb; padding: 8px 10px; font-size: 13px; vertical-align: middle; text-align: center; }
  th { background: #e5f0ff; }
  .card table th, .card table td { text-align: center; vertical-align: middle; }
  tr:nth-child(even) { background:#f9fafb; }
  .tx-status-fail { color: #dc2626; font-weight: 600; }
  .tx-status-success { color: #2563eb; font-weight: 600; }
  .tx-status-cancel { color: #059669; font-weight: 600; }
  .tx-status-void-manual { color: #7c3aed; font-weight: 600; }
  .tx-status-refund-manual { color: #84cc16; font-weight: 600; }
  .tx-badge { display: inline-block; padding: 3px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; white-space: nowrap; border: 1px solid transparent; }
  .tx-badge-fail { background: #fecaca; color: #b91c1c; }
  .tx-badge-cancel { background: #a7f3d0; color: #047857; }
  .tx-badge-success { background: #bae6fd; color: #0369a1; }
  .tx-badge-void-manual { background: #e9d5ff; color: #6d28d9; }
  .tx-badge-refund-manual { background: #d9f99d; color: #65a30d; }
  .tx-badge-noti { background: #f3f4f6; color: #4b5563; }
  .tx-row-voided-refunded { background: #fef2f2 !important; }
  .tx-row-voided-refunded td { color: #991b1b; }
  .col-no { width: 15px; min-width: 15px; font-size: 10px; }
  .col-date { width: 68px; min-width: 68px; font-size: 11px; }
  .col-time { width: 72px; min-width: 72px; font-size: 10px; }
  .col-route { width: 15px; min-width: 15px; font-size: 10px; }
  .col-merchant { min-width: 110px; max-width: 180px; font-size: 11px; }
  .col-narrow { width: 8%; min-width: 55px; font-size: 11px; }
  .col-body-val { font-size: 11px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .col-TransactionId { max-width: 85px; }
  .col-OrderNo { min-width: 130px; max-width: 200px; }
  .col-Amount { max-width: 90px; }
  .col-internal-amount { min-width: 75px; max-width: 110px; font-size: 11px; }
  .col-currency { width: 38px; min-width: 38px; font-size: 10px; }
  .col-CustomerId { min-width: 95px; max-width: 130px; }
  .col-PaymentDescription, .col-Description { min-width: 100px; max-width: 180px; }
  .col-action { width: 12%; min-width: 90px; }
  .col-void-refund-detail { min-width: 160px; max-width: 280px; font-size: 11px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .tx-list-table { table-layout: fixed; }
  .tx-list-table th, .tx-list-table td { padding: 4px 6px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; text-align: center; vertical-align: middle; }
  .tx-list-table th { position: relative; }
  .tx-col-resizer { position: absolute; right: 0; top: 0; bottom: 0; width: 8px; cursor: col-resize; z-index: 1; }
  .tx-col-resizer:hover { background: rgba(37, 99, 235, 0.2); }
  .tx-list-table tr { display: table-row; }
  .tx-legend-grid { display: grid; grid-template-columns: auto 1fr auto 1fr; gap: 2px 10px; font-size: 10px; margin-bottom: 12px; align-items: start; }
  .tx-legend-grid .tx-legend-term-cell { padding: 3px 6px; border: 1px solid #e5e7eb; border-radius: 4px; background: #f3f4f6; font-weight: 600; color: #1f2937; white-space: nowrap; }
  .tx-legend-grid .tx-legend-desc-cell { padding: 3px 6px; border: 1px solid #e5e7eb; border-radius: 4px; background: #f9fafb; color: #4b5563; }
  .tx-date-form { display: inline-flex; align-items: center; gap: 4px; font-size: 11px; vertical-align: middle; }
  .tx-date-form .tx-date-label { font-weight: 500; color: #374151; margin: 0; }
  .tx-date-form .tx-date-input { padding: 2px 4px; font-size: 11px; border: 1px solid #d1d5db; border-radius: 4px; line-height: 1.2; }
  .tx-date-form .tx-date-sep { color: #6b7280; font-size: 11px; }
  .tx-date-form .tx-date-btn { padding: 2px 8px; font-size: 11px; background: #2563eb; color: #fff; border: none; border-radius: 4px; cursor: pointer; line-height: 1.2; }
  .time-jp { color: #2563eb; }
  .btn-void { padding: 6px 12px; font-size: 12px; background: #7c3aed; color: #fff; border: none; border-radius: 4px; cursor: pointer; text-decoration: none; display: inline-block; }
  .btn-void:hover { background: #6d28d9; }
  .btn-refund { padding: 6px 12px; font-size: 12px; background: #0d9488; color: #fff; border: none; border-radius: 4px; cursor: pointer; text-decoration: none; display: inline-block; }
  .btn-refund:hover { background: #0f766e; }
  .btn-email { padding: 6px 12px; font-size: 12px; background: #2563eb; color: #fff; border: none; border-radius: 4px; cursor: pointer; text-decoration: none; display: inline-block; }
  .btn-email:hover { background: #1d4ed8; }
  .btn-email-disabled { padding: 6px 12px; font-size: 12px; background: #e5e7eb; color: #9ca3af; border-radius: 4px; cursor: not-allowed; display: inline-block; }
  .col-status { font-weight: 600; }
  .status-ok { color: #059669; }
  .status-fail { color: #dc2626; }
  .status-skip { color: #6b7280; }
  .label-manual { font-size: 11px; color: #6b7280; }
  .alert { padding: 10px 14px; border-radius: 8px; margin-bottom: 12px; font-size: 13px; }
  .alert-ok { background: #d1fae5; color: #065f46; border: 1px solid #6ee7b7; }
  .alert-fail { background: #fee2e2; color: #991b1b; border: 1px solid #fca5a5; }
  .env-switcher { font-size: 13px; font-weight: 600; margin-left: 12px; }
  .env-switcher a { padding: 4px 10px; border-radius: 6px; text-decoration: none; color: #4b5563; background: #f3f4f6; }
  .env-switcher a:hover { background: #e5e7eb; color: #1f2937; }
  .env-switcher a.env-active { background: #2563eb; color: #fff; }
  .sync-history-section { margin-top: 24px; padding-top: 16px; border-top: 1px solid #e5e7eb; }
  .sync-history-section h3 { font-size: 14px; margin: 0 0 10px 0; color: #374151; }
  .sync-history-list { display: flex; flex-direction: column; gap: 6px; }
  .sync-history-item details { border: 1px solid #e5e7eb; border-radius: 8px; background: #f9fafb; }
  .sync-history-item summary { padding: 8px 12px; cursor: pointer; font-size: 13px; list-style: none; display: flex; align-items: center; justify-content: space-between; }
  .sync-history-item summary::-webkit-details-marker { display: none; }
  .sync-history-item summary::after { content: '펼치기'; font-size: 11px; color: #2563eb; }
  .sync-history-item[open] summary::after { content: '접기'; }
  .sync-history-detail { padding: 10px 12px; border-top: 1px solid #e5e7eb; background: #fff; overflow-x: auto; }
  .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
  .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
  .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
  .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
  .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
  .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
  .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
  .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
  .nav-group-summary::-webkit-details-marker { display:none; }
  .nav-group-summary::marker { content:""; }
  .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
  .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
  .nav-group-items { padding-left:4px; padding-bottom:4px; }
  .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
  .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
  .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
  .nav-group-items a:hover, .nav-group-items a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
  .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
  .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
  .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:8px; border:1px solid #e5e7eb; }
`;

function renderCancelRefundPage(locale, adminUser, title, mainContent, alertHtml, currentUrl, member, req, actionHtml, env) {
  const clientIp = (req && req.headers) ? ((req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() || req.ip || '') : '';
  const nowKr = (req && req.headers) ? new Date().toLocaleString('ko-KR', { hour12: false }) : '';
  const nowTh = (req && req.headers) ? new Date().toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false }) : '';
  const raw = mainContent && String(mainContent).trim() ? mainContent : '';
  const isFullMarkup = raw.indexOf('<table') === 0 || raw.indexOf('<form') === 0 || raw.includes('</table>');
  const noDataLabel = t(locale, 'cr_no_data');
  const mainInner = raw || '<table><tbody><tr><td colspan="6" style="text-align:center;color:#777;">' + noDataLabel + '</td></tr></tbody></table>';
  const wrapped = isFullMarkup ? mainInner : '<table>' + mainInner + '</table>';
  const basePath = (currentUrl || '').split('?')[0] || '';
  const envLinkLive = basePath + (basePath.indexOf('?') >= 0 ? '&' : '?') + 'env=live';
  const envLinkSandbox = basePath + (basePath.indexOf('?') >= 0 ? '&' : '?') + 'env=sandbox';
  const currentEnv = (env || 'live').toString().toLowerCase() === 'sandbox' ? 'sandbox' : 'live';
  const envSwitcherHtml = '<span class="env-switcher"><a href="' + envLinkLive + '" class="' + (currentEnv === 'live' ? 'env-active' : '') + '">PRODUCTION</a> | <a href="' + envLinkSandbox + '" class="' + (currentEnv === 'sandbox' ? 'env-active' : '') + '">SANDBOX</a></span>';
  const titleRow = actionHtml
    ? `<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:14px;"><h1 style="margin:0;">${title} ${envSwitcherHtml}</h1><div style="flex-shrink:0;">${actionHtml}</div></div>`
    : `<h1 style="margin:0;">${title} ${envSwitcherHtml}</h1>`;
  return `<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${title}</title>
  <style>${cancelRefundLayoutCss}</style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, member || null, currentUrl || '')}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, currentUrl || '')}
      <div class="card">
        ${alertHtml || ''}
        ${titleRow}
        ${wrapped}
      </div>
    </main>
  </div>
  <script>
    // 공통 2단계 확인: data-confirm, data-confirm-second 속성이 있는 버튼/폼에 적용
    (function() {
      function attachConfirm(el) {
        if (!el || el.dataset._confirmBound) return;
        el.dataset._confirmBound = '1';
        var msg1 = el.getAttribute('data-confirm');
        var msg2 = el.getAttribute('data-confirm-second');
        el.addEventListener('click', function(e) {
          // form submit 버튼만 대상으로 처리
          var form = el.form;
          if (!form) return;
          if (msg1 && !window.confirm(msg1)) {
            e.preventDefault();
            e.stopPropagation();
            return;
          }
          if (msg2 && !window.confirm(msg2)) {
            e.preventDefault();
            e.stopPropagation();
          }
        });
      }
      document.addEventListener('DOMContentLoaded', function() {
        var els = document.querySelectorAll('[data-confirm]');
        for (var i = 0; i < els.length; i++) {
          attachConfirm(els[i]);
        }
      });
    })();
  </script>
</body>
</html>`;
}

// ----- 종합거래 > 거래내역 (지정 컬럼만 표시) -----
function parseNotiBody(log) {
  const raw = log.body;
  if (raw && typeof raw === 'object') return raw;
  if (typeof raw === 'string') {
    try { return JSON.parse(raw); } catch { return {}; }
  }
  return {};
}
// 테스트 결제 환경 설정과 동일한 Currency 코드 → 표시용 (JPY, USD, KRW, THB)
function formatCurrencyForDisplay(val) {
  if (val == null || val === '') return null;
  const s = String(val).trim();
  if (s === '392' || s === 'JPY') return 'JPY';
  if (s === '840' || s === 'USD') return 'USD';
  if (s === '764' || s === 'THB') return 'THB';
  if (s === '410' || s === 'KRW' || s === 'KOR') return 'KRW';
  return s;
}
// 금액 천단위 구분(.) + 소수점(,) 표시
function formatAmountWithSeparator(val) {
  if (val == null || val === '') return '-';
  const n = Number(val);
  if (Number.isNaN(n)) return '-';
  const isNeg = n < 0;
  const abs = Math.abs(n);
  const intPart = Math.floor(abs);
  const decPart = abs - intPart;
  const intStr = String(intPart).replace(/\B(?=(\d{3})+(?!\d))/g, '.');
  if (decPart < 1e-9) return (isNeg ? '-' : '') + intStr;
  const decStr = (decPart.toFixed(2) || '00').split('.')[1] || '00';
  return (isNeg ? '-' : '') + intStr + ',' + decStr;
}
const TRANSACTION_LIST_COLUMNS = [
  { type: 'fixed', id: 'no' },
  { type: 'fixed', id: 'received_date' },
  { type: 'fixed', id: 'received_time' },
  { type: 'fixed', id: 'route_no' },
  { type: 'fixed', id: 'merchant' },
  { type: 'body', keys: ['TransactionId', 'transactionId'] },
  { type: 'body', keys: ['OrderNo', 'orderNo'] },
  { type: 'body', keys: ['Amount', 'amount'] },
  { type: 'fixed', id: 'internal_amount' },
  { type: 'body', keys: ['status'] },
  { type: 'body', keys: ['PaymentDate', 'paymentDate'] },
  { type: 'body', keys: ['Currency', 'currency'], display: 'currency' },
  { type: 'body', keys: ['CustomerId', 'customerId'] },
  { type: 'fixed', id: 'status' },
  { type: 'fixed', id: 'noti' },
  { type: 'fixed', id: 'void_refund_detail' },
];
const TRANSACTION_SEARCH_FIELDS = [
  { key: 'all', label: '전체' },
  { key: 'OrderNo', label: 'OrderNo' },
  { key: 'CustomerId', label: 'CustomerId' },
  { key: 'TransactionId', label: 'TransactionId' },
  { key: 'Amount', label: 'Amount' },
  { key: 'merchant', label: '가맹점' },
  { key: 'Route', label: 'Route' },
  { key: 'Currency', label: 'Currency' },
  { key: 'Description', label: 'Description' },
];
app.get('/admin/transactions', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const q = req.query || {};
  const sortBy = (q.sort || 'date').toString();
  const sortDir = (q.sortDir || 'desc').toString().toLowerCase() === 'asc' ? 'asc' : 'desc';
  const searchKw = (q.search || '').toString().trim();
  const searchField = (q.searchField || 'all').toString();
  let dateFrom = (q.dateFrom || '').toString().trim();
  let dateTo = (q.dateTo || '').toString().trim();
  const period = (q.period || '').toString();
  const successOnly = (q.successOnly || '') === '1' || String(q.successOnly || '').toLowerCase() === 'true';
  const perPage = Math.max(10, Math.min(200, parseInt(q.perPage, 10) || 25));
  const page = Math.max(1, parseInt(q.page, 10) || 1);
  // 거래 내역 = PG에서 수신한 모든 노티(성공/실패/취소). env 쿼리로 PRODUCTION/SANDBOX 선택.
  let list = [...getEnvFilteredLogs(req)].slice().reverse();

  // 기간 프리셋(당일/전일/이번주/지난주/당월/전월) 적용: 사용자가 직접 일자를 지정한 경우에는 우선
  if (!dateFrom && !dateTo && period) {
    const now = new Date();
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const ymd = (d) => {
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, '0');
      const day = String(d.getDate()).padStart(2, '0');
      return `${y}-${m}-${day}`;
    };
    let start = null;
    let end = null;
    if (period === 'today') {
      start = new Date(today);
      end = new Date(today);
    } else if (period === 'yesterday') {
      start = new Date(today);
      start.setDate(start.getDate() - 1);
      end = new Date(start);
    } else if (period === 'thisWeek') {
      const day = today.getDay(); // 0=일요일
      const monOffset = day === 0 ? -6 : 1 - day;
      start = new Date(today);
      start.setDate(start.getDate() + monOffset);
      end = new Date(start);
      end.setDate(end.getDate() + 6);
    } else if (period === 'lastWeek') {
      const day = today.getDay();
      const monOffset = day === 0 ? -6 : 1 - day;
      end = new Date(today);
      end.setDate(end.getDate() + monOffset - 1);
      start = new Date(end);
      start.setDate(start.getDate() - 6);
    } else if (period === 'thisMonth') {
      start = new Date(today.getFullYear(), today.getMonth(), 1);
      end = new Date(today.getFullYear(), today.getMonth() + 1, 0);
    } else if (period === 'lastMonth') {
      start = new Date(today.getFullYear(), today.getMonth() - 1, 1);
      end = new Date(today.getFullYear(), today.getMonth(), 0);
    }
    if (start && end) {
      dateFrom = ymd(start);
      dateTo = ymd(end);
    }
  }
  if (dateFrom || dateTo) {
    const fromTs = dateFrom ? Date.parse(dateFrom) : 0;
    const toTs = dateTo ? (Date.parse(dateTo) + 86400000) : Infinity;
    list = list.filter((log) => {
      const t = Date.parse(log.receivedAtIso || log.receivedAt);
      return !Number.isNaN(t) && t >= fromTs && t < toTs;
    });
  }
  if (successOnly) {
    list = list.filter((log) => {
      const body = parseNotiBody(log);
      return isSuccessPaymentBody(body);
    });
  }

  if (searchKw) {
    const kw = searchKw.toLowerCase();
    list = list.filter((log) => {
      const body = parseNotiBody(log);
      const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
      const routeNo = merchant && (merchant.routeNo != null && String(merchant.routeNo).trim() !== '') ? String(merchant.routeNo) : (String(log.routeKey || '').match(/\d+/) || [])[0] || '';
      if (searchField === 'all') {
        const str = [
          log.receivedAtIso || log.receivedAt,
          body.TransactionId, body.transactionId, body.OrderNo, body.orderNo, body.Amount, body.amount,
          body.PaymentStatus, body.paymentStatus, body.status, body.PaymentDate, body.paymentDate,
          formatCurrencyForDisplay(body.Currency || body.currency), body.Currency, body.currency,
          body.CustomerId, body.customerId, body.PaymentDescription, body.paymentDescription, body.Description, body.description,
          log.merchantId || '', routeNo,
        ].filter(Boolean).join(' ').toLowerCase();
        return str.indexOf(kw) !== -1;
      }
      let val = '';
      if (searchField === 'OrderNo') val = (body.OrderNo || body.orderNo || '') + '';
      else if (searchField === 'CustomerId') val = (body.CustomerId || body.customerId || '') + '';
      else if (searchField === 'TransactionId') val = (body.TransactionId || body.transactionId || '') + '';
      else if (searchField === 'Amount') val = (body.Amount || body.amount || '') + '';
      else if (searchField === 'merchant') val = (log.merchantId || '') + '';
      else if (searchField === 'Route') val = routeNo + '';
      else if (searchField === 'Currency') val = (formatCurrencyForDisplay(body.Currency || body.currency) || body.Currency || body.currency || '') + '';
      else if (searchField === 'Description') val = (body.PaymentDescription || body.paymentDescription || body.Description || body.description || '') + '';
      return val.toLowerCase().indexOf(kw) !== -1;
    });
  }
  const rev = sortDir === 'asc' ? 1 : -1;
  if (sortBy === 'route') {
    list.sort((a, b) => {
      const ma = a.merchantId ? MERCHANTS.get(a.merchantId) : null;
      const mb = b.merchantId ? MERCHANTS.get(b.merchantId) : null;
      const ra = ma && (ma.routeNo != null && String(ma.routeNo).trim() !== '') ? String(ma.routeNo) : (String(a.routeKey || '').match(/\d+/) || [])[0] || '';
      const rb = mb && (mb.routeNo != null && String(mb.routeNo).trim() !== '') ? String(mb.routeNo) : (String(b.routeKey || '').match(/\d+/) || [])[0] || '';
      const c = ra.localeCompare(rb, 'ja');
      if (c !== 0) return c * rev;
      return ((Date.parse(b.receivedAtIso || b.receivedAt) || 0) - (Date.parse(a.receivedAtIso || a.receivedAt) || 0)) * rev;
    });
  } else if (sortBy === 'currency') {
    list.sort((a, b) => {
      const ba = parseNotiBody(a);
      const bb = parseNotiBody(b);
      const ca = String(formatCurrencyForDisplay(ba.Currency || ba.currency) || ba.Currency || ba.currency || '').toLowerCase();
      const cb = String(formatCurrencyForDisplay(bb.Currency || bb.currency) || bb.Currency || bb.currency || '').toLowerCase();
      const c = ca.localeCompare(cb);
      if (c !== 0) return c * rev;
      return ((Date.parse(b.receivedAtIso || b.receivedAt) || 0) - (Date.parse(a.receivedAtIso || a.receivedAt) || 0)) * rev;
    });
  } else if (sortBy === 'status') {
    const statusOrder = (log) => {
      const body = parseNotiBody(log);
      const ps = body.PaymentStatus ?? body.paymentStatus ?? body.status;
      const isCancel = isDefinitelyCancelPaymentStatus(ps);
      const isSuccess = isSuccessPaymentBody(body);
      if (!isSuccess && !isCancel) return 0; // 실패/기타
      if (isCancel) return 1;                // 취소
      return 2;                              // 결제(성공)
    };
    list.sort((a, b) => {
      const c = (statusOrder(a) - statusOrder(b)) * rev;
      if (c !== 0) return c;
      return ((Date.parse(b.receivedAtIso || b.receivedAt) || 0) - (Date.parse(a.receivedAtIso || a.receivedAt) || 0)) * rev;
    });
  } else {
    list.sort((a, b) => ((Date.parse(b.receivedAtIso || b.receivedAt) || 0) - (Date.parse(a.receivedAtIso || a.receivedAt) || 0)) * rev);
  }
  const totalCount = list.length;
  const totalPages = Math.max(1, Math.ceil(totalCount / perPage));
  const pageNum = Math.min(page, totalPages);
  list = list.slice((pageNum - 1) * perPage, pageNum * perPage);
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const formatVal = (v) => {
    if (v == null || v === '') return '-';
    if (typeof v === 'object' || Array.isArray(v)) return esc(JSON.stringify(v));
    return esc(String(v));
  };
  const getBodyVal = (body, keys) => {
    for (const k of keys) {
      if (body[k] != null && body[k] !== '') return body[k];
    }
    return null;
  };
  const thLabels = [
    '번호',
    t(locale, 'cr_th_received_date'),
    t(locale, 'cr_th_received_time'),
    'Route',
    t(locale, 'cr_th_merchant'),
    'TransactionId', 'OrderNo', 'Amount', '전산금액', 'status', 'PaymentDate', 'Currency',
    'CustomerId',
    t(locale, 'tx_th_status'),
    t(locale, 'tx_th_noti'),
    '내역/사유',
  ];
  const voidRefundByTxId = buildVoidRefundNotiMap(30);
  const formatVoidRefundSentAt = (iso) => {
    if (!iso) return '';
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    const h = String(d.getHours()).padStart(2, '0');
    const min = String(d.getMinutes()).padStart(2, '0');
    return y + '-' + m + '-' + day + ' ' + h + ':' + min;
  };
  const baseUrl = '/admin/transactions';
  const env = getEnvFromReq(req);
  const qs = (overrides) => {
    const o = { sort: sortBy, search: searchKw, dateFrom, dateTo, searchField, sortDir, perPage, page: pageNum, env, period, successOnly: successOnly ? '1' : '', ...overrides };
    const parts = [];
    if (o.sort) parts.push('sort=' + encodeURIComponent(o.sort));
    if (o.search) parts.push('search=' + encodeURIComponent(o.search));
    if (o.dateFrom) parts.push('dateFrom=' + encodeURIComponent(o.dateFrom));
    if (o.dateTo) parts.push('dateTo=' + encodeURIComponent(o.dateTo));
    if (o.searchField && o.searchField !== 'all') parts.push('searchField=' + encodeURIComponent(o.searchField));
    if (o.sortDir && o.sortDir !== 'desc') parts.push('sortDir=' + encodeURIComponent(o.sortDir));
    if (o.perPage && o.perPage !== 25) parts.push('perPage=' + encodeURIComponent(o.perPage));
    if (o.page && o.page !== 1) parts.push('page=' + encodeURIComponent(o.page));
    if (o.period && o.period !== 'all') parts.push('period=' + encodeURIComponent(o.period));
    if (o.successOnly === '1') parts.push('successOnly=1');
    if (o.env) parts.push('env=' + encodeURIComponent(o.env));
    return parts.length ? '?' + parts.join('&') : '';
  };
  const sortLinks = [
    { key: 'date', label: '일자별' },
    { key: 'route', label: 'Route' },
    { key: 'currency', label: 'Currency' },
    { key: 'status', label: '상태' },
  ].map((o) => {
    const url = baseUrl + qs({ sort: o.key, page: 1 });
    const active = sortBy === o.key;
    return '<a href="' + url + '" style="padding:4px 10px;font-size:12px;border-radius:4px;text-decoration:none;background:' + (active ? '#2563eb' : '#e5e7eb') + ';color:' + (active ? '#fff' : '#374151') + ';margin-right:4px;">' + esc(o.label) + '</a>';
  }).join('');
  const sortDirLinks = '<a href="' + baseUrl + qs({ sortDir: 'asc', page: 1 }) + '" style="padding:4px 8px;font-size:11px;border-radius:4px;text-decoration:none;background:' + (sortDir === 'asc' ? '#2563eb' : '#e5e7eb') + ';color:' + (sortDir === 'asc' ? '#fff' : '#374151') + ';">오름차순</a><a href="' + baseUrl + qs({ sortDir: 'desc', page: 1 }) + '" style="padding:4px 8px;font-size:11px;border-radius:4px;text-decoration:none;margin-left:4px;background:' + (sortDir === 'desc' ? '#2563eb' : '#e5e7eb') + ';color:' + (sortDir === 'desc' ? '#fff' : '#374151') + ';">내림차순</a>';

  const periodOptions = [
    { key: 'today', label: '당일' },
    { key: 'yesterday', label: '전일' },
    { key: 'thisWeek', label: '이번주' },
    { key: 'lastWeek', label: '지난주' },
    { key: 'thisMonth', label: '당월' },
    { key: 'lastMonth', label: '전월' },
  ];
  const periodLinks = periodOptions.map((o) => {
    const url = baseUrl + qs({ period: o.key, page: 1, dateFrom: '', dateTo: '' });
    const active = period === o.key;
    return '<a href="' + url + '" style="padding:4px 10px;font-size:12px;border-radius:4px;text-decoration:none;background:' + (active ? '#2563eb' : '#e5e7eb') + ';color:' + (active ? '#fff' : '#374151') + ';margin-left:4px;">' + esc(o.label) + '</a>';
  }).join('');

  const searchFieldOptions = TRANSACTION_SEARCH_FIELDS.map((f) => '<option value="' + esc(f.key) + '"' + (searchField === f.key ? ' selected' : '') + '>' + esc(f.label) + '</option>').join('');
  const searchForm = '<form method="get" action="' + baseUrl + '" style="display:inline;margin-left:8px;"><input type="hidden" name="sort" value="' + esc(sortBy) + '" /><input type="hidden" name="sortDir" value="' + esc(sortDir) + '" /><input type="hidden" name="dateFrom" value="' + esc(dateFrom) + '" /><input type="hidden" name="dateTo" value="' + esc(dateTo) + '" /><input type="hidden" name="perPage" value="' + esc(perPage) + '" /><input type="hidden" name="env" value="' + esc(env) + '" /><input type="hidden" name="period" value="' + esc(period) + '" /><select name="searchField" style="padding:4px 6px;font-size:12px;border:1px solid #d1d5db;border-radius:4px;">' + searchFieldOptions + '</select><input type="text" name="search" value="' + esc(searchKw) + '" placeholder="검색" style="padding:4px 8px;font-size:12px;width:140px;border:1px solid #d1d5db;border-radius:4px;margin-left:4px;" /><label style="margin-left:6px;font-size:11px;color:#374151;"><input type="checkbox" name="successOnly" value="1"' + (successOnly ? ' checked' : '') + ' /> 성공건만</label><button type="submit" style="padding:4px 10px;font-size:12px;background:#2563eb;color:#fff;border:none;border-radius:4px;cursor:pointer;margin-left:4px;">검색</button></form>';

  const dateForm = '<form method="get" action="' + baseUrl + '" class="tx-date-form"><input type="hidden" name="sort" value="' + esc(sortBy) + '" /><input type="hidden" name="sortDir" value="' + esc(sortDir) + '" /><input type="hidden" name="search" value="' + esc(searchKw) + '" /><input type="hidden" name="searchField" value="' + esc(searchField) + '" /><input type="hidden" name="perPage" value="' + esc(perPage) + '" /><input type="hidden" name="env" value="' + esc(env) + '" /><input type="hidden" name="period" value="' + esc(period) + '" /><input type="hidden" name="successOnly" value="' + (successOnly ? '1' : '') + '" /><label class="tx-date-label">기간</label><input type="date" name="dateFrom" value="' + esc(dateFrom) + '" class="tx-date-input" /><span class="tx-date-sep">~</span><input type="date" name="dateTo" value="' + esc(dateTo) + '" class="tx-date-input" /><button type="submit" class="tx-date-btn">적용</button></form>';
  const exportUrl = baseUrl + '/export' + qs({ page: 1 });
  const excelBtn = '<a href="' + exportUrl + '" style="margin-left:auto;padding:6px 12px;font-size:12px;background:#0d9488;color:#fff;border-radius:4px;text-decoration:none;">Excel로 Export</a>';
  const toolbarHtml = '<div style="margin-bottom:10px;font-size:12px;display:flex;flex-wrap:nowrap;align-items:center;gap:6px;overflow-x:auto;white-space:nowrap;">' + sortLinks + sortDirLinks + '<span style="margin-left:8px;font-size:11px;color:#374151;">기간:</span>' + periodLinks + dateForm + searchForm + excelBtn + '</div>';
  const LEGEND_MAX_DESC = 80;
  const truncDesc = (s) => { const t = String(s || '').trim(); return t.length <= LEGEND_MAX_DESC ? t : t.slice(0, LEGEND_MAX_DESC - 1) + '…'; };
  const legendItems = [
    { label: t(locale, 'tx_status_fail'), desc: truncDesc(t(locale, 'tx_desc_fail')) },
    { label: t(locale, 'tx_status_cancel'), desc: truncDesc(t(locale, 'tx_desc_cancel')) },
    { label: t(locale, 'tx_status_void_auto'), desc: truncDesc(t(locale, 'tx_desc_void_auto')) },
    { label: t(locale, 'tx_status_void_manual'), desc: truncDesc(t(locale, 'tx_desc_void_manual')) },
    { label: t(locale, 'tx_status_refund_auto'), desc: truncDesc(t(locale, 'tx_desc_refund_auto')) },
    { label: t(locale, 'tx_status_refund_manual'), desc: truncDesc(t(locale, 'tx_desc_refund_manual')) },
    { label: t(locale, 'tx_status_force_void'), desc: truncDesc(t(locale, 'tx_desc_force_void')) },
    { label: t(locale, 'tx_status_force_refund'), desc: truncDesc(t(locale, 'tx_desc_force_refund')) },
  ];
  const legendCells = (() => {
    const parts = [];
    for (let i = 0; i < 4; i++) {
      parts.push('<div class="tx-legend-term-cell">' + esc(legendItems[i].label) + '</div><div class="tx-legend-desc-cell">' + esc(legendItems[i].desc) + '</div>');
      parts.push('<div class="tx-legend-term-cell">' + esc(legendItems[i + 4].label) + '</div><div class="tx-legend-desc-cell">' + esc(legendItems[i + 4].desc) + '</div>');
    }
    return parts.join('');
  })();
  const legendHtml = '<div class="card tx-legend" style="margin-bottom:16px;"><div class="tx-legend-grid">' + legendCells + '</div></div>';
  const txColDefaults = [40, 72, 95, 52, 95, 100, 120, 80, 78, 58, 88, 42, 98, 68, 52, 200];
  const colgroupHtml = '<colgroup>' + thLabels.map((_, i) => '<col id="tx-col-' + i + '" style="width:' + (txColDefaults[i] || 80) + 'px;min-width:40px;">').join('') + '</colgroup>';
  const thead = '<thead><tr>' + thLabels.map((l, i) => '<th class="col-body-key">' + esc(l) + '<div class="tx-col-resizer" data-col="' + i + '" title="드래그하여 열 너비 조절"></div></th>').join('') + '</tr></thead>';
  const rows = list.map((log, index) => {
    const body = parseNotiBody(log);
    const ps = body.PaymentStatus ?? body.paymentStatus ?? body.status;
    const isCancel = isDefinitelyCancelPaymentStatus(ps);
    const isSuccess = isSuccessPaymentBody(body);
    const isError = isErrorPaymentStatus(ps);
    let statusKey = 'tx_status_fail';
    let statusClass = 'tx-status-fail';
    if (isCancel) {
      statusKey = 'tx_status_cancel';
      statusClass = 'tx-status-cancel';
    } else if (isSuccess) {
      statusKey = 'tx_status_paid';
      statusClass = 'tx-status-success';
    } else if (isError) {
      statusKey = 'tx_status_error';
      statusClass = 'tx-status-fail';
    }
    const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : '');
    let notiLabel = '-';
    if (txId) {
      if (hasVoidNotiSent(txId)) notiLabel = t(locale, 'cr_type_void');      // 무효(자동/수동/강제 구분 없이)
      else if (hasRefundNotiSent(txId)) notiLabel = t(locale, 'cr_type_refund');  // 환불
    }
    const isVoidedOrRefunded = isSuccess && txId && (hasVoidNotiSent(txId) || hasRefundNotiSent(txId));
    const rowClass = isVoidedOrRefunded ? ' tx-row-voided-refunded' : '';
    const badgeClass = statusClass.replace('tx-status-', 'tx-badge-');
    const dt = formatDateAndTimeTHJP(log.receivedAtIso || log.receivedAt);
    const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
    const routeNoDisplay = getRouteNoDisplay(merchant, log.routeKey);
    const cells = [];
    for (const col of TRANSACTION_LIST_COLUMNS) {
      if (col.type === 'fixed') {
        if (col.id === 'no') cells.push('<td class="col-no">' + esc(String((pageNum - 1) * perPage + index + 1)) + '</td>');
        else if (col.id === 'received_date') cells.push('<td class="col-date">' + esc(dt.date) + '</td>');
        else if (col.id === 'received_time') cells.push('<td class="col-time">TH:' + esc(dt.timeTh) + ' JP:' + esc(dt.timeJp) + '</td>');
        else if (col.id === 'route_no') cells.push('<td class="col-route">' + esc(routeNoDisplay) + '</td>');
        else if (col.id === 'merchant') cells.push('<td class="col-merchant">' + esc(log.merchantId || '') + '</td>');
        else if (col.id === 'internal_amount') {
          const amt = getBodyVal(body, ['Amount', 'amount']);
          const internalAmt = (amt != null && amt !== '' && !isVoidedOrRefunded) ? (Number(amt) / 100) : null;
          cells.push('<td class="col-internal-amount">' + esc(formatAmountWithSeparator(internalAmt)) + '</td>');
        } else if (col.id === 'status') cells.push('<td class="col-narrow"><span class="tx-badge ' + esc(badgeClass) + '">' + esc(t(locale, statusKey)) + '</span></td>');
        else if (col.id === 'noti') cells.push('<td class="col-narrow"><span class="tx-badge tx-badge-noti">' + esc(notiLabel) + '</span></td>');
        else if (col.id === 'void_refund_detail') {
          let detailHtml = '-';
          if (isVoidedOrRefunded && txId) {
            const entry = voidRefundByTxId[txId];
            const isVoid = hasVoidNotiSent(txId);
            const kindLabel = isVoid ? '무효' : '환불';
            const defaultReason = isVoid ? '관리자 무효 요청' : '관리자 환불 요청';
            const reason = (entry && entry.reason) ? entry.reason : defaultReason;
            if (entry && entry.sentAtIso) {
              const sentStr = formatVoidRefundSentAt(entry.sentAtIso);
              const relayStr = entry.relayStatus ? ' 가맹점:' + entry.relayStatus : '';
              const internalStr = entry.internalStatus ? ' 전산:' + entry.internalStatus : '';
              detailHtml = '내역: ' + kindLabel + ' 처리 ' + sentStr + relayStr + internalStr + ' / 사유: ' + reason;
            } else {
              detailHtml = '내역: ' + kindLabel + ' 처리됨 / 사유: ' + reason;
            }
          }
          cells.push('<td class="col-void-refund-detail">' + esc(detailHtml) + '</td>');
        }
      } else {
        let v = getBodyVal(body, col.keys);
        const isAmountCol = col.keys && (col.keys[0] === 'Amount' || col.keys[0] === 'amount');
        if (isAmountCol && isVoidedOrRefunded) v = '-';
        else if (isAmountCol) v = formatAmountWithSeparator(v);
        else if (col.display === 'currency') v = formatCurrencyForDisplay(v) ?? v;
        const keyName = col.keys && col.keys[0] ? col.keys[0] : 'body';
        const cellClass = 'col-' + (keyName === 'paymentDescription' || keyName === 'PaymentDescription' ? 'Description' : keyName);
        cells.push('<td class="' + cellClass + '" style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + formatVal(v) + '</td>');
      }
    }
    return '<tr' + (rowClass ? ' class="' + rowClass.trim() + '"' : '') + '>' + cells.join('') + '</tr>';
  }).join('');
  const pageLinks = [];
  for (let i = 1; i <= totalPages; i++) {
    const url = baseUrl + qs({ page: i });
    pageLinks.push('<a href="' + url + '" style="padding:4px 8px;margin:0 2px;font-size:12px;border-radius:4px;text-decoration:none;background:' + (i === pageNum ? '#2563eb' : '#e5e7eb') + ';color:' + (i === pageNum ? '#fff' : '#374151') + ';">' + i + '</a>');
  }
  const paginationCenter = totalPages > 0 ? '<div style="text-align:center;margin:12px 0;">' + pageLinks.join('') + '</div>' : '';
  const perPageOptions = [10, 25, 50, 100].map((n) => '<a href="' + baseUrl + qs({ perPage: n, page: 1 }) + '" style="padding:4px 8px;margin:0 2px;font-size:12px;border-radius:4px;text-decoration:none;background:' + (perPage === n ? '#059669' : '#e5e7eb') + ';color:' + (perPage === n ? '#fff' : '#374151') + ';">' + n + '</a>').join('');
  const perPageBar = '<div style="margin-top:12px;font-size:12px;color:#4b5563;">한 번에 보기: ' + perPageOptions + ' 건 (총 ' + totalCount + '건)</div>';
  const txResizeScript = '<script>(function(){var table=document.querySelector(".tx-list-table");if(!table)return;var cols=table.querySelectorAll("col");var headers=table.querySelectorAll("thead th");var resizer=null,startX=0,startW=0,colIdx=0;function onMove(e){if(resizer==null)return;var dx=e.clientX-startX;var newW=Math.max(40,startW+dx);cols[colIdx].style.width=newW+"px";}function onUp(){resizer=null;document.removeEventListener("mousemove",onMove);document.removeEventListener("mouseup",onUp);document.body.style.cursor="";document.body.style.userSelect="";}table.querySelectorAll(".tx-col-resizer").forEach(function(el){el.addEventListener("mousedown",function(e){e.preventDefault();colIdx=parseInt(el.getAttribute("data-col"),10);startX=e.clientX;startW=headers[colIdx]?headers[colIdx].offsetWidth:80;resizer=el;document.body.style.cursor="col-resize";document.body.style.userSelect="none";document.addEventListener("mousemove",onMove);document.addEventListener("mouseup",onUp);});});})();</script>';
  const txHint = '<p class="hint" style="margin-bottom:10px;font-size:13px;color:#6b7280;">로그분석(PG 노티 로그)과 동일한 수신 건을 표시합니다. 성공/취소/실패 모두 포함됩니다. 기간·검색 조건을 적용하면 건수가 줄어들 수 있으니, 건이 안 보이면 기간을 넓히거나 검색을 지워보세요.</p>';
  const emptyStateHtml = totalCount === 0
    ? '<div class="alert" style="margin-bottom:14px;padding:12px 16px;background:#eff6ff;border:1px solid #93c5fd;border-radius:8px;color:#1e40af;font-size:13px;"><strong>표시할 노티 거래가 없습니다.</strong> 최근 7일 이내에 PG에서 수신한 노티만 표시됩니다. <a href="/admin/logs">로그분석(피지 노티)</a>에서 수신 건이 있는지 확인하고, 위에서 env(PRODUCTION/SANDBOX)를 선택해 보세요.</div>'
    : '';
  const tableContent = emptyStateHtml + txHint + legendHtml + toolbarHtml + '<table class="tx-list-table">' + colgroupHtml + thead + '<tbody>' + rows + '</tbody></table>' + txResizeScript + paginationCenter + perPageBar;
  res.send(renderCancelRefundPage(locale, adminUser, t(locale, 'nav_transaction_list') + ' (' + totalCount + ')', tableContent, '', req.originalUrl, req.session.member, req, undefined, getEnvFromReq(req)));
});

// ChillPay API 거래 내역: 프로덕션/샌드박스 각각 Search Payment Transaction으로 조회
app.get('/admin/transactions/chillpay-api', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const q = req.query || {};
  const env = getEnvFromReq(req);
  const useSandbox = env === 'sandbox';
  const baseUrl = '/admin/transactions/chillpay-api';
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const qs = (overrides) => {
    const o = { env, ...overrides };
    const parts = [];
    if (o.env) parts.push('env=' + encodeURIComponent(o.env));
    if (o.dateFrom) parts.push('dateFrom=' + encodeURIComponent(o.dateFrom));
    if (o.dateTo) parts.push('dateTo=' + encodeURIComponent(o.dateTo));
    if (o.pageSize) parts.push('pageSize=' + encodeURIComponent(o.pageSize));
    if (o.pageNumber) parts.push('pageNumber=' + encodeURIComponent(o.pageNumber));
    if (o.searchKeyword) parts.push('searchKeyword=' + encodeURIComponent(o.searchKeyword));
    if (o.orderNo) parts.push('orderNo=' + encodeURIComponent(o.orderNo));
    if (o.status) parts.push('status=' + encodeURIComponent(o.status));
    return parts.length ? '?' + parts.join('&') : '';
  };
  // YYYY-MM-DD → dd/MM/yyyy HH:mm:ss (ChillPay API 형식)
  const formatYmdForChillPay = (ymdStr, timePart) => {
    if (!ymdStr || !/^\d{4}-\d{2}-\d{2}$/.test(String(ymdStr).trim())) return '';
    const [y, m, d] = String(ymdStr).trim().split('-');
    return d + '/' + m + '/' + y + ' ' + (timePart || '00:00:00');
  };
  const dateFrom = (q.dateFrom || '').toString().trim();
  const dateTo = (q.dateTo || '').toString().trim();
  const pageSize = Math.max(1, Math.min(100, parseInt(q.pageSize, 10) || 50));
  const pageNumber = Math.max(1, parseInt(q.pageNumber, 10) || 1);
  const searchKeyword = (q.searchKeyword || '').toString().trim();
  const orderNo = (q.orderNo || '').toString().trim();
  const status = (q.status || '').toString().trim();

  let resultHtml = '';
  let alertHtml = '';
  const didFetch = dateFrom || dateTo || searchKeyword || orderNo || status || q.fetch === '1';
  if (didFetch) {
    const transactionDateFrom = formatYmdForChillPay(dateFrom, '00:00:00');
    const transactionDateTo = formatYmdForChillPay(dateTo, '23:59:59');
    const apiParams = {
      orderBy: 'TransactionId',
      orderDir: 'DESC',
      pageSize: String(pageSize),
      pageNumber: String(pageNumber),
      searchKeyword,
      orderNo,
      status,
      transactionDateFrom,
      transactionDateTo,
      paymentDateFrom: '',
      paymentDateTo: '',
    };
    const result = await chillPaySearchPayment(useSandbox, apiParams);
    if (!result.success) {
      alertHtml = '<div class="alert alert-error" style="padding:10px;margin-bottom:12px;background:#fef2f2;border:1px solid #fecaca;border-radius:6px;color:#b91c1c;">ChillPay API 오류: ' + esc(result.error) + '</div>';
    } else {
      const list = result.data || [];
      const totalRecord = result.totalRecord != null ? result.totalRecord : list.length;
      const filteredRecord = result.filteredRecord != null ? result.filteredRecord : list.length;
      const thLabels = ['TransactionId', '거래일시', 'Merchant', 'Customer', 'OrderNo', 'PaymentChannel', '결제일시', 'Amount', 'Fee', 'Discount', 'TotalAmount', 'Currency', 'RouteNo', 'Status', 'Settled'];
      const thead = '<thead><tr>' + thLabels.map((l) => '<th style="text-align:center;padding:6px 8px;border:1px solid #e5e7eb;background:#f3f4f6;">' + esc(l) + '</th>').join('') + '</tr></thead>';
      const rows = list.map((row) => {
        const cells = [
          row.transactionId,
          row.transactionDate || '-',
          row.merchant || '-',
          row.customer || '-',
          row.orderNo || '-',
          row.paymentChannel || '-',
          row.paymentDate || '-',
          row.amount != null ? row.amount : '-',
          row.fee != null ? row.fee : '-',
          row.discount != null ? row.discount : '-',
          row.totalAmount != null ? row.totalAmount : '-',
          row.currency || '-',
          row.routeNo != null ? row.routeNo : '-',
          row.status || '-',
          row.settled === true ? 'Y' : (row.settled === false ? 'N' : '-'),
        ];
        return '<tr>' + cells.map((c) => '<td style="text-align:center;padding:6px 8px;border:1px solid #e5e7eb;">' + esc(String(c)) + '</td>').join('') + '</tr>';
      }).join('');
      resultHtml = '<div style="margin-top:14px;"><p style="font-size:13px;color:#374151;">총 ' + totalRecord + '건 (이번 페이지 ' + filteredRecord + '건)</p><table style="width:100%;border-collapse:collapse;font-size:12px;">' + thead + '<tbody>' + rows + '</tbody></table></div>';
      const totalPages = Math.max(1, Math.ceil(totalRecord / pageSize));
      const pageLinks = [];
      for (let i = 1; i <= Math.min(totalPages, 20); i++) {
        const url = baseUrl + qs({ dateFrom, dateTo, pageSize, pageNumber: i, searchKeyword, orderNo, status });
        pageLinks.push('<a href="' + url + '" style="padding:4px 8px;margin:0 2px;font-size:12px;border-radius:4px;text-decoration:none;background:' + (i === pageNumber ? '#2563eb' : '#e5e7eb') + ';color:' + (i === pageNumber ? '#fff' : '#374151') + ';">' + i + '</a>');
      }
      if (totalPages > 1) resultHtml += '<div style="text-align:center;margin:12px 0;">' + pageLinks.join('') + '</div>';
    }
  }

  const formHtml = '<form method="get" action="' + baseUrl + '" style="margin-bottom:16px;"><input type="hidden" name="env" value="' + esc(env) + '" /><input type="hidden" name="fetch" value="1" />'
    + '<label style="margin-right:6px;">기간 (TransactionDate)</label><input type="date" name="dateFrom" value="' + esc(dateFrom) + '" style="padding:4px 8px;margin-right:4px;" />'
    + ' ~ <input type="date" name="dateTo" value="' + esc(dateTo) + '" style="padding:4px 8px;margin-left:4px;margin-right:12px;" />'
    + '<label style="margin-left:8px;margin-right:4px;">페이지크기</label><input type="number" name="pageSize" min="1" max="100" value="' + esc(String(pageSize)) + '" style="width:60px;padding:4px 8px;margin-right:12px;" />'
    + '<label style="margin-right:4px;">OrderNo</label><input type="text" name="orderNo" value="' + esc(orderNo) + '" style="width:120px;padding:4px 8px;margin-right:12px;" />'
    + '<label style="margin-right:4px;">Status</label><input type="text" name="status" value="' + esc(status) + '" style="width:80px;padding:4px 8px;margin-right:12px;" />'
    + '<label style="margin-right:4px;">검색어</label><input type="text" name="searchKeyword" value="' + esc(searchKeyword) + '" style="width:120px;padding:4px 8px;margin-right:12px;" />'
    + '<button type="submit" style="padding:6px 14px;background:#2563eb;color:#fff;border:none;border-radius:6px;cursor:pointer;">ChillPay에서 가져오기</button></form>';
  const backLink = '<p style="margin-bottom:12px;"><a href="/admin/transactions' + (env ? '?env=' + encodeURIComponent(env) : '') + '" style="color:#2563eb;">← 노티거래내역으로 돌아가기</a></p>';
  const mainContent = backLink + formHtml + alertHtml + resultHtml;
  res.send(renderCancelRefundPage(locale, adminUser, 'ChillPay API 거래 내역', mainContent, '', req.originalUrl, req.session.member, req, undefined, env));
});

// 피지거래내역: 동기화 버튼(오른쪽), 동기화 초기화(빨강), SORT, 검색, 일자별 노출
app.post('/admin/pg-transactions/sync', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const bodyEnv = req.body && req.body.env ? String(req.body.env).trim() : '';
  const env = bodyEnv || getPgEnvFromReq(req);
  const envKey = env === 'sandbox' ? 'sandbox' : 'production';
  const cfg = loadChillPayTransactionConfig();
  const cred = envKey === 'sandbox' ? cfg.sandbox : cfg.production;
  if (!cred.mid || !cred.apiKey || !cred.md5) {
    return res.redirect('/admin/pg-transactions?env=' + encodeURIComponent(envKey === 'sandbox' ? 'sandbox' : 'live') + '&sync=no_credentials');
  }
  try {
    await runPgTransactionFetchAsync();
  } catch (e) {
    console.error('[PG transactions] sync error', e && e.message);
  }
  return res.redirect('/admin/pg-transactions?env=' + encodeURIComponent(envKey === 'sandbox' ? 'sandbox' : 'live'));
});

app.get('/admin/pg-transactions/reset', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const env = (req.query && req.query.env) ? String(req.query.env).trim() : getPgEnvFromReq(req);
  const envKey = env === 'sandbox' ? 'sandbox' : 'production';
  const envLabel = envKey === 'sandbox' ? 'SANDBOX' : 'PRODUCTION';
  const cfg = loadChillPayTransactionConfig();
  const months = Number.isFinite(cfg.pgTransactionInitialSyncMonths) && cfg.pgTransactionInitialSyncMonths > 0 ? cfg.pgTransactionInitialSyncMonths : 3;
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const backUrl = '/admin/pg-transactions?env=' + encodeURIComponent(envKey === 'sandbox' ? 'sandbox' : 'live');
  const mainContent = '<div class="card" style="max-width:520px;"><p style="margin:0 0 16px 0;color:#1f2937;">동기화가 초기화 됩니다.</p><p style="margin:0 0 20px 0;color:#6b7280;font-size:14px;">해당 환경(' + esc(envLabel) + ')의 <strong>기존 데이터가 삭제</strong>되고, 설정한 과거 ' + months + '개월 데이터만 덮어 씁니다. 초기화 전 데이터는 최신 2회까지 자동 저장되며 복원에서 되돌릴 수 있습니다.</p>'
    + '<form method="post" action="/admin/pg-transactions/reset"><input type="hidden" name="env" value="' + esc(envKey) + '" /><button type="submit" style="padding:8px 16px;background:#dc2626;color:#fff;border:none;border-radius:6px;cursor:pointer;font-weight:600;">다시 확인</button></form>'
    + '<p style="margin-top:16px;"><a href="' + backUrl + '" style="color:#2563eb;">← 피지거래내역으로 돌아가기</a></p></div>';
  res.send(renderCancelRefundPage(locale, adminUser, '동기화 초기화', mainContent, '', req.originalUrl, req.session.member, req, undefined, envKey === 'sandbox' ? 'sandbox' : 'live'));
});

app.post('/admin/pg-transactions/reset', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const envKey = (req.body && req.body.env) ? String(req.body.env).trim() : 'live';
  const env = envKey === 'sandbox' ? 'sandbox' : 'live';
  const store = loadPgTransactionStore();
  pushResetBackup(store);
  if (envKey === 'sandbox') {
    store.sandbox.byDate = {};
    store.sandbox.lastFetchedAt = null;
  } else {
    store.production.byDate = {};
    store.production.lastFetchedAt = null;
  }
  savePgTransactionStore(store);
  try {
    await runPgTransactionFetchAsync();
  } catch (e) {
    console.error('[PG transactions] reset sync error', e && e.message);
  }
  return res.redirect('/admin/pg-transactions?env=' + encodeURIComponent(env));
});

app.post('/admin/pg-transactions/backup', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const store = loadPgTransactionStore();
  const backups = loadPgTransactionBackups();
  backups.manualBackup = {
    savedAt: new Date().toISOString(),
    production: JSON.parse(JSON.stringify(store.production || { lastFetchedAt: null, byDate: {} })),
    sandbox: JSON.parse(JSON.stringify(store.sandbox || { lastFetchedAt: null, byDate: {} })),
  };
  savePgTransactionBackups(backups);
  const env = (req.body && req.body.env) ? String(req.body.env).trim() : 'live';
  return res.redirect('/admin/pg-transactions?env=' + encodeURIComponent(env === 'sandbox' ? 'sandbox' : 'live') + '&backup=ok');
});

app.get('/admin/pg-transactions/restore', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const backups = loadPgTransactionBackups();
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const backUrl = '/admin/pg-transactions';
  const formatSavedAt = (iso) => { try { const d = new Date(iso); return isNaN(d.getTime()) ? iso : d.toLocaleString('ko-KR', { hour12: false }); } catch { return iso; } };
  let optionsHtml = '';
  if (backups.resetBackups.length > 0) {
    optionsHtml += backups.resetBackups.map((b, i) => '<p style="margin:8px 0;"><form method="post" action="/admin/pg-transactions/restore" style="display:inline;"><input type="hidden" name="backupType" value="reset' + (i + 1) + '" />초기화 전 백업 ' + (i + 1) + ' (' + esc(formatSavedAt(b.savedAt)) + ') <button type="submit" style="padding:4px 10px;background:#059669;color:#fff;border:none;border-radius:4px;cursor:pointer;">복원</button></form></p>').join('');
  } else {
    optionsHtml += '<p style="color:#6b7280;">초기화 전 자동 백업이 없습니다.</p>';
  }
  if (backups.manualBackup) {
    optionsHtml += '<p style="margin:16px 0 8px 0;font-weight:600;">수동 백업</p><p style="margin:8px 0;"><form method="post" action="/admin/pg-transactions/restore" style="display:inline;"><input type="hidden" name="backupType" value="manual" />' + esc(formatSavedAt(backups.manualBackup.savedAt)) + ' <button type="submit" style="padding:4px 10px;background:#059669;color:#fff;border:none;border-radius:4px;cursor:pointer;">복원</button></form></p>';
  } else {
    optionsHtml += '<p style="margin-top:16px;color:#6b7280;">수동 백업이 없습니다. 피지거래내역에서 백업 버튼으로 저장할 수 있습니다.</p>';
  }
  const mainContent = '<div class="card" style="max-width:520px;"><h2 style="margin:0 0 12px 0;">데이터 복원</h2><p style="margin:0 0 16px 0;color:#6b7280;">복원하면 현재 거래 내역이 선택한 백업으로 덮어 씁니다.</p><h3 style="margin:16px 0 8px 0;">초기화 전 자동 백업 (최신 2회)</h3>' + optionsHtml + '<p style="margin-top:20px;"><a href="' + backUrl + '" style="color:#2563eb;">← 피지거래내역으로 돌아가기</a></p></div>';
  res.send(renderCancelRefundPage(locale, adminUser, '복원', mainContent, '', req.originalUrl, req.session.member, req, undefined, getEnvFromReq(req)));
});

app.post('/admin/pg-transactions/restore', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const backupType = (req.body && req.body.backupType) ? String(req.body.backupType).trim() : '';
  const backups = loadPgTransactionBackups();
  let store = null;
  if (backupType === 'manual' && backups.manualBackup) {
    store = { production: JSON.parse(JSON.stringify(backups.manualBackup.production)), sandbox: JSON.parse(JSON.stringify(backups.manualBackup.sandbox)) };
  } else if (backupType === 'reset1' && backups.resetBackups[0]) {
    store = { production: JSON.parse(JSON.stringify(backups.resetBackups[0].production)), sandbox: JSON.parse(JSON.stringify(backups.resetBackups[0].sandbox)) };
  } else if (backupType === 'reset2' && backups.resetBackups[1]) {
    store = { production: JSON.parse(JSON.stringify(backups.resetBackups[1].production)), sandbox: JSON.parse(JSON.stringify(backups.resetBackups[1].sandbox)) };
  }
  if (store) {
    savePgTransactionStore(store);
    return res.redirect('/admin/pg-transactions?restore=ok');
  }
  return res.redirect('/admin/pg-transactions/restore');
});

app.get('/admin/pg-transactions', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const env = getPgEnvFromReq(req);
  const q = req.query || {};
  const periodSort = (q.sort || 'today').toString();
  const sortDir = (q.sortDir || 'desc').toString().toLowerCase() === 'asc' ? 'asc' : 'desc';
  const orderBy = (q.orderBy || 'date').toString();
  const searchKw = (q.search || '').toString().trim();
  const searchField = (q.searchField || 'all').toString();
  let dateFrom = (q.dateFrom || '').toString().trim();
  let dateTo = (q.dateTo || '').toString().trim();
  let store = loadPgTransactionStore();
  let block = env === 'sandbox' ? store.sandbox : store.production;
  let byDate = block.byDate || {};
  let lastFetchedAt = block.lastFetchedAt || null;
  const cfg = loadChillPayTransactionConfig();
  const cred = env === 'sandbox' ? cfg.sandbox : cfg.production;
  const hasCreds = !!(cred && cred.mid && cred.apiKey && cred.md5);
  // 최초 진입 시 데이터가 전혀 없고 자격증명이 있다면, 동기화를 한 번 수행해서 바로 채운다.
  if (!lastFetchedAt && (!byDate || Object.keys(byDate).length === 0) && hasCreds) {
    try {
      await runPgTransactionFetchAsync();
      store = loadPgTransactionStore();
      block = env === 'sandbox' ? store.sandbox : store.production;
      byDate = block.byDate || {};
      lastFetchedAt = block.lastFetchedAt || null;
    } catch (e) {
      console.error('[PG transactions] initial fetch from /admin/pg-transactions failed', e && e.message);
    }
  }
  const syncIntervalMin = Number.isFinite(cfg.pgTransactionSyncIntervalMinutes) && cfg.pgTransactionSyncIntervalMinutes > 0 ? cfg.pgTransactionSyncIntervalMinutes : 30;
  // 당월/전월 등 기간 버튼은 서버 로컬 날짜 기준으로 계산 (타임존 변환 오류 방지)
  const now = new Date();
  const y = now.getFullYear();
  const m = now.getMonth();
  const d = now.getDate();
  const toYmd = (date) => date.getFullYear() + '-' + String(date.getMonth() + 1).padStart(2, '0') + '-' + String(date.getDate()).padStart(2, '0');
  const todayKey = toYmd(now);
  const yesterdayDate = new Date(y, m, d - 1);
  const yesterdayKey = toYmd(yesterdayDate);
  const dayOfWeek = now.getDay();
  const monOffset = dayOfWeek === 0 ? -6 : 1 - dayOfWeek;
  const thisWeekMon = new Date(y, m, d + monOffset);
  const thisWeekSun = new Date(thisWeekMon);
  thisWeekSun.setDate(thisWeekSun.getDate() + 6);
  const lastWeekMon = new Date(thisWeekMon);
  lastWeekMon.setDate(lastWeekMon.getDate() - 7);
  const lastWeekSun = new Date(lastWeekMon);
  lastWeekSun.setDate(lastWeekSun.getDate() + 6);
  // 노티거래내역과 동일하게, 기간 프리셋(당일/전일/이번주/지난주/당월/전월)을 선택하면
  // 내부적으로 dateFrom/dateTo(YYYY-MM-DD)를 계산해 준다.
  if (!dateFrom && !dateTo && periodSort && periodSort !== 'all') {
    const today = new Date(y, m, d);
    const ymd = (dt) => toYmd(dt);
    let start = null;
    let end = null;
    if (periodSort === 'today') {
      start = new Date(today);
      end = new Date(today);
    } else if (periodSort === 'yesterday') {
      start = new Date(today);
      start.setDate(start.getDate() - 1);
      end = new Date(start);
    } else if (periodSort === 'thisWeek') {
      start = new Date(thisWeekMon);
      end = new Date(thisWeekSun);
    } else if (periodSort === 'lastWeek') {
      start = new Date(lastWeekMon);
      end = new Date(lastWeekSun);
    } else if (periodSort === 'thisMonth') {
      start = new Date(y, m, 1);
      end = new Date(y, m + 1, 0);
    } else if (periodSort === 'lastMonth') {
      start = new Date(y, m - 1, 1);
      end = new Date(y, m, 0);
    }
    if (start && end) {
      dateFrom = ymd(start);
      dateTo = ymd(end);
    }
  }
  const allKeys = Object.keys(byDate).sort((a, b) => b.localeCompare(a));
  let periodDateKeys = allKeys;
  if (dateFrom || dateTo) {
    periodDateKeys = allKeys.filter((k) => (!dateFrom || k >= dateFrom) && (!dateTo || k <= dateTo));
  }
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const baseUrl = '/admin/pg-transactions';
  const qs = (overrides) => {
    const o = { env, sort: periodSort, sortDir, orderBy, search: searchKw, searchField, dateFrom, dateTo, ...overrides };
    const parts = [];
    if (o.env) parts.push('env=' + encodeURIComponent(o.env));
    if (o.sort && o.sort !== 'today') parts.push('sort=' + encodeURIComponent(o.sort));
    if (o.sortDir && o.sortDir !== 'desc') parts.push('sortDir=' + encodeURIComponent(o.sortDir));
    if (o.orderBy && o.orderBy !== 'date') parts.push('orderBy=' + encodeURIComponent(o.orderBy));
    if (o.search) parts.push('search=' + encodeURIComponent(o.search));
    if (o.searchField && o.searchField !== 'all') parts.push('searchField=' + encodeURIComponent(o.searchField));
    if (o.dateFrom) parts.push('dateFrom=' + encodeURIComponent(o.dateFrom));
    if (o.dateTo) parts.push('dateTo=' + encodeURIComponent(o.dateTo));
    return parts.length ? '?' + parts.join('&') : '';
  };
  const syncBtnHtml = '<form method="post" action="' + baseUrl + '/sync" style="display:inline;"><input type="hidden" name="env" value="' + esc(env) + '" /><button type="submit" style="padding:4px 10px;font-size:12px;background:#2563eb;color:#fff;border:none;border-radius:6px;cursor:pointer;">동기화</button></form>'
    + ' <a href="' + baseUrl + '/reset?env=' + encodeURIComponent(env) + '" style="margin-left:8px;padding:4px 10px;font-size:12px;background:#dc2626;color:#fff;border-radius:6px;text-decoration:none;">동기화 초기화</a>'
    + ' <form method="post" action="' + baseUrl + '/backup" style="display:inline;margin-left:8px;"><input type="hidden" name="env" value="' + esc(env) + '" /><button type="submit" style="padding:4px 10px;font-size:12px;background:#0d9488;color:#fff;border:none;border-radius:6px;cursor:pointer;">백업</button></form>'
    + ' <a href="' + baseUrl + '/restore" style="margin-left:8px;padding:4px 10px;font-size:12px;background:#6b7280;color:#fff;border-radius:6px;text-decoration:none;">복원</a>';
  let alertPgHtml = '';
  if (q.backup === 'ok') alertPgHtml = '<div style="padding:10px;margin-bottom:12px;background:#d1fae5;border:1px solid #6ee7b7;border-radius:6px;color:#065f46;">백업이 저장되었습니다.</div>';
  if (q.restore === 'ok') alertPgHtml = '<div style="padding:10px;margin-bottom:12px;background:#d1fae5;border:1px solid #6ee7b7;border-radius:6px;color:#065f46;">복원이 완료되었습니다.</div>';
  if (q.sync === 'no_credentials') {
    alertPgHtml = '<div style="padding:10px;margin-bottom:12px;background:#fef2f2;border:1px solid #fecaca;border-radius:6px;color:#991b1b;">해당 환경의 ChillPay 키(Mid/ApiKey/MD5)가 없습니다. <a href="/admin/settings">환경설정</a>에서 ChillPay 환경 설정을 저장한 뒤 다시 동기화해 주세요.</div>';
  } else {
    const lastApiError = env === 'sandbox' ? PG_TRANSACTIONS_LAST_ERROR_SANDBOX : PG_TRANSACTIONS_LAST_ERROR_PROD;
    if (lastApiError) {
      alertPgHtml = '<div style="padding:10px;margin-bottom:12px;background:#fef2f2;border:1px solid #fecaca;border-radius:6px;color:#991b1b;">ChillPay Search Payment API 오류: ' + esc(lastApiError) + '</div>';
    }
  }
  const lastFetchedHtml = lastFetchedAt
    ? '<p style="font-size:10px;color:#6b7280;margin-bottom:8px;">마지막 조회: ' + esc(new Date(lastFetchedAt).toLocaleString('ko-KR', { hour12: false })) + ' (' + syncIntervalMin + '분마다 자동 조회)</p>'
    : '<p style="font-size:10px;color:#6b7280;margin-bottom:8px;">아직 조회된 내역이 없습니다. ' + syncIntervalMin + '분마다 자동 조회됩니다.</p>';
  const orderByOptions = [
    { key: 'date', label: '일자별' },
    { key: 'routeNo', label: 'Route' },
    { key: 'currency', label: 'Currency' },
    { key: 'status', label: '상태' },
  ];
  const orderByLinks = orderByOptions.map((o) => {
    const url = baseUrl + qs({ orderBy: o.key });
    const active = orderBy === o.key;
    return '<a href="' + url + '" style="padding:2px 8px;font-size:10px;border-radius:4px;text-decoration:none;background:' + (active ? '#2563eb' : '#e5e7eb') + ';color:' + (active ? '#fff' : '#374151') + ';margin-right:2px;">' + esc(o.label) + '</a>';
  }).join('');
  const sortDirLinks = '<a href="' + baseUrl + qs({ sortDir: 'asc' }) + '" style="padding:2px 6px;font-size:10px;border-radius:4px;text-decoration:none;background:' + (sortDir === 'asc' ? '#2563eb' : '#e5e7eb') + ';color:' + (sortDir === 'asc' ? '#fff' : '#374151') + ';">오름차순</a><a href="' + baseUrl + qs({ sortDir: 'desc' }) + '" style="padding:2px 6px;font-size:10px;border-radius:4px;text-decoration:none;margin-left:2px;background:' + (sortDir === 'desc' ? '#2563eb' : '#e5e7eb') + ';color:' + (sortDir === 'desc' ? '#fff' : '#374151') + ';">내림차순</a>';
  const periodOptions = [
    { key: 'today', label: '당일' },
    { key: 'yesterday', label: '전일' },
    { key: 'thisWeek', label: '이번주' },
    { key: 'lastWeek', label: '지난주' },
    { key: 'thisMonth', label: '당월' },
    { key: 'lastMonth', label: '전월' },
    { key: 'all', label: '전체' },
  ];
  const periodLinks = periodOptions.map((o) => {
    const url = baseUrl + qs({ sort: o.key, dateFrom: '', dateTo: '' });
    const active = periodSort === o.key;
    return '<a class="pg-period-link" data-period="' + esc(o.key) + '" href="' + url + '" style="padding:2px 8px;font-size:10px;border-radius:4px;text-decoration:none;background:' + (active ? '#2563eb' : '#e5e7eb') + ';color:' + (active ? '#fff' : '#374151') + ';margin-right:2px;">' + esc(o.label) + '</a>';
  }).join('');
  const pgSearchFields = [
    { key: 'all', label: '전체' },
    { key: 'TransactionId', label: 'TransactionId' },
    { key: 'OrderNo', label: 'OrderNo' },
    { key: 'Customer', label: 'Customer' },
    { key: 'Merchant', label: 'Merchant' },
    { key: 'RouteNo', label: 'RouteNo' },
    { key: 'Currency', label: 'Currency' },
    { key: 'Status', label: 'Status' },
    { key: 'Amount', label: 'Amount' },
  ];
  const searchFieldOptions = pgSearchFields.map((f) => '<option value="' + esc(f.key) + '"' + (searchField === f.key ? ' selected' : '') + '>' + esc(f.label) + '</option>').join('');
  const dateForm = '<form method="get" action="' + baseUrl + '" style="display:inline;margin-left:4px;"><input type="hidden" name="env" value="' + esc(env) + '" /><input type="hidden" name="sort" value="' + esc(periodSort) + '" /><input type="hidden" name="sortDir" value="' + esc(sortDir) + '" /><input type="hidden" name="orderBy" value="' + esc(orderBy) + '" /><input type="hidden" name="search" value="' + esc(searchKw) + '" /><input type="hidden" name="searchField" value="' + esc(searchField) + '" /><label style="margin-right:2px;font-size:10px;color:#374151;">기간</label><input type="date" name="dateFrom" value="' + esc(dateFrom) + '" style="padding:2px 4px;font-size:10px;border:1px solid #d1d5db;border-radius:4px;" /><span style="margin:0 2px;">~</span><input type="date" name="dateTo" value="' + esc(dateTo) + '" style="padding:2px 4px;font-size:10px;border:1px solid #d1d5db;border-radius:4px;" /><button type="submit" style="padding:2px 8px;font-size:10px;background:#2563eb;color:#fff;border:none;border-radius:4px;cursor:pointer;margin-left:2px;">적용</button></form>';
  const searchForm = '<form method="get" action="' + baseUrl + '" style="display:inline;margin-left:4px;"><input type="hidden" name="env" value="' + esc(env) + '" /><input type="hidden" name="sort" value="' + esc(periodSort) + '" /><input type="hidden" name="sortDir" value="' + esc(sortDir) + '" /><input type="hidden" name="orderBy" value="' + esc(orderBy) + '" /><input type="hidden" name="dateFrom" value="' + esc(dateFrom) + '" /><input type="hidden" name="dateTo" value="' + esc(dateTo) + '" /><select name="searchField" style="padding:2px 4px;font-size:10px;border:1px solid #d1d5db;border-radius:4px;">' + searchFieldOptions + '</select><input type="text" name="search" value="' + esc(searchKw) + '" placeholder="검색" style="padding:2px 6px;font-size:10px;width:100px;border:1px solid #d1d5db;border-radius:4px;margin-left:2px;" /><button type="submit" style="padding:2px 8px;font-size:10px;background:#2563eb;color:#fff;border:none;border-radius:4px;cursor:pointer;margin-left:2px;">검색</button></form>';
  const exportUrl = baseUrl + '/export' + qs();
  const excelBtn = '<a href="' + exportUrl + '" style="margin-left:auto;padding:4px 8px;font-size:10px;background:#0d9488;color:#fff;border-radius:4px;text-decoration:none;">Excel로 Export</a>';
  const toolbarHtml = '<div style="margin-bottom:8px;font-size:10px;display:flex;flex-wrap:wrap;align-items:center;gap:4px;max-width:100%;">' + orderByLinks + sortDirLinks + '<span style="margin-left:4px;font-size:10px;color:#374151;">기간:</span>' + periodLinks + dateForm + searchForm + excelBtn + '</div>';
  const thLabels = ['TransactionId', '거래일시', 'Merchant', 'Customer', 'OrderNo', 'PaymentChannel', '결제일시', 'Amount', 'Fee', 'TotalAmount', 'Currency', 'RouteNo', 'Status', 'Settled'];
  const colWidths = ['7%', '7%', '6%', '6%', '18%', '6%', '7%', '5%', '4%', '6%', '4%', '4%', '5%', '3%'];
  const colgroup = '<colgroup>' + colWidths.map((w) => '<col style="width:' + w + ';">').join('') + '</colgroup>';
  const thead = '<thead><tr>' + thLabels.map((l) => '<th style="text-align:center;padding:4px 6px;border:1px solid #e5e7eb;background:#f3f4f6;position:relative;font-size:10px;white-space:nowrap;">' + esc(l) + '</th>').join('') + '</tr></thead>';
  const rowToCells = (row) => {
    const cells = [
      row.transactionId,
      row.transactionDate || '-',
      row.merchant || '-',
      row.customer || '-',
      row.orderNo || '-',
      row.paymentChannel || '-',
      row.paymentDate || '-',
      row.amount != null ? row.amount : '-',
      row.fee != null ? row.fee : '-',
      row.totalAmount != null ? row.totalAmount : '-',
      row.currency || '-',
      row.routeNo != null ? row.routeNo : '-',
      row.status || '-',
      row.settled === true ? 'Y' : (row.settled === false ? 'N' : '-'),
    ];
    return '<tr>' + cells.map((c, idx) => {
      var extra = 'font-size:10px;';
      if (idx === 4) extra += 'min-width:200px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;';
      if (idx === 10 || idx === 11 || idx === 12 || idx === 13) extra += 'overflow:hidden;text-overflow:ellipsis;';
      return '<td style="text-align:center;padding:4px 6px;border:1px solid #e5e7eb;' + extra + '">' + esc(String(c)) + '</td>';
    }).join('') + '</tr>';
  };
  const rowMatchesSearch = (row) => {
    if (!searchKw) return true;
    const kw = searchKw.toLowerCase();
    if (searchField === 'all') {
      const str = [row.transactionId, row.orderNo, row.merchant, row.customer, row.amount, row.totalAmount, row.status, row.paymentChannel, row.currency, row.paymentDate, row.transactionDate, row.routeNo].filter(Boolean).join(' ').toLowerCase();
      return str.indexOf(kw) !== -1;
    }
    let val = '';
    if (searchField === 'TransactionId') val = String(row.transactionId || '');
    else if (searchField === 'OrderNo') val = String(row.orderNo || '');
    else if (searchField === 'Customer') val = String(row.customer || '');
    else if (searchField === 'Merchant') val = String(row.merchant || '');
    else if (searchField === 'RouteNo') val = String(row.routeNo != null ? row.routeNo : '');
    else if (searchField === 'Currency') val = String(row.currency || '');
    else if (searchField === 'Status') val = String(row.status || '');
    else if (searchField === 'Amount') val = String(row.amount != null ? row.amount : '') + String(row.totalAmount != null ? row.totalAmount : '');
    return val.toLowerCase().indexOf(kw) !== -1;
  };
  const parseAmount = (v) => { const n = parseFloat(String(v || '0').replace(/,/g, '')); return Number.isFinite(n) ? n : 0; };
  const parsePgDate = (str) => { if (!str) return 0; const m = String(str).trim().match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/); if (!m) return 0; const [, d, mo, y] = m; return new Date(parseInt(y, 10), parseInt(mo, 10) - 1, parseInt(d, 10)).getTime(); };
  const sortRows = (list) => {
    const rev = sortDir === 'asc' ? 1 : -1;
    return list.slice().sort((a, b) => {
      let c = 0;
      if (orderBy === 'date') c = (parsePgDate(a.transactionDate) - parsePgDate(b.transactionDate)) * rev;
      else if (orderBy === 'routeNo') c = (String(a.routeNo != null ? a.routeNo : '').localeCompare(String(b.routeNo != null ? b.routeNo : ''), 'ja')) * rev;
      else if (orderBy === 'currency') c = (String(a.currency || '').localeCompare(String(b.currency || ''), 'ja')) * rev;
      else if (orderBy === 'status') c = String(a.status || '').localeCompare(String(b.status || ''), 'ja') * rev;
      else c = (parsePgDate(a.transactionDate) - parsePgDate(b.transactionDate)) * rev;
      return c;
    });
  };
  let sectionsHtml = '';
  for (const dateKey of periodDateKeys) {
    let list = (byDate[dateKey] || []).filter(rowMatchesSearch);
    list = sortRows(list);
    if (list.length === 0 && searchKw) continue;
    const title = dateKey === todayKey ? '오늘 (' + dateKey + ')' : dateKey;
    const rows = list.map(rowToCells).join('');
    sectionsHtml += '<div style="margin-bottom:16px;"><h2 style="font-size:10px;margin:0 0 6px 0;color:#1f2937;">' + esc(title) + ' (' + list.length + '건)</h2><table class="pg-tx-table" style="width:100%;border-collapse:collapse;font-size:10px;table-layout:fixed;">' + colgroup + thead + '<tbody>' + rows + '</tbody></table></div>';
  }
  if (!sectionsHtml) {
    const hasAnyData = Object.keys(byDate).length > 0;
    const cred = env === 'sandbox' ? cfg.sandbox : cfg.production;
    const hasCreds = !!(cred.mid && cred.apiKey && cred.md5);
    let hint;
    if (searchKw) {
      hint = ' 검색 조건을 바꿔 보세요.';
    } else if (lastFetchedAt && hasAnyData) {
      hint = ' 선택한 기간(당일/전일/이번주 등)에 거래가 없습니다. SORT에서 <strong>당월</strong>, <strong>전월</strong>, <strong>전체</strong>를 선택해 보세요.';
    } else if (lastFetchedAt && !hasAnyData) {
      hint = ' 동기화가 완료되었습니다. 해당 환경·기간에서 ChillPay에 조회된 거래가 없습니다. SORT에서 <strong>당월</strong>, <strong>전월</strong>, <strong>전체</strong>를 선택해 보시거나, ChillPay에 결제 건이 있는지 확인해 주세요.';
    } else if (hasCreds) {
      hint = ' 위 <strong>동기화</strong> 버튼을 눌러 거래 내역을 불러 오세요.';
    } else {
      const envLabel = env === 'sandbox' ? 'SANDBOX' : 'PRODUCTION';
      hint = ' 환경설정 → ChillPay 무효/환불 설정에서 <strong>' + envLabel + '</strong>의 Mid, ApiKey, MD5를 입력한 뒤 위 <strong>동기화</strong> 버튼을 눌러 주세요.';
    }
    sectionsHtml = '<p style="color:#6b7280;font-size:10px;">해당 환경의 거래 내역이 없습니다.' + hint + '</p>';
  }
  const resizeScript = '<script>(function(){try{var tables=document.querySelectorAll(\"table.pg-tx-table\");Array.prototype.forEach.call(tables,function(table){var ths=table.querySelectorAll(\"thead th\");Array.prototype.forEach.call(ths,function(th,index){var handle=document.createElement(\"div\");handle.style.position=\"absolute\";handle.style.top=\"0\";handle.style.right=\"0\";handle.style.width=\"6px\";handle.style.cursor=\"col-resize\";handle.style.userSelect=\"none\";handle.style.height=\"100%\";handle.addEventListener(\"mousedown\",function(e){e.preventDefault();var startX=e.pageX;var startWidth=th.offsetWidth;function onMove(ev){var newW=startWidth+(ev.pageX-startX);if(newW<40)newW=40;th.style.width=newW+\"px\";Array.prototype.forEach.call(table.querySelectorAll(\"tbody tr\"),function(tr){if(tr.children[index]){tr.children[index].style.width=newW+\"px\";}});}function onUp(){document.removeEventListener(\"mousemove\",onMove);document.removeEventListener(\"mouseup\",onUp);}document.addEventListener(\"mousemove\",onMove);document.addEventListener(\"mouseup\",onUp);});th.appendChild(handle);});});}catch(e){}})();</script>';
  const periodLinkScript = '<script>(function(){function pad(n){return (n<10?\"0\":\"\")+n;}function ymd(d){return d.getFullYear()+\"-\"+pad(d.getMonth()+1)+\"-\"+pad(d.getDate());}function getRange(period){var now=new Date(),y=now.getFullYear(),m=now.getMonth(),d=now.getDate();if(period===\"today\"){return{from:ymd(now),to:ymd(now)};}if(period===\"yesterday\"){var yest=new Date(y,m,d-1);return{from:ymd(yest),to:ymd(yest)};}if(period===\"thisWeek\"||period===\"lastWeek\"){var dow=now.getDay(),monOff=dow===0?6:dow-1;var mon=new Date(y,m,d-monOff);if(period===\"lastWeek\"){mon.setDate(mon.getDate()-7);}var sun=new Date(mon);sun.setDate(sun.getDate()+6);return{from:ymd(mon),to:ymd(sun)};}if(period===\"thisMonth\"){return{from:y+\"-\"+pad(m+1)+\"-01\",to:ymd(new Date(y,m+1,0))};}if(period===\"lastMonth\"){return{from:ymd(new Date(y,m-1,1)),to:ymd(new Date(y,m,0))};}return null;}document.querySelectorAll(\"a.pg-period-link\").forEach(function(a){var period=a.getAttribute(\"data-period\");if(period===\"all\")return;var range=getRange(period);if(range){var url=new URL(a.href,window.location.origin);url.searchParams.set(\"dateFrom\",range.from);url.searchParams.set(\"dateTo\",range.to);a.href=url.pathname+\"?\"+url.searchParams.toString();}});})();</script>';
  const mainContent = alertPgHtml + lastFetchedHtml + '<div style="max-width:100%;overflow-x:hidden;">' + toolbarHtml + sectionsHtml + '</div>' + periodLinkScript + resizeScript;
  res.send(renderCancelRefundPage(locale, adminUser, t(locale, 'nav_pg_transaction_list'), mainContent, '', req.originalUrl, req.session.member, req, syncBtnHtml, env));
});

app.get('/admin/pg-transactions/export', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const q = req.query || {};
  const env = getPgEnvFromReq(req);
  const periodSort = (q.sort || 'today').toString();
  const sortDir = (q.sortDir || 'desc').toString().toLowerCase() === 'asc' ? 'asc' : 'desc';
  const orderBy = (q.orderBy || 'date').toString();
  const searchKw = (q.search || '').toString().trim();
  const searchField = (q.searchField || 'all').toString();
  const store = loadPgTransactionStore();
  const block = env === 'sandbox' ? store.sandbox : store.production;
  const byDate = block.byDate || {};
  const now = new Date();
  const y = now.getFullYear();
  const m = now.getMonth();
  const d = now.getDate();
  const toYmd = (date) => date.getFullYear() + '-' + String(date.getMonth() + 1).padStart(2, '0') + '-' + String(date.getDate()).padStart(2, '0');
  let dateFromExport = (q.dateFrom || '').toString().trim();
  let dateToExport = (q.dateTo || '').toString().trim();
  if (!dateFromExport && !dateToExport && periodSort && periodSort !== 'all') {
    const today = new Date(y, m, d);
    let start = null;
    let end = null;
    if (periodSort === 'today') { start = new Date(today); end = new Date(today); }
    else if (periodSort === 'yesterday') { start = new Date(today); start.setDate(start.getDate() - 1); end = new Date(start); }
    else if (periodSort === 'thisWeek') {
      const dayOfWeek = now.getDay();
      const monOffset = dayOfWeek === 0 ? -6 : 1 - dayOfWeek;
      start = new Date(y, m, d + monOffset);
      end = new Date(start); end.setDate(end.getDate() + 6);
    } else if (periodSort === 'lastWeek') {
      const dayOfWeek = now.getDay();
      const monOffset = dayOfWeek === 0 ? -6 : 1 - dayOfWeek;
      start = new Date(y, m, d + monOffset - 7);
      end = new Date(start); end.setDate(end.getDate() + 6);
    } else if (periodSort === 'thisMonth') { start = new Date(y, m, 1); end = new Date(y, m + 1, 0); }
    else if (periodSort === 'lastMonth') { start = new Date(y, m - 1, 1); end = new Date(y, m, 0); }
    if (start && end) { dateFromExport = toYmd(start); dateToExport = toYmd(end); }
  }
  const allKeysExport = Object.keys(byDate).sort((a, b) => b.localeCompare(a));
  let periodDateKeys = allKeysExport;
  if (dateFromExport || dateToExport) {
    periodDateKeys = allKeysExport.filter((k) => (!dateFromExport || k >= dateFromExport) && (!dateToExport || k <= dateToExport));
  }
  const rowMatchesSearch = (row) => {
    if (!searchKw) return true;
    const kw = searchKw.toLowerCase();
    if (searchField === 'all') {
      const str = [row.transactionId, row.orderNo, row.merchant, row.customer, row.amount, row.totalAmount, row.status, row.paymentChannel, row.currency, row.paymentDate, row.transactionDate, row.routeNo].filter(Boolean).join(' ').toLowerCase();
      return str.indexOf(kw) !== -1;
    }
    let val = '';
    if (searchField === 'TransactionId') val = String(row.transactionId || '');
    else if (searchField === 'OrderNo') val = String(row.orderNo || '');
    else if (searchField === 'Customer') val = String(row.customer || '');
    else if (searchField === 'Merchant') val = String(row.merchant || '');
    else if (searchField === 'RouteNo') val = String(row.routeNo != null ? row.routeNo : '');
    else if (searchField === 'Currency') val = String(row.currency || '');
    else if (searchField === 'Status') val = String(row.status || '');
    else if (searchField === 'Amount') val = String(row.amount != null ? row.amount : '') + String(row.totalAmount != null ? row.totalAmount : '');
    return val.toLowerCase().indexOf(kw) !== -1;
  };
  const parsePgDate = (str) => { if (!str) return 0; const m = String(str).trim().match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/); if (!m) return 0; const [, d, mo, y] = m; return new Date(parseInt(y, 10), parseInt(mo, 10) - 1, parseInt(d, 10)).getTime(); };
  const sortRows = (list) => {
    const rev = sortDir === 'asc' ? 1 : -1;
    return list.slice().sort((a, b) => {
      let c = 0;
      if (orderBy === 'date') c = (parsePgDate(a.transactionDate) - parsePgDate(b.transactionDate)) * rev;
      else if (orderBy === 'routeNo') c = (String(a.routeNo != null ? a.routeNo : '').localeCompare(String(b.routeNo != null ? b.routeNo : ''), 'ja')) * rev;
      else if (orderBy === 'currency') c = (String(a.currency || '').localeCompare(String(b.currency || ''), 'ja')) * rev;
      else if (orderBy === 'status') c = String(a.status || '').localeCompare(String(b.status || ''), 'ja') * rev;
      else c = (parsePgDate(a.transactionDate) - parsePgDate(b.transactionDate)) * rev;
      return c;
    });
  };
  let list = [];
  for (const dateKey of periodDateKeys) {
    const rows = (byDate[dateKey] || []).filter(rowMatchesSearch);
    list = list.concat(rows);
  }
  list = sortRows(list);
  const csvEscape = (v) => { const s = String(v ?? ''); if (/[",\n\r]/.test(s)) return '"' + s.replace(/"/g, '""') + '"'; return s; };
  const headerRow = ['TransactionId', '거래일시', 'Merchant', 'Customer', 'OrderNo', 'PaymentChannel', '결제일시', 'Amount', 'Fee', 'TotalAmount', 'Currency', 'RouteNo', 'Status', 'Settled'];
  const rows = list.map((row) => {
    return [
      row.transactionId,
      row.transactionDate || '',
      row.merchant || '',
      row.customer || '',
      row.orderNo || '',
      row.paymentChannel || '',
      row.paymentDate || '',
      row.amount != null ? row.amount : '',
      row.fee != null ? row.fee : '',
      row.totalAmount != null ? row.totalAmount : '',
      row.currency || '',
      row.routeNo != null ? row.routeNo : '',
      row.status || '',
      row.settled === true ? 'Y' : (row.settled === false ? 'N' : ''),
    ].map(csvEscape).join(',');
  });
  const csv = '\uFEFF' + headerRow.map(csvEscape).join(',') + '\n' + rows.join('\n');
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', 'attachment; filename="pg-transactions-' + (new Date().toISOString().slice(0, 10)) + '.csv"');
  res.send(csv);
});

app.get('/admin/transactions/export', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const q = req.query || {};
  const sortBy = (q.sort || 'date').toString();
  const sortDir = (q.sortDir || 'desc').toString().toLowerCase() === 'asc' ? 'asc' : 'desc';
  const searchKw = (q.search || '').toString().trim();
  const searchField = (q.searchField || 'all').toString();
  const dateFrom = (q.dateFrom || '').toString().trim();
  const dateTo = (q.dateTo || '').toString().trim();
  let list = [...getEnvFilteredLogs(req)].slice().reverse();
  if (dateFrom || dateTo) {
    const fromTs = dateFrom ? Date.parse(dateFrom) : 0;
    const toTs = dateTo ? (Date.parse(dateTo) + 86400000) : Infinity;
    list = list.filter((log) => {
      const t = Date.parse(log.receivedAtIso || log.receivedAt);
      return !Number.isNaN(t) && t >= fromTs && t < toTs;
    });
  }
  if (searchKw) {
    const kw = searchKw.toLowerCase();
    list = list.filter((log) => {
      const body = parseNotiBody(log);
      const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
      const routeNo = merchant && (merchant.routeNo != null && String(merchant.routeNo).trim() !== '') ? String(merchant.routeNo) : (String(log.routeKey || '').match(/\d+/) || [])[0] || '';
      if (searchField === 'all') {
        const str = [
          log.receivedAtIso || log.receivedAt,
          body.TransactionId, body.transactionId, body.OrderNo, body.orderNo, body.Amount, body.amount,
          body.PaymentStatus, body.paymentStatus, body.status, body.PaymentDate, body.paymentDate,
          formatCurrencyForDisplay(body.Currency || body.currency), body.Currency, body.currency,
          body.CustomerId, body.customerId, body.PaymentDescription, body.paymentDescription, body.Description, body.description,
          log.merchantId || '', routeNo,
        ].filter(Boolean).join(' ').toLowerCase();
        return str.indexOf(kw) !== -1;
      }
      let val = '';
      if (searchField === 'OrderNo') val = (body.OrderNo || body.orderNo || '') + '';
      else if (searchField === 'CustomerId') val = (body.CustomerId || body.customerId || '') + '';
      else if (searchField === 'TransactionId') val = (body.TransactionId || body.transactionId || '') + '';
      else if (searchField === 'Amount') val = (body.Amount || body.amount || '') + '';
      else if (searchField === 'merchant') val = (log.merchantId || '') + '';
      else if (searchField === 'Route') val = routeNo + '';
      else if (searchField === 'Currency') val = (formatCurrencyForDisplay(body.Currency || body.currency) || body.Currency || body.currency || '') + '';
      else if (searchField === 'Description') val = (body.PaymentDescription || body.paymentDescription || body.Description || body.description || '') + '';
      return val.toLowerCase().indexOf(kw) !== -1;
    });
  }
  const rev = sortDir === 'asc' ? 1 : -1;
  if (sortBy === 'route') {
    list.sort((a, b) => {
      const ma = a.merchantId ? MERCHANTS.get(a.merchantId) : null;
      const mb = b.merchantId ? MERCHANTS.get(b.merchantId) : null;
      const ra = ma && (ma.routeNo != null && String(ma.routeNo).trim() !== '') ? String(ma.routeNo) : (String(a.routeKey || '').match(/\d+/) || [])[0] || '';
      const rb = mb && (mb.routeNo != null && String(mb.routeNo).trim() !== '') ? String(mb.routeNo) : (String(b.routeKey || '').match(/\d+/) || [])[0] || '';
      const c = ra.localeCompare(rb, 'ja');
      if (c !== 0) return c * rev;
      return ((Date.parse(b.receivedAtIso || b.receivedAt) || 0) - (Date.parse(a.receivedAtIso || a.receivedAt) || 0)) * rev;
    });
  } else if (sortBy === 'currency') {
    list.sort((a, b) => {
      const ba = parseNotiBody(a);
      const bb = parseNotiBody(b);
      const ca = String(formatCurrencyForDisplay(ba.Currency || ba.currency) || ba.Currency || ba.currency || '').toLowerCase();
      const cb = String(formatCurrencyForDisplay(bb.Currency || bb.currency) || bb.Currency || bb.currency || '').toLowerCase();
      const c = ca.localeCompare(cb);
      if (c !== 0) return c * rev;
      return ((Date.parse(b.receivedAtIso || b.receivedAt) || 0) - (Date.parse(a.receivedAtIso || a.receivedAt) || 0)) * rev;
    });
  } else if (sortBy === 'status') {
    const statusOrder = (log) => {
      const body = parseNotiBody(log);
      const ps = body.PaymentStatus ?? body.paymentStatus ?? body.status;
      const isCancel = ps === 0 || ps === '0' || String(ps).toLowerCase() === 'cancel';
      const isSuccess = ps === 1 || ps === '1'
        || (body.paymentStatus && String(body.paymentStatus).toLowerCase() === 'success')
        || body.status === 1;
      if (isCancel) return 1;
      if (!isSuccess) return 0;
      const baseDate = body.TransactionDate || body.transactionDate || body.PaymentDate || body.paymentDate || log.receivedAtIso || log.receivedAt;
      const w = getVoidRefundWindow(baseDate);
      if (w === 'void_auto') return 2;
      if (w === 'void_manual') return 3;
      return 4;
    };
    list.sort((a, b) => {
      const c = (statusOrder(a) - statusOrder(b)) * rev;
      if (c !== 0) return c;
      return ((Date.parse(b.receivedAtIso || b.receivedAt) || 0) - (Date.parse(a.receivedAtIso || a.receivedAt) || 0)) * rev;
    });
  } else {
    list.sort((a, b) => ((Date.parse(b.receivedAtIso || b.receivedAt) || 0) - (Date.parse(a.receivedAtIso || a.receivedAt) || 0)) * rev);
  }
  const headerRow = ['번호', '수신일', '수신시', 'Route', '가맹점', 'TransactionId', 'OrderNo', 'Amount', '전산금액', 'status', 'PaymentDate', 'Currency', 'CustomerId', '상태', '노티', '내역/사유'];
  const exportVoidRefundByTxId = buildVoidRefundNotiMap(30);
  const formatExportSentAt = (iso) => {
    if (!iso) return '';
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0') + ' ' + String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0');
  };
  const csvEscape = (v) => {
    const s = String(v ?? '');
    if (/[",\n\r]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
    return s;
  };
  const rows = list.map((log, idx) => {
    const body = parseNotiBody(log);
    const ps = body.PaymentStatus ?? body.paymentStatus ?? body.status;
    const isCancel = isDefinitelyCancelPaymentStatus(ps);
    const isSuccess = isSuccessPaymentBody(body);
    const isError = isErrorPaymentStatus(ps);
    let statusLabel = '실패';
    if (isCancel) statusLabel = '취소';
    else if (isSuccess) statusLabel = '결제';
    else if (isError) statusLabel = '오류';
    const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
    const routeNo = getRouteNoDisplay(merchant, log.routeKey);
    const dt = formatDateAndTimeTHJP(log.receivedAtIso || log.receivedAt);
    const txId = body.TransactionId ?? body.transactionId ?? '';
    let notiLabel = '';
    if (txId && hasVoidNotiSent(txId)) notiLabel = '무효';
    else if (txId && hasRefundNotiSent(txId)) notiLabel = '환불';
    const isVoidedOrRefunded = isSuccess && txId && (hasVoidNotiSent(txId) || hasRefundNotiSent(txId));
    const amountVal = body.Amount ?? body.amount ?? '';
    const amountStr = isVoidedOrRefunded ? '-' : formatAmountWithSeparator(amountVal !== '' && amountVal != null ? amountVal : null);
    const internalAmt = (amountVal !== '' && amountVal != null && !isVoidedOrRefunded) ? (Number(amountVal) / 100) : null;
    const internalAmtStr = formatAmountWithSeparator(internalAmt);
    let detailStr = '';
    if (isVoidedOrRefunded && txId) {
      const entry = exportVoidRefundByTxId[txId];
      const isVoid = hasVoidNotiSent(txId);
      const kindLabel = isVoid ? '무효' : '환불';
      const defaultReason = isVoid ? '관리자 무효 요청' : '관리자 환불 요청';
      const reason = (entry && entry.reason) ? entry.reason : defaultReason;
      if (entry && entry.sentAtIso) {
        const sentStr = formatExportSentAt(entry.sentAtIso);
        const relayStr = entry.relayStatus ? ' 가맹점:' + entry.relayStatus : '';
        const internalStr = entry.internalStatus ? ' 전산:' + entry.internalStatus : '';
        detailStr = '내역: ' + kindLabel + ' 처리 ' + sentStr + relayStr + internalStr + ' / 사유: ' + reason;
      } else {
        detailStr = '내역: ' + kindLabel + ' 처리됨 / 사유: ' + reason;
      }
    }
    return [
      idx + 1,
      dt.date,
      'TH:' + dt.timeTh + ' JP:' + dt.timeJp,
      routeNo,
      log.merchantId || '',
      txId,
      body.OrderNo ?? body.orderNo ?? '',
      amountStr,
      internalAmtStr,
      ps ?? '',
      body.PaymentDate ?? body.paymentDate ?? '',
      formatCurrencyForDisplay(body.Currency || body.currency) || body.Currency || body.currency || '',
      body.CustomerId ?? body.customerId ?? '',
      statusLabel,
      notiLabel,
      detailStr,
    ].map(csvEscape).join(',');
  });
  const csv = '\uFEFF' + headerRow.map(csvEscape).join(',') + '\n' + rows.join('\n');
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', 'attachment; filename="transactions-' + (new Date().toISOString().slice(0, 10)) + '.csv"');
  res.send(csv);
});

const MAX_SYNC_HISTORY = 50;

// 노티 로그/거래 내역용 기본 환경 선택 (기본: live, 쿼리로 sandbox 전환)
function getEnvFromReq(req) {
  return (req.query && req.query.env || 'live').toString().toLowerCase() === 'sandbox' ? 'sandbox' : 'live';
}

// 피지거래내역(ChillPay Search Payment) 전용 환경 선택
function getPgEnvFromReq(req) {
  const q = (req && req.query && req.query.env ? String(req.query.env) : '').toLowerCase();
  if (q === 'sandbox') return 'sandbox';
  if (q === 'live') return 'live';

  // 쿼리가 없으면 환경설정(useSandbox) → APP_ENV 순으로 기본값 결정
  try {
    const cfg = loadChillPayTransactionConfig();
    if (cfg && cfg.useSandbox) return 'sandbox';
  } catch {
    // 설정 로드 실패 시 무시
  }
  return APP_ENV === 'test' ? 'sandbox' : 'live';
}
function isLogSandbox(log) {
  return String(log.env || '').toLowerCase().trim() === 'sandbox';
}
// 로그분석(PG 노티 로그)과 동일한 live 목록: env가 정확히 'sandbox'가 아닌 건 모두 표시 (성공 건이 노티거래내역에도 나오도록)
function isLiveLog(log) {
  return (log.env || '') !== 'sandbox';
}
function getEnvFilteredLogs(req) {
  const showSandbox = getEnvFromReq(req) === 'sandbox';
  const logs = Array.isArray(NOTI_LOGS) ? NOTI_LOGS : [];
  return logs.filter((log) => showSandbox ? isLogSandbox(log) : isLiveLog(log));
}
// PaymentStatus 구분값 (두 메뉴얼 모두 반영)
// ----- 취소 vs 무효 vs 환불 (ChillPay 기준) -----
// 취소(Cancel): 결제 시 문제·고객 취소 등으로 PG가 노티로 보내준 것만 수신. 우리가 ChillPay에서 "결제 취소"를 요청/처리하는 API·기능은 없음. 100% 노티 반영만.
// 무효(Void): 우리가 Request Void API로 ChillPay에 무효 요청 가능. 무효/환불 가능 시간 설정 대상.
// 환불(Refund): 우리가 Request Refund API로 ChillPay에 환불 요청 가능.
// [1] ChillPay Transaction Services API (docs/ChillPay_Transaction_Services_API_분석_및_취소무효_자동화.md Appendix A)
//     1=Success, 2=Fail, 3=Cancel, 4=Error, 5=Request, 6=Void Requested, 7=Voided, 8=Refund Requested, 9=Refunded
// [2] ChillCredit Merchant Integration Manual (Ontheline_inline_ChillCredit-Merchant-Integration-Manual-Document-EN) Appendix C - 결제 결과 콜백
//     0=Success(Complete payment), 1=Fail, 2=Cancel(고객취소), 3=Error, 9=Pending, 20=Void Success, 21=Refund Success, 22=Request Refund, 23=Settlement, 24=Void Fail, 25=Refund Fail
// → 노티(콜백)에서는 0=성공, 2=취소 이므로, 0을 취소로 넣지 않고 2/3/Cancel 만 취소로 처리.
function isSuccessPaymentBody(body) {
  if (!body || typeof body !== 'object') return false;
  const ps = body.PaymentStatus ?? body.paymentStatus ?? body.status;
  return ps === 0 || ps === '0' || ps === 1 || ps === '1'
    || (typeof ps === 'string' && ps.toLowerCase() === 'success');
}
// 취소 노티로 인정: PDF Appendix C 기준 2=Cancel, Transaction API 기준 3=Cancel. 0은 PDF에서 Success이므로 제외.
function isDefinitelyCancelPaymentStatus(ps) {
  if (ps === 0 || ps === '0' || ps === 1 || ps === '1') return false;
  return ps === 2 || ps === '2' || ps === 3 || ps === '3'
    || ps === 'Cancel' || ps === 'Canceled' || ps === 'Cancelled'
    || (typeof ps === 'string' && ps.toLowerCase() === 'cancel');
}
// 오류 상태로 간주할 PaymentStatus (API 4=Error, 콜백 3=Error, 문자열 'Error')
function isErrorPaymentStatus(ps) {
  if (ps === 4 || ps === '4') return true;
  if (ps === 3 || ps === '3') return true;
  return typeof ps === 'string' && ps.toLowerCase() === 'error';
}
// 재전송 구분용: 성공(0,1)이 아니면 취소·무효·환불·실패로 간주
function isCancelNotiBody(body) {
  if (!body || typeof body !== 'object') return false;
  const ps = body.PaymentStatus ?? body.paymentStatus ?? body.status;
  if (ps === 0 || ps === '0' || ps === 1 || ps === '1') return false;
  return ps === 2 || ps === '2' || ps === 3 || ps === '3'
    || ps === 6 || ps === '6' || ps === 7 || ps === '7' || ps === 8 || ps === '8' || ps === 9 || ps === '9'
    || ps === 20 || ps === '20' || ps === 21 || ps === '21' || ps === 22 || ps === '22' || ps === 24 || ps === '24' || ps === 25 || ps === '25'
    || ps === 'Cancel' || ps === 'Canceled' || ps === 'Cancelled' || String(ps).toLowerCase() === 'cancel';
}
function buildSyncResultTableHtml(entry, titleLabel, escFn) {
  if (!entry || !Array.isArray(entry.items)) return '';
  const resultLabel = (r) => {
    if (r === 'sent') return '노티 전송함';
    if (r === 'alreadySent') return '이미 전송됨';
    if (r === 'noMatch') return '로그 미매칭';
    if (r === 'notVoided') return '무효 아님(상태)';
    if (r === 'notRefunded') return '환불 아님(상태)';
    return r;
  };
  const syncedAtStr = entry.syncedAt ? (function () { const d = new Date(entry.syncedAt); return isNaN(d.getTime()) ? entry.syncedAt : d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0') + ' ' + String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0'); })() : '';
  const syncRows = entry.items.map((it) => '<tr><td>' + escFn(it.transactionId) + '</td><td>' + escFn(it.orderNo) + '</td><td>' + escFn(resultLabel(it.result)) + '</td></tr>').join('');
  return '<div style="font-weight:600;margin-bottom:8px;">' + titleLabel + ' · 조회 시각 ' + escFn(syncedAtStr) + ' · 총 ' + (entry.total || 0) + '건 중 전송 ' + (entry.sent || 0) + '건</div><table style="width:100%;border-collapse:collapse;font-size:12px;"><thead><tr><th style="text-align:center;vertical-align:middle;padding:6px 8px;border:1px solid #e5e7eb;background:#f3f4f6;">TransactionId</th><th style="text-align:center;vertical-align:middle;padding:6px 8px;border:1px solid #e5e7eb;background:#f3f4f6;">OrderNo</th><th style="text-align:center;vertical-align:middle;padding:6px 8px;border:1px solid #e5e7eb;background:#f3f4f6;">처리 결과</th></tr></thead><tbody>' + syncRows + '</tbody></table>';
}

app.get('/admin/cancel-refund/cancel', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const q = req.query || {};
  const escQ = (s) => (s ? String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '');
  let alertHtml = (q.resend === 'ok' ? '<div class="alert alert-ok">취소 건 전산 재전송이 완료되었습니다. 전산 결과에서 성공 여부를 확인하세요.</div>' : '') + (q.resend === 'fail' && q.reason ? '<div class="alert alert-fail">취소 건 전산 재전송 실패: ' + escQ(q.reason) + '</div>' : '');
  alertHtml += (q.resendPg === 'ok' ? '<div class="alert alert-ok">취소 건 피지(가맹점) 재전송이 완료되었습니다. 가맹점 수신 여부는 피지 결과에서 확인하세요.</div>' : '') + (q.resendPg === 'fail' && q.reasonPg ? '<div class="alert alert-fail">취소 건 피지 재전송 실패: ' + escQ(q.reasonPg) + '</div>' : '');
  const { voidMap: voidSentMapForForce } = buildVoidRefundNotiSentMaps(90);
  const filteredLogs = getEnvFilteredLogs(req);
  const reversed = [...filteredLogs].slice().reverse();
  // 결제 완료(성공) 노티는 취소 내역에 넣지 않음. ChillPay에서 취소로 온 노티만 표시(취소는 노티 수신만, 우리가 API로 취소 처리 불가).
  const cancelled = reversed.filter((log) => {
    const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
    if (isSuccessPaymentBody(body)) return false;
    const ps = body.PaymentStatus ?? body.paymentStatus ?? body.status;
    return isDefinitelyCancelPaymentStatus(ps);
  });
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const rows = cancelled.map((log) => {
    const realIndex = NOTI_LOGS.indexOf(log);
    const dt = formatDateAndTimeTHJP(log.receivedAtIso || log.receivedAt);
    const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
    const ps = body.PaymentStatus ?? body.paymentStatus ?? body.status;
    const psLabel = ps === 2 || ps === '2' ? '2(Cancel)' : ps === 3 || ps === '3' ? '3(Cancel)' : (typeof ps === 'string' && ps.toLowerCase() === 'cancel') ? 'Cancel' : esc(String(ps));
    const txId = body.TransactionId ?? body.transactionId ?? '-';
    const orderNo = body.OrderNo ?? body.orderNo ?? '-';
    const amount = body.Amount ?? body.amount ?? '-';
    const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
    const routeNoDisplay = getRouteNoDisplay(merchant, log.routeKey);
    const internalUrl = merchant && (merchant.internalTargetId ? findInternalTargetUrl(merchant.internalTargetId, 'callback') : null) || INTERNAL_NOTI_URL;
    const hasPgUrl = merchant && (merchant.callbackUrl || merchant.resultUrl);
    const internalBtn = internalUrl ? '<form method="post" action="/admin/cancel-refund/cancel-resend-internal" style="display:inline;"><input type="hidden" name="index" value="' + realIndex + '" /><button type="submit" style="padding:4px 10px;font-size:12px;background:#2563eb;color:#fff;border:none;border-radius:4px;cursor:pointer;">전산 재전송</button></form>' : '<span style="color:#9ca3af;font-size:11px;">전산 URL 미설정</span>';
    const pgBtn = hasPgUrl ? '<form method="post" action="/admin/cancel-refund/cancel-resend-pg" style="display:inline;margin-left:6px;"><input type="hidden" name="index" value="' + realIndex + '" /><button type="submit" class="btn-resend-pg" style="padding:4px 10px;font-size:12px;background:#059669;color:#fff;border:none;border-radius:4px;cursor:pointer;">피지 재전송</button></form>' : '<span style="color:#9ca3af;font-size:11px;">가맹점 URL 미설정</span>';
    const resendBtn = '<span class="cancel-resend-cell">' + internalBtn + pgBtn + '</span>';
    return `<tr>
      <td class="col-date">${esc(dt.date)}</td>
      <td class="col-time">TH: ${esc(dt.timeTh)}<br><span class="time-jp">JP: ${esc(dt.timeJp)}</span></td>
      <td class="col-narrow">${esc(routeNoDisplay)}</td>
      <td class="col-narrow">${esc(log.merchantId || '')}</td>
      <td>${esc(txId)} / ${esc(orderNo)} / ${esc(amount)}</td>
      <td class="col-narrow" style="font-size:11px;">${psLabel}</td>
      <td class="col-action">${resendBtn}</td>
    </tr>`;
  }).join('');
  const helpPg = '<p class="hint" style="margin-bottom:12px;color:#6b7280;font-size:13px;"><strong>취소</strong>는 ChillPay 노티로만 수신되는 건입니다. 우리가 칠페이에서 결제 취소를 요청·처리하는 기능은 없습니다. 아래는 PG에서 <strong>취소</strong>로 들어온 노티만 표시하며, 재전송 시 PaymentStatus=2(취소)로 전달됩니다. ChillCredit 매뉴얼: 0=성공, 2=Cancel. <a href="/admin/cancel-refund/noti">노티 결과</a>·<a href="/admin/internal-result">전산 결과</a>에서 TransactionId로 확인하세요.</p>';
  const thead = '<thead><tr><th>수신일</th><th>수신시각</th><th>Route No.</th><th>가맹점</th><th>TransactionId / OrderNo / Amount</th><th>PaymentStatus</th><th>전산 재전송 / 피지 재전송</th></tr></thead>';
  res.send(renderCancelRefundPage(locale, adminUser, t(locale, 'nav_cancel_refund_cancel') + ' (' + cancelled.length + ')', alertHtml + helpPg + '<table>' + thead + '<tbody>' + rows + '</tbody></table>', '', req.originalUrl, req.session.member, req, undefined, getEnvFromReq(req)));
});

// 취소 노티 재전송: 전산 = 가공 로직(개발환경설정) 적용, 피지 = 원문 그대로 전송. 이 구조 유지.
app.post('/admin/cancel-refund/cancel-resend-internal', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const locale = getLocale(req);
  const index = parseInt(req.body.index, 10);
  if (Number.isNaN(index) || index < 0 || index >= NOTI_LOGS.length) {
    return res.redirect('/admin/cancel-refund/cancel?resend=fail&reason=' + encodeURIComponent('잘못된 요청'));
  }
  const log = NOTI_LOGS[index];
  const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
  if (!merchant) {
    return res.redirect('/admin/cancel-refund/cancel?resend=fail&reason=' + encodeURIComponent('가맹점 없음'));
  }
  let internalUrl = merchant.internalTargetId ? findInternalTargetUrl(merchant.internalTargetId, 'callback') : null;
  if (!internalUrl && INTERNAL_NOTI_URL) internalUrl = INTERNAL_NOTI_URL;
  if (!internalUrl) {
    return res.redirect('/admin/cancel-refund/cancel?resend=fail&reason=' + encodeURIComponent('전산 URL 미설정'));
  }
  const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body || '{}'); } catch { return {}; } })() : {});
  if (isSuccessPaymentBody(body)) {
    return res.redirect('/admin/cancel-refund/cancel?resend=fail&reason=' + encodeURIComponent('이 건은 결제 완료 노티(PaymentStatus=1)입니다. 취소 노티가 아니므로 전산 재전송할 수 없습니다.'));
  }
  // 전산 재전송: 가공 로직(개발환경설정) 적용 — transformForInternal 사용
  const bodyForCancel = { ...body, PaymentStatus: 2 };
  const internalPayload = transformForInternal(bodyForCancel, merchant);
  try {
    const result = await sendToInternal(internalUrl, internalPayload);
    appendInternalLog({
      storedAt: new Date().toISOString(),
      merchantId: log.merchantId,
      routeNo: merchant.routeNo || '',
      internalTargetId: merchant.internalTargetId || '',
      payload: internalPayload,
      internalTargetUrl: internalUrl,
      internalDeliveryStatus: result.success ? 'ok' : 'fail',
    });
    return res.redirect('/admin/cancel-refund/cancel?resend=' + (result.success ? 'ok' : 'fail') + (result.success ? '' : '&reason=' + encodeURIComponent('HTTP ' + (result.status || '') + ' 또는 전산 응답 실패')));
  } catch (e) {
    return res.redirect('/admin/cancel-refund/cancel?resend=fail&reason=' + encodeURIComponent(e && e.message ? e.message : '전송 예외'));
  }
});

app.post('/admin/cancel-refund/cancel-resend-pg', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const index = parseInt(req.body.index, 10);
  if (Number.isNaN(index) || index < 0 || index >= NOTI_LOGS.length) {
    return res.redirect('/admin/cancel-refund/cancel?resendPg=fail&reasonPg=' + encodeURIComponent('잘못된 요청'));
  }
  const log = NOTI_LOGS[index];
  const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
  if (!merchant) {
    return res.redirect('/admin/cancel-refund/cancel?resendPg=fail&reasonPg=' + encodeURIComponent('가맹점 없음'));
  }
  const targetUrl = (log.targetUrl && String(log.targetUrl).trim()) || merchant.callbackUrl || merchant.resultUrl || '';
  if (!targetUrl) {
    return res.redirect('/admin/cancel-refund/cancel?resendPg=fail&reasonPg=' + encodeURIComponent('가맹점 URL 없음'));
  }
  const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body || '{}'); } catch { return {}; } })() : {});
  if (isSuccessPaymentBody(body)) {
    return res.redirect('/admin/cancel-refund/cancel?resendPg=fail&reasonPg=' + encodeURIComponent('이 건은 결제 완료 노티(PaymentStatus=1)입니다. 취소 노티가 아니므로 피지 재전송할 수 없습니다.'));
  }
  const hasBody = log.body !== undefined && log.body !== null && (typeof log.body === 'object' || typeof log.body === 'string');
  if (!hasBody) {
    return res.redirect('/admin/cancel-refund/cancel?resendPg=fail&reasonPg=' + encodeURIComponent('노티 본문 없음'));
  }
  let bodyToSend = log.body;
  if (typeof bodyToSend === 'string') {
    try { bodyToSend = JSON.parse(bodyToSend); } catch { return res.redirect('/admin/cancel-refund/cancel?resendPg=fail&reasonPg=' + encodeURIComponent('노티 본문 파싱 실패')); }
  }
  if (!bodyToSend || typeof bodyToSend !== 'object' || Array.isArray(bodyToSend)) {
    return res.redirect('/admin/cancel-refund/cancel?resendPg=fail&reasonPg=' + encodeURIComponent('노티 본문 형식 오류'));
  }
  if (isSuccessPaymentBody(bodyToSend)) {
    return res.redirect('/admin/cancel-refund/cancel?resendPg=fail&reasonPg=' + encodeURIComponent('결제 완료 노티는 취소 재전송할 수 없습니다.'));
  }
  // 피지 재전송: 원문 그대로 전송(가공/transform 미적용). 취소 고정만 PaymentStatus=2 로 보정
  bodyToSend = { ...bodyToSend, PaymentStatus: 2 };
  try {
    const relayRes = await relayToMerchant(targetUrl, bodyToSend, { contentType: 'application/json' });
    const ok = relayRes.status >= 200 && relayRes.status < 300;
    if (ok) {
      if (NOTI_LOGS[index]) {
        NOTI_LOGS[index].relayStatus = 'ok';
        NOTI_LOGS[index].relayFailReason = '';
      }
      return res.redirect('/admin/cancel-refund/cancel?resendPg=ok');
    }
    const bodyPart = relayRes.data != null ? (typeof relayRes.data === 'string' ? String(relayRes.data) : JSON.stringify(relayRes.data)) : '';
    const reason = `HTTP ${relayRes.status}` + (bodyPart ? ': ' + bodyPart.slice(0, 300) : '');
    return res.redirect('/admin/cancel-refund/cancel?resendPg=fail&reasonPg=' + encodeURIComponent(reason));
  } catch (err) {
    const reason = err.code || err.message || String(err);
    return res.redirect('/admin/cancel-refund/cancel?resendPg=fail&reasonPg=' + encodeURIComponent(reason));
  }
});

app.get('/admin/cancel-refund/noti', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const q = req.query || {};
  const escQ = (s) => (s ? String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '');
  let alertNotiHtml = (q.resend === 'ok' ? '<div class="alert alert-ok">취소(무효/환불) 재전송이 완료되었습니다. 전산 결과에서 확인하세요.</div>' : '') + (q.resend === 'fail' && q.reason ? '<div class="alert alert-fail">취소(무효/환불) 재전송 실패: ' + escQ(q.reason) + '</div>' : '');
  const typeFilter = (q.type || 'all').toString();
  const days = parseInt(q.days, 10) || 30;
  const envNoti = getEnvFromReq(req);
  const showSandbox = envNoti === 'sandbox';
  let entries = loadVoidRefundNotiLog(days);
  entries = entries.filter((e) => showSandbox ? isLogSandbox(e) : !isLogSandbox(e));
  const filtered = typeFilter === 'void' ? entries.filter((e) => e.type === 'void') : typeFilter === 'refund' ? entries.filter((e) => e.type === 'refund') : entries;
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const relayLabel = (s, skipReason) => {
    if (s === 'ok') return '<span class="status-ok">' + t(locale, 'cr_status_ok') + '</span>';
    if (s === 'fail') return '<span class="status-fail">' + t(locale, 'cr_status_fail') + '</span>';
    if (s === 'skip') return '<span class="status-skip">미전송' + (skipReason === 'no_merchant' ? ' (가맹점 미등록)' : skipReason === 'no_body' ? ' (본문 없음)' : '') + '</span>';
    return '-';
  };
  const internalLabel = (s, internalSkipReason) => {
    if (s === 'ok') return '<span class="status-ok">' + t(locale, 'cr_status_ok') + '</span>';
    if (s === 'fail') return '<span class="status-fail">' + t(locale, 'cr_status_fail') + '</span>';
    if (s === 'skip') return '<span class="status-skip">미전송' + (internalSkipReason === 'no_internal_url' ? ' (전산 URL 미설정)' : '') + '</span>';
    return '-';
  };
  const rows = filtered.map((e) => {
    const dt = e.sentAtIso ? new Date(e.sentAtIso).toLocaleString('ko-KR', { hour12: false }) : '-';
    const typeLabel = e.type === 'void' ? t(locale, 'cr_type_void') : e.type === 'refund' ? t(locale, 'cr_type_refund') : e.type || '-';
    const notiMerchant = e.merchantId ? MERCHANTS.get(e.merchantId) : null;
    const internalTargetName = getInternalTargetName(notiMerchant && notiMerchant.internalTargetId);
    const resendForm = '<form method="post" action="/admin/cancel-refund/noti-resend-internal" style="display:inline;"><input type="hidden" name="transactionId" value="' + esc(e.transactionId || '') + '" /><input type="hidden" name="notiType" value="' + esc(e.type || 'void') + '" /><input type="hidden" name="merchantId" value="' + esc(e.merchantId || '') + '" /><input type="hidden" name="env" value="' + esc(envNoti) + '" /><input type="hidden" name="type" value="' + esc(typeFilter) + '" /><input type="hidden" name="days" value="' + esc(String(days)) + '" /><button type="submit" style="padding:4px 10px;font-size:12px;background:#2563eb;color:#fff;border:none;border-radius:4px;cursor:pointer;">취소(무효/환불) 재전송</button></form>';
    return `<tr>
      <td class="col-date">${esc(dt)}</td>
      <td class="col-narrow">${esc(typeLabel)}</td>
      <td class="col-narrow">${esc(e.transactionId || '')}</td>
      <td class="col-narrow">${esc(e.orderNo || '')}</td>
      <td class="col-narrow">${esc(e.merchantId || '')}</td>
      <td class="col-narrow">${esc(e.routeNo || '')}</td>
      <td class="col-narrow">${esc(internalTargetName)}</td>
      <td class="col-status">${relayLabel(e.relayStatus, e.skipReason)}</td>
      <td class="col-status">${internalLabel(e.internalStatus, e.internalSkipReason)}</td>
      <td class="col-action">${resendForm}</td>
    </tr>`;
  }).join('');
  const envParam = '&env=' + encodeURIComponent(envNoti);
  const filterLinks = `<div style="margin-bottom:12px;"><span style="font-size:13px;color:#374151;">${t(locale, 'cr_filter_type')}: </span>
    <a href="/admin/cancel-refund/noti?type=all&days=${days}${envParam}" style="padding:6px 12px;font-size:13px;border-radius:6px;text-decoration:none;background:${typeFilter === 'all' ? '#2563eb' : '#e5e7eb'};color:${typeFilter === 'all' ? '#fff' : '#374151'};">${t(locale, 'cr_filter_all')}</a>
    <a href="/admin/cancel-refund/noti?type=void&days=${days}${envParam}" style="padding:6px 12px;font-size:13px;border-radius:6px;text-decoration:none;background:${typeFilter === 'void' ? '#2563eb' : '#e5e7eb'};color:${typeFilter === 'void' ? '#fff' : '#374151'};">${t(locale, 'cr_type_void')}</a>
    <a href="/admin/cancel-refund/noti?type=refund&days=${days}${envParam}" style="padding:6px 12px;font-size:13px;border-radius:6px;text-decoration:none;background:${typeFilter === 'refund' ? '#2563eb' : '#e5e7eb'};color:${typeFilter === 'refund' ? '#fff' : '#374151'};">${t(locale, 'cr_type_refund')}</a>
    <span style="margin-left:12px;font-size:13px;">${t(locale, 'cr_period_recent')} ${days}${t(locale, 'cr_days')}</span>
    <a href="/admin/cancel-refund/noti?type=${typeFilter}&days=7${envParam}" style="margin-left:6px;font-size:12px;">7${t(locale, 'cr_days')}</a>
    <a href="/admin/cancel-refund/noti?type=${typeFilter}&days=30${envParam}" style="margin-left:4px;font-size:12px;">30${t(locale, 'cr_days')}</a>
    <a href="/admin/cancel-refund/noti?type=${typeFilter}&days=90${envParam}" style="margin-left:4px;font-size:12px;">90${t(locale, 'cr_days')}</a>
  </div>`;
  const thead = '<thead><tr><th>' + t(locale, 'cr_th_sent_at') + '</th><th>' + t(locale, 'cr_th_type') + '</th><th>TransactionId</th><th>OrderNo</th><th>' + t(locale, 'cr_th_merchant') + '</th><th>' + t(locale, 'cr_th_route_no') + '</th><th>전산 대상</th><th>' + t(locale, 'cr_th_merchant_receive') + '</th><th>' + t(locale, 'cr_th_internal_receive') + '</th><th>강제 재노티</th></tr></thead>';
  const notiHelp = '<p class="hint" style="margin-bottom:12px;color:#6b7280;font-size:13px;">아래는 무효/환불 노티 <strong>전송 시도</strong> 이력입니다. 전송한 건은 가맹점·전산에 「성공/실패」로 표시되고, 가맹점 미등록 등으로 전송하지 않은 건은 「미전송」으로 표시됩니다. 따라서 <strong>노티가 아예 안 나간 경우</strong>는 여기에서 해당 건이 「미전송」인지, 「실패」인지로 구분할 수 있습니다.</p>'
    + '<p class="hint" style="margin-bottom:12px;color:#6b7280;font-size:13px;"><strong>전산 수신</strong> 열: 전산 시스템으로 노티가 <strong>생성·전달된 결과</strong>입니다. 성공=전산에 정상 전달됨, 실패=전송했으나 전산 쪽 오류/거절, 미전송=전산 URL 미설정 등으로 전송하지 않음. <strong>강제 재노티</strong>: 해당 무효/환불 노티를 전산으로 다시 보냅니다. 전산 결과에서 확인하세요.</p>';
  const tableContent = alertNotiHtml + notiHelp + filterLinks + '<table>' + thead + '<tbody>' + rows + '</tbody></table>';
  res.send(renderCancelRefundPage(locale, adminUser, t(locale, 'nav_cancel_refund_noti') + ' (' + filtered.length + ')', tableContent, '', req.originalUrl, req.session.member, req, undefined, envNoti));
});

app.post('/admin/cancel-refund/noti-resend-internal', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const locale = getLocale(req);
  const transactionId = (req.body.transactionId || '').toString().trim();
  const notiType = (req.body.notiType || 'void').toString().toLowerCase();
  const merchantId = (req.body.merchantId || '').toString().trim();
  const env = (req.body.env || 'live').toString();
  const typeFilter = (req.body.type || 'all').toString();
  const days = (req.body.days || '30').toString();
  const baseRedirect = '/admin/cancel-refund/noti?type=' + encodeURIComponent(typeFilter) + '&days=' + encodeURIComponent(days) + '&env=' + encodeURIComponent(env);
  if (!transactionId || !merchantId) {
    return res.redirect(baseRedirect + '&resend=fail&reason=' + encodeURIComponent('TransactionId 또는 가맹점 없음'));
  }
  const merchant = MERCHANTS.get(merchantId);
  if (!merchant) {
    return res.redirect(baseRedirect + '&resend=fail&reason=' + encodeURIComponent('가맹점 없음'));
  }
  let internalUrl = merchant.internalTargetId ? findInternalTargetUrl(merchant.internalTargetId, 'callback') : null;
  if (!internalUrl && INTERNAL_NOTI_URL) internalUrl = INTERNAL_NOTI_URL;
  if (!internalUrl) {
    return res.redirect(baseRedirect + '&resend=fail&reason=' + encodeURIComponent('전산 URL 미설정'));
  }
  const paymentStatus = notiType === 'refund' ? '9' : '2';
  const log = NOTI_LOGS.find((l) => {
    const body = l.body && typeof l.body === 'object' ? l.body : (typeof l.body === 'string' ? (() => { try { return JSON.parse(l.body || '{}'); } catch { return {}; } })() : {});
    const tid = body.TransactionId != null ? String(body.TransactionId) : (body.transactionId != null ? String(body.transactionId) : '');
    const isSuccess = body.PaymentStatus === 0 || body.PaymentStatus === '0' || body.PaymentStatus === 1 || body.PaymentStatus === '1'
      || (body.paymentStatus && String(body.paymentStatus).toLowerCase() === 'success')
      || body.status === 1;
    return tid === transactionId && l.merchantId === merchantId && isSuccess;
  });
  if (!log || !log.body) {
    return res.redirect(baseRedirect + '&resend=fail&reason=' + encodeURIComponent('해당 결제 건을 찾을 수 없음 (NOTI_LOGS)'));
  }
  const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body || '{}'); } catch { return {}; } })() : {});
  const payload = typeof body === 'object' ? { ...body, PaymentStatus: paymentStatus } : { ...JSON.parse(log.body || '{}'), PaymentStatus: paymentStatus };
  const internalPayload = transformForInternal(payload, merchant);
  try {
    const result = await sendToInternal(internalUrl, internalPayload);
    appendInternalLog({
      storedAt: new Date().toISOString(),
      merchantId: log.merchantId,
      routeNo: merchant.routeNo || '',
      internalTargetId: merchant.internalTargetId || '',
      payload: internalPayload,
      internalTargetUrl: internalUrl,
      internalDeliveryStatus: result.success ? 'ok' : 'fail',
    });
    return res.redirect(baseRedirect + '&resend=' + (result.success ? 'ok' : 'fail') + (result.success ? '' : '&reason=' + encodeURIComponent('HTTP ' + (result.status || '') + ' 또는 전산 응답 실패')));
  } catch (e) {
    return res.redirect(baseRedirect + '&resend=fail&reason=' + encodeURIComponent(e && e.message ? e.message : '전송 예외'));
  }
});

app.get('/admin/cancel-refund/void', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const q = req.query || {};
  const escQ = (s) => (s ? String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '');
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  let alertHtml = q.void === 'ok' ? '<div class="alert alert-ok">' + t(locale, 'cr_alert_void_ok') + '</div>' : q.void === 'fail' && q.reason ? '<div class="alert alert-fail">' + t(locale, 'cr_alert_void_fail') + ': ' + escQ(q.reason) + '</div>' : '';
  if (q.deleted === '1') alertHtml += '<div class="alert alert-ok">목록에서 제거되었습니다. <a href="/admin/cancel-refund/void-deleted-list?env=' + encodeURIComponent(getEnvFromReq(req)) + '">삭제거래</a>에서 확인·복원할 수 있습니다.</div>';
  if (q.sync === 'ok') {
    const sent = parseInt(q.sent, 10) || 0;
    const total = parseInt(q.total, 10) || 0;
    const alreadySent = parseInt(q.alreadySent, 10) || 0;
    const noMatch = parseInt(q.noMatch, 10) || 0;
    let syncMsg = t(locale, 'cr_alert_sync_void_ok').replace(/\{\{total\}\}/g, total).replace(/\{\{sent\}\}/g, sent);
    if (sent === 0 && (alreadySent > 0 || noMatch > 0)) {
      const parts = [];
      if (alreadySent > 0) parts.push('이미 전송 ' + alreadySent + '건');
      if (noMatch > 0) parts.push('로그 미매칭 ' + noMatch + '건');
      syncMsg += ' (원인: ' + parts.join(', ') + '. ChillPay 조회 건의 TransactionId가 이 서버의 거래내역(NOTI_LOGS)에 없거나, 가맹점 미등록 시 미전송됩니다.)';
    }
    alertHtml += '<div class="alert alert-ok">' + syncMsg + '</div>';
  } else if (q.sync === 'fail' && q.reason) {
    alertHtml += '<div class="alert alert-fail">' + t(locale, 'cr_alert_sync_fail') + ': ' + escQ(q.reason) + '</div>';
  }
  const env = getEnvFromReq(req);
  const escV = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const cfgVoid = loadChillPayTransactionConfig();
  const syncResultValidMs = (Number(cfgVoid.syncResultDisplayMinutes) > 0 ? cfgVoid.syncResultDisplayMinutes : 30) * 60 * 1000;
  let syncResultHtml = '';
  const lastVoid = req.session.lastSyncVoid;
  const voidWithinWindow = lastVoid && lastVoid.syncedAt && (Date.now() - new Date(lastVoid.syncedAt).getTime() < syncResultValidMs);
  if (voidWithinWindow && lastVoid && Array.isArray(lastVoid.items) && lastVoid.items.length > 0) {
    const blockHtml = buildSyncResultTableHtml(lastVoid, '최근 무효 동기화 조회 결과 (ChillPay에서 가져온 목록)', escV);
    syncResultHtml = '<div class="card" style="margin-bottom:16px;">' + blockHtml + '</div>';
  }
  const voidHistory = req.session.lastSyncVoidHistory || [];
  let historyListHtml = '';
  if (voidHistory.length > 0) {
    const items = voidHistory.slice(0, MAX_SYNC_HISTORY);
    historyListHtml = '<div class="sync-history-section"><h3>과거 무효 동기화 내역 (최신순)</h3><div class="sync-history-list">' + items.map((entry, idx) => {
      const syncedAtStr = entry.syncedAt ? (function () { const d = new Date(entry.syncedAt); return isNaN(d.getTime()) ? entry.syncedAt : d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0') + ' ' + String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0'); })() : '';
      const summary = syncedAtStr + ' · 총 ' + (entry.total || 0) + '건 조회, ' + (entry.sent || 0) + '건 전송';
      const detailTable = buildSyncResultTableHtml(entry, '무효 동기화 조회 결과', escV);
      return '<div class="sync-history-item"><details><summary>' + escV(summary) + '</summary><div class="sync-history-detail">' + detailTable + '</div></details></div>';
    }).join('') + '</div></div>';
  }
  const confirmSyncVoid = (t(locale, 'cr_confirm_sync_void') || '').replace(/'/g, "\\'");
  const syncForm = '<form method="post" action="/admin/cancel-refund/sync-void" style="display:inline;" onsubmit="return confirm(\'' + confirmSyncVoid + '\');"><input type="hidden" name="env" value="' + escV(env) + '" /><button type="submit" class="btn-email">' + t(locale, 'cr_btn_sync_void') + '</button></form>';
  const { voidMap: voidSentMap } = buildVoidRefundNotiSentMaps(90);
  const filteredLogs = getEnvFilteredLogs(req);
  const reversed = [...filteredLogs].slice().reverse();
  const voidUiDeleted = loadVoidUiDeletedList();
  const voidList = reversed.filter((log) => {
    const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
    const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : null);
    const isSuccess = body.PaymentStatus === 0 || body.PaymentStatus === '0' || body.PaymentStatus === 1 || body.PaymentStatus === '1'
      || (body.paymentStatus && String(body.paymentStatus).toLowerCase() === 'success')
      || body.status === 1;
    if (!txId || !isSuccess || !log.merchantId || !MERCHANTS.get(log.merchantId)) return false;
    if (isVoidUiDeleted(txId, log.merchantId, env, voidUiDeleted)) return false;
    // 무효요청 가능 구간 판정: 화면에 보이는 노티 수신 시각(TH 기준)을 우선 사용
    const baseDate = log.receivedAtIso || log.receivedAt || body.TransactionDate || body.transactionDate || body.PaymentDate || body.paymentDate;
    const w = getVoidRefundWindow(baseDate);
    return w === 'void_auto' || w === 'void_manual';
  });
  const cfg = loadChillPayTransactionConfig();
  const rows = voidList.map((log) => {
    const realIndex = NOTI_LOGS.indexOf(log);
    const dt = formatDateAndTimeTHJP(log.receivedAtIso || log.receivedAt);
    const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
    // 무효요청 버튼 활성화 여부도 동일하게 노티 수신 시각 기준으로 판정
    const baseDate = log.receivedAtIso || log.receivedAt || body.TransactionDate || body.transactionDate || body.PaymentDate || body.paymentDate;
    const windowType = getVoidRefundWindow(baseDate);
    const txId = body.TransactionId ?? body.transactionId ?? '-';
    const orderNo = body.OrderNo ?? body.orderNo ?? '-';
    const amount = body.Amount ?? body.amount ?? '-';
    const amountNum = parseFloat(String(amount).replace(/,/g, ''));
    const cfgAmount = cfg;
    let amountHuman = '-';
    if (Number.isFinite(amountNum)) {
      const op = cfgAmount.amountDisplayOp || DEFAULT_AMOUNT_DISPLAY_OP;
      const val = Number.isFinite(cfgAmount.amountDisplayValue) ? cfgAmount.amountDisplayValue : DEFAULT_AMOUNT_DISPLAY_VALUE;
      let res = amountNum;
      if (op === '*') res = amountNum * val;
      else if (op === '/') res = val !== 0 ? amountNum / val : amountNum;
      else if (op === '+') res = amountNum + val;
      else if (op === '-') res = amountNum - val;
      amountHuman = formatAmountWithSeparator(res);
    }
    const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
    const routeNoDisplay = getRouteNoDisplay(merchant, log.routeKey);
    const confirmVoid = (t(locale, 'cr_confirm_void') || '').replace(/'/g, "\\'");
    let confirmVoid2Msg = t(locale, 'cr_confirm_void_second');
    if (!confirmVoid2Msg || confirmVoid2Msg === 'cr_confirm_void_second') {
      confirmVoid2Msg = '정말 무효 요청을 진행하시겠습니까? 칠페이와 가맹점/전산에 무효 노티가 전송됩니다.';
    }
    const confirmVoid2 = confirmVoid2Msg.replace(/'/g, "\\'");
    const sentEntry = voidSentMap[String(txId).trim()];
    const alreadyVoided = !!sentEntry;
    // 무효 컬럼: 이미 무효 노티를 보낸 건은 "무효완료"로 고정, 그 외에는 시간 구간에 따라 버튼/비활성 표시
    let voidHtml = '';
    if (alreadyVoided) {
      voidHtml = '<span class="btn-email-disabled" title="이미 무효 노티가 발송된 거래입니다.">무효완료</span>';
    } else if (windowType === 'void_auto') {
      // 전역 스크립트에서 data-confirm, data-confirm-second로 2단계 확인 처리
      voidHtml = `<form method="post" action="/admin/cancel-refund/void-request" style="display:inline;"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="env" value="${esc(env)}" /><button type="submit" class="btn-void" data-confirm="${confirmVoid2}" data-confirm-second="${confirmVoid}">${t(locale, 'cr_btn_void_request')}</button></form>`;
    } else {
      // 이메일발송 비활성 스타일과 동일하게, 텍스트는 "무효요청"만 노출
      voidHtml = '<span class="btn-email-disabled" title="자동 무효 가능 시간이 지나 무효 요청을 할 수 없습니다.">무효요청</span>';
    }
    // 관리 컬럼: 목록삭제만
    const manageHtml = '<a href="/admin/cancel-refund/void-delete-confirm?transactionId=' + encodeURIComponent(body.TransactionId ?? body.transactionId ?? '') + '&merchantId=' + encodeURIComponent(log.merchantId || '') + '&env=' + encodeURIComponent(env) + '&source=void" class="btn-delete-from-list" style="padding:4px 10px;font-size:12px;background:#9ca3af;color:#fff;border:none;border-radius:4px;cursor:pointer;text-decoration:none;margin-left:4px;">목록삭제</a>';
    let emailHtml = '';
    if (windowType !== 'void_auto') {
      const manualFrom = cfg.voidCutoffHour + ':' + String(cfg.voidCutoffMinute).padStart(2, '0');
      const manualTo = cfg.refundStartHour + ':' + String(cfg.refundStartMinute).padStart(2, '0');
      const manualWindowTip = (t(locale, 'cr_manual_email_window') || '') + ': ' + manualFrom + '~' + manualTo;
      if (isCurrentTimeInVoidManualWindow()) {
        const { subject, body: bodyText } = buildVoidEmailContent(log);
        const emailTo = (cfg.emailTo || 'help@chillpay.co.th').trim();
        const mailto = 'mailto:' + encodeURIComponent(emailTo) + '?subject=' + encodeURIComponent(subject) + '&body=' + encodeURIComponent(bodyText);
        emailHtml = `<a href="${mailto}" class="btn-email">${t(locale, 'cr_btn_email_send')}</a>`;
      } else {
        emailHtml = `<span class="btn-email-disabled" title="${manualWindowTip}">${t(locale, 'cr_btn_email_disabled')}</span>`;
      }
    }
    const sentDt = sentEntry ? formatDateAndTimeTHJP(sentEntry.sentAtIso || sentEntry.sentAt) : { date: '-', timeTh: '-', timeJp: '-' };
    return `<tr>
      <td class="col-date">${esc(dt.date)}</td>
      <td class="col-time">TH: ${esc(dt.timeTh)}<br><span class="time-jp">JP: ${esc(dt.timeJp)}</span></td>
      <td class="col-date">${esc(sentDt.date)}</td>
      <td class="col-time">TH: ${esc(sentDt.timeTh)}<br><span class="time-jp">JP: ${esc(sentDt.timeJp)}</span></td>
      <td class="col-narrow">${esc(routeNoDisplay)}</td>
      <td class="col-narrow">${esc(log.merchantId || '')}</td>
      <td class="col-narrow">${esc(txId)}</td>
      <td class="col-narrow">${esc(orderNo)}</td>
      <td class="col-narrow">${esc(amount)}</td>
      <td class="col-narrow">${esc(amountHuman)}</td>
      <td class="col-action">${voidHtml}</td>
      <td class="col-action">${manageHtml}</td>
      <td class="col-action">${emailHtml}</td>
    </tr>`;
  }).join('');
  const thead = '<thead><tr><th>' + t(locale, 'cr_th_received_date') + '</th><th>' + t(locale, 'cr_th_received_time') + '</th><th>전송일</th><th>전송시각</th><th>' + t(locale, 'cr_th_route_no') + '</th><th>' + t(locale, 'cr_th_merchant') + '</th><th>TransactionId</th><th>OrderNo</th><th>Amount</th><th>금액</th><th>무효</th><th>관리</th><th>이메일</th></tr></thead>';
  const voidNote = '<p class="hint" style="margin-bottom:12px;color:#6b7280;font-size:13px;">전송일·전송시각은 무효 노티를 가맹점/전산에 보낸 시각(일본·태국)입니다. 전산에 <strong>결제 내역이 없는</strong> 건은 무효 노티를 보내도 전산에서 반영되지 않거나 실패로 나올 수 있습니다. (미들웨어는 ChillPay 결제 성공 노티만 보유한 건을 목록에 띄우므로, 해당 결제 노티가 당시 전산에 전달되지 않았을 수 있습니다.) 결제 노티가 전산에 먼저 전달되었는지는 <a href="/admin/internal-result">전산 결과</a>에서 해당 TransactionId·결제 건을 확인하세요.</p>';
  const tableContent = voidNote + syncResultHtml + '<table>' + thead + '<tbody>' + rows + '</tbody></table>' + historyListHtml;
  res.send(renderCancelRefundPage(locale, adminUser, t(locale, 'nav_cancel_refund_void') + ' (' + voidList.length + ')', tableContent, alertHtml, req.originalUrl, req.session.member, req, syncForm, env));
});

app.get('/admin/cancel-refund/force-void', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const q = req.query || {};
  const escQ = (s) => (s ? String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '');
  const relayStatus = (r) => (r === 'ok' ? '성공' : r === 'fail' ? '실패' : r === 'skip' ? '미전송' : '');
  const okDetail = (q.relay || q.internal) ? ' 가맹점: ' + relayStatus(q.relay) + ', 전산: ' + relayStatus(q.internal) : '';
  let alertHtml = q.ok === '1' ? '<div class="alert alert-ok">' + t(locale, 'cr_alert_force_void_ok') + (okDetail ? '<br><span style="font-size:12px;">노티 전송 결과' + okDetail + '</span>' : '') + '</div>' : (q.fail === '1' && q.reason ? '<div class="alert alert-fail">' + escQ(q.reason) + '</div>' : '');
  if (q.deleted === '1') alertHtml += '<div class="alert alert-ok">목록에서 제거되었습니다. <a href="/admin/cancel-refund/void-deleted-list?env=' + encodeURIComponent(getEnvFromReq(req)) + '">삭제거래</a>에서 확인·복원할 수 있습니다.</div>';
  const { voidMap: voidSentMapForForce } = buildVoidRefundNotiSentMaps(90);
  const filteredLogs = getEnvFilteredLogs(req);
  const reversed = [...filteredLogs].slice().reverse();
  const forceVoidDeleted = loadVoidUiDeletedList();
  const forceVoidPageEnv = getEnvFromReq(req);
  const forceVoidList = reversed.filter((log) => {
    const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
    const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : null);
    const isSuccess = body.PaymentStatus === 0 || body.PaymentStatus === '0' || body.PaymentStatus === 1 || body.PaymentStatus === '1'
      || (body.paymentStatus && String(body.paymentStatus).toLowerCase() === 'success')
      || body.status === 1;
    if (!txId || !isSuccess || !log.merchantId || !MERCHANTS.get(log.merchantId)) return false;
    if (isVoidUiDeleted(txId, log.merchantId, forceVoidPageEnv, forceVoidDeleted)) return false;
    const baseDate = body.TransactionDate || body.transactionDate || body.PaymentDate || body.paymentDate || log.receivedAtIso || log.receivedAt;
    const w = getVoidRefundWindow(baseDate);
    return w === 'void_auto' || w === 'void_manual';
  });
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const confirmForceVoid = (t(locale, 'cr_confirm_force_void') || '').replace(/'/g, "\\'").replace(/"/g, '&quot;');
  const confirmForceVoid2 = (t(locale, 'cr_confirm_force_void_second') || '정말 승인(강제 무효)하시겠습니까? 가맹점과 전산에 무효 노티가 전송됩니다.').replace(/'/g, "\\'").replace(/"/g, '&quot;');
  const rows = forceVoidList.map((log) => {
    const realIndex = NOTI_LOGS.indexOf(log);
    const dt = formatDateAndTimeTHJP(log.receivedAtIso || log.receivedAt);
    const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
    const txId = body.TransactionId ?? body.transactionId ?? '-';
    const orderNo = body.OrderNo ?? body.orderNo ?? '-';
    const amount = body.Amount ?? body.amount ?? '-';
    const amountNum = parseFloat(String(amount).replace(/,/g, ''));
    const cfgAmount = loadChillPayTransactionConfig();
    let amountHuman = '-';
    if (Number.isFinite(amountNum)) {
      const op = cfgAmount.amountDisplayOp || DEFAULT_AMOUNT_DISPLAY_OP;
      const val = Number.isFinite(cfgAmount.amountDisplayValue) ? cfgAmount.amountDisplayValue : DEFAULT_AMOUNT_DISPLAY_VALUE;
      let res = amountNum;
      if (op === '*') res = amountNum * val;
      else if (op === '/') res = val !== 0 ? amountNum / val : amountNum;
      else if (op === '+') res = amountNum + val;
      else if (op === '-') res = amountNum - val;
      amountHuman = formatAmountWithSeparator(res);
    }
    const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
    const routeNoDisplay = getRouteNoDisplay(merchant, log.routeKey);
    const manageHtml = ' <a href="/admin/cancel-refund/void-delete-confirm?transactionId=' + encodeURIComponent(body.TransactionId ?? body.transactionId ?? '') + '&merchantId=' + encodeURIComponent(log.merchantId || '') + '&env=' + encodeURIComponent(forceVoidPageEnv) + '&source=force_void" class="btn-delete-from-list" style="padding:4px 10px;font-size:12px;background:#9ca3af;color:#fff;border:none;border-radius:4px;cursor:pointer;text-decoration:none;margin-left:4px;">목록삭제</a>';
    const alreadyVoided = !!voidSentMapForForce[String(txId).trim()];
    let action = '';
    if (alreadyVoided) {
      action = '<span class="btn-email-disabled" title="이미 무효 노티가 발송된 거래입니다.">무효완료</span>';
    } else {
      action = '<form method="post" action="/admin/cancel-refund/force-void-request" style="display:inline;"><input type="hidden" name="index" value="' + realIndex + '" /><input type="hidden" name="env" value="' + esc(forceVoidPageEnv) + '" /><button type="submit" class="btn-void" data-confirm="' + confirmForceVoid2 + '" data-confirm-second="' + confirmForceVoid + '">' + t(locale, 'cr_btn_force_void') + '</button></form>';
    }
    return `<tr>
      <td class="col-date">${esc(dt.date)}</td>
      <td class="col-time">TH: ${esc(dt.timeTh)}<br><span class="time-jp">JP: ${esc(dt.timeJp)}</span></td>
      <td class="col-narrow">${esc(routeNoDisplay)}</td>
      <td class="col-narrow">${esc(log.merchantId || '')}</td>
      <td class="col-narrow">${esc(txId)}</td>
      <td class="col-narrow">${esc(orderNo)}</td>
      <td class="col-narrow">${esc(amount)}</td>
      <td class="col-narrow">${esc(amountHuman)}</td>
      <td class="col-action">${manageHtml}</td>
      <td class="col-action">${action}</td>
    </tr>`;
  }).join('');
  const descHtml = '<p class="perm-legend" style="margin-bottom:12px;">' + (t(locale, 'cr_force_void_desc') || '').replace(/</g, '&lt;') + '</p><p class="hint" style="margin-bottom:12px;color:#6b7280;font-size:13px;">전산에 <strong>결제 내역이 없는</strong> 건은 무효 노티를 보내도 전산에서 반영되지 않거나 실패로 표시됩니다. 미들웨어에만 결제 성공 노티가 있고 전산에는 해당 결제가 없을 수 있으므로, <a href="/admin/internal-result">전산 결과</a>에서 해당 TransactionId의 결제 건 전달 여부를 확인하세요.</p>';
  const thead = '<thead><tr><th>' + t(locale, 'cr_th_received_date') + '</th><th>' + t(locale, 'cr_th_received_time') + '</th><th>' + t(locale, 'cr_th_route_no') + '</th><th>' + t(locale, 'cr_th_merchant') + '</th><th>TransactionId</th><th>OrderNo</th><th>Amount</th><th>금액</th><th>관리</th><th>강제무효</th></tr></thead>';
  const tableContent = descHtml + '<table>' + thead + '<tbody>' + rows + '</tbody></table>';
  res.send(renderCancelRefundPage(locale, adminUser, t(locale, 'nav_cancel_refund_force_void') + ' (' + forceVoidList.length + ')', tableContent, alertHtml, req.originalUrl, req.session.member, req, undefined, forceVoidPageEnv));
});

app.get('/admin/cancel-refund/void-delete-confirm', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const q = req.query || {};
  const transactionId = (q.transactionId || '').toString().trim();
  const merchantId = (q.merchantId || '').toString().trim();
  const env = (q.env || 'live').toString();
  const source = (q.source || 'void').toString();
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  if (!transactionId || !merchantId) {
    return res.redirect('/admin/cancel-refund/void?env=' + encodeURIComponent(env));
  }
  const sourceLabel = source === 'force_void' ? '강제 무효' : '무효거래';
  const backUrl = source === 'force_void' ? '/admin/cancel-refund/force-void?env=' + encodeURIComponent(env) : '/admin/cancel-refund/void?env=' + encodeURIComponent(env);
  const confirmBody = '<p class="hint" style="margin-bottom:12px;">이 건을 목록에서 제거합니다. 삭제한 관리자 아이디와 시각이 기록되며, <strong>삭제거래</strong>에 1달간 보관된 뒤 자동 삭제됩니다. 삭제거래에서 복원할 수 있습니다.</p>'
    + '<p style="margin-bottom:12px;"><strong>TransactionId</strong>: ' + esc(transactionId) + ' / <strong>가맹점</strong>: ' + esc(merchantId) + ' (' + sourceLabel + ')</p>'
    + '<form method="post" action="/admin/cancel-refund/void-delete" id="void-delete-form">'
    + '<input type="hidden" name="transactionId" value="' + esc(transactionId) + '" />'
    + '<input type="hidden" name="merchantId" value="' + esc(merchantId) + '" />'
    + '<input type="hidden" name="env" value="' + esc(env) + '" />'
    + '<input type="hidden" name="source" value="' + esc(source) + '" />'
    + '<p style="margin-bottom:12px;"><label><input type="checkbox" id="void-delete-confirm-cb" name="confirm" value="1" /> 위 내용을 확인했으며, 목록에서 제거(삭제)합니다.</label></p>'
    + '<button type="submit" id="void-delete-submit" style="padding:8px 16px;background:#9ca3af;color:#fff;border:none;border-radius:6px;cursor:not-allowed;" disabled>삭제 실행</button> '
    + '<a href="' + esc(backUrl) + '" style="padding:8px 16px;background:#6b7280;color:#fff;border-radius:6px;text-decoration:none;">취소</a>'
    + '</form>'
    + '<script>document.getElementById("void-delete-confirm-cb").addEventListener("change", function(){ var btn = document.getElementById("void-delete-submit"); btn.disabled = !this.checked; btn.style.cursor = this.checked ? "pointer" : "not-allowed"; btn.style.background = this.checked ? "#dc2626" : "#9ca3af"; });</script>';
  const tableContent = '<div class="card" style="max-width:560px;">' + confirmBody + '</div>';
  res.send(renderCancelRefundPage(locale, adminUser, '목록에서 제거 확인', tableContent, '', req.originalUrl, req.session.member, req, undefined, env));
});

app.post('/admin/cancel-refund/void-delete', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const transactionId = (req.body.transactionId || '').toString().trim();
  const merchantId = (req.body.merchantId || '').toString().trim();
  const env = (req.body.env || 'live').toString();
  const source = (req.body.source || 'void').toString();
  const deletedBy = (req.session.adminUser || 'unknown').toString();
  const backPath = source === 'force_void' ? '/admin/cancel-refund/force-void' : '/admin/cancel-refund/void';
  const backUrl = backPath + '?env=' + encodeURIComponent(env) + '&deleted=1';
  if (!transactionId || !merchantId) {
    return res.redirect(backPath + '?env=' + encodeURIComponent(env));
  }
  appendVoidUiDeleted({ transactionId, merchantId, env, source, deletedBy });
  return res.redirect(backUrl);
});

app.get('/admin/cancel-refund/void-deleted-list', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const q = req.query || {};
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const env = getEnvFromReq(req);
  const deletedList = loadVoidUiDeletedList();
  const filtered = deletedList.filter((d) => (String(d.env || 'live').toLowerCase() === (env === 'sandbox' ? 'sandbox' : 'live')));
  const sourceLabel = (s) => (s === 'force_void' ? '강제 무효' : '무효거래');
  const rows = filtered.map((d) => {
    const deletedAtStr = d.deletedAtIso ? new Date(d.deletedAtIso).toLocaleString('ko-KR', { hour12: false }) : '-';
    const restoreForm = '<form method="post" action="/admin/cancel-refund/void-deleted-restore" style="display:inline;"><input type="hidden" name="id" value="' + esc(d.id || '') + '" /><input type="hidden" name="env" value="' + esc(env) + '" /><button type="submit" style="padding:4px 10px;font-size:12px;background:#16a34a;color:#fff;border:none;border-radius:4px;cursor:pointer;">복원</button></form>';
    return '<tr><td class="col-narrow">' + esc(d.transactionId || '') + '</td><td class="col-narrow">' + esc(d.merchantId || '') + '</td><td class="col-narrow">' + sourceLabel(d.source) + '</td><td class="col-date">' + esc(deletedAtStr) + '</td><td class="col-narrow">' + esc(d.deletedBy || '') + '</td><td class="col-action">' + restoreForm + '</td></tr>';
  }).join('');
  const thead = '<thead><tr><th>TransactionId</th><th>가맹점</th><th>출처</th><th>삭제 시각</th><th>삭제한 아이디</th><th>복원</th></tr></thead>';
  const hint = '<p class="hint" style="margin-bottom:12px;color:#6b7280;">무효거래/강제 무효 목록에서 「목록에서 제거」한 건이 1달간 보관됩니다. 1달이 지나면 자동 삭제되어 다시 해당 목록에 노출됩니다. 복원 시 해당 목록에 다시 표시됩니다.</p>';
  let listAlert = '';
  if (q.restore === 'ok') listAlert = '<div class="alert alert-ok">복원되었습니다. 무효거래/강제 무효 목록에서 다시 확인할 수 있습니다.</div>';
  if (q.restore === 'fail') listAlert = '<div class="alert alert-fail">복원 처리에 실패했거나 이미 삭제된 항목일 수 있습니다.</div>';
  const tableContent = listAlert + hint + '<table>' + thead + '<tbody>' + rows + '</tbody></table>';
  res.send(renderCancelRefundPage(locale, adminUser, '삭제거래 (' + filtered.length + ')', tableContent, '', req.originalUrl, req.session.member, req, undefined, env));
});

app.post('/admin/cancel-refund/void-deleted-restore', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const id = (req.body.id || '').toString().trim();
  const env = (req.body.env || 'live').toString();
  if (!id) return res.redirect('/admin/cancel-refund/void-deleted-list?env=' + encodeURIComponent(env));
  const ok = removeVoidUiDeletedById(id);
  return res.redirect('/admin/cancel-refund/void-deleted-list?env=' + encodeURIComponent(env) + '&restore=' + (ok ? 'ok' : 'fail'));
});

app.post('/admin/cancel-refund/sync-void', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const locale = getLocale(req);
  const env = (req.body.env || 'live').toString().toLowerCase() === 'sandbox' ? 'sandbox' : 'live';
  try {
    const result = await syncChillPayVoidNoti();
    if (result.success) {
      const entry = {
        syncedAt: result.syncedAt || new Date().toISOString(),
        total: result.total || 0,
        sent: result.sent || 0,
        alreadySent: result.alreadySent || 0,
        noMatch: result.noMatch || 0,
        items: result.items || [],
      };
      req.session.lastSyncVoid = entry;
      req.session.lastSyncVoidHistory = req.session.lastSyncVoidHistory || [];
      req.session.lastSyncVoidHistory.unshift(entry);
      if (req.session.lastSyncVoidHistory.length > MAX_SYNC_HISTORY) req.session.lastSyncVoidHistory.length = MAX_SYNC_HISTORY;
      const params = new URLSearchParams({ sync: 'ok', sent: String(result.sent || 0), total: String(result.total || 0), env });
      if (result.alreadySent != null) params.set('alreadySent', String(result.alreadySent));
      if (result.noMatch != null) params.set('noMatch', String(result.noMatch));
      return res.redirect('/admin/cancel-refund/void?' + params.toString());
    }
    return res.redirect('/admin/cancel-refund/void?sync=fail&reason=' + encodeURIComponent(result.error || t(locale, 'cr_alert_sync_fail')) + '&env=' + env);
  } catch (err) {
    const msg = err && err.message ? err.message : String(err);
    return res.redirect('/admin/cancel-refund/void?sync=fail&reason=' + encodeURIComponent(msg) + '&env=' + env);
  }
});

app.get('/admin/cancel-refund/refund', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const q = req.query || {};
  const escQ = (s) => (s ? String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '');
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  let alertHtml = q.refund === 'ok' ? '<div class="alert alert-ok">환불 요청이 완료되었습니다.</div>' : q.refund === 'fail' && q.reason ? '<div class="alert alert-fail">환불 요청 실패: ' + escQ(q.reason) + '</div>' : '';
  if (q.sync === 'ok') {
    const sent = parseInt(q.sent, 10) || 0;
    const total = parseInt(q.total, 10) || 0;
    const alreadySent = parseInt(q.alreadySent, 10) || 0;
    const noMatch = parseInt(q.noMatch, 10) || 0;
    let syncMsg = 'ChillPay 환불 건 동기화 완료. 조회 ' + total + '건 중 환불 노티 전송 ' + sent + '건.';
    if (sent === 0 && (alreadySent > 0 || noMatch > 0)) {
      const parts = [];
      if (alreadySent > 0) parts.push('이미 전송 ' + alreadySent + '건');
      if (noMatch > 0) parts.push('로그 미매칭 ' + noMatch + '건');
      syncMsg += ' (원인: ' + parts.join(', ') + '. ChillPay 조회 건의 TransactionId가 이 서버의 거래내역(NOTI_LOGS)에 없거나, 가맹점 미등록 시 미전송됩니다.)';
    }
    alertHtml += '<div class="alert alert-ok">' + syncMsg + '</div>';
  } else if (q.sync === 'fail' && q.reason) {
    alertHtml += '<div class="alert alert-fail">동기화 실패: ' + escQ(q.reason) + '</div>';
  }
  const env = getEnvFromReq(req);
  const escR = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const cfgRefund = loadChillPayTransactionConfig();
  const syncResultValidMsRefund = (Number(cfgRefund.syncResultDisplayMinutes) > 0 ? cfgRefund.syncResultDisplayMinutes : 30) * 60 * 1000;
  let syncResultHtml = '';
  const lastRefund = req.session.lastSyncRefund;
  const refundWithinWindow = lastRefund && lastRefund.syncedAt && (Date.now() - new Date(lastRefund.syncedAt).getTime() < syncResultValidMsRefund);
  if (refundWithinWindow && lastRefund && Array.isArray(lastRefund.items) && lastRefund.items.length > 0) {
    const blockHtml = buildSyncResultTableHtml(lastRefund, '최근 환불 동기화 조회 결과 (ChillPay에서 가져온 목록)', escR);
    syncResultHtml = '<div class="card" style="margin-bottom:16px;">' + blockHtml + '</div>';
  }
  const refundHistory = req.session.lastSyncRefundHistory || [];
  let historyListHtml = '';
  if (refundHistory.length > 0) {
    const items = refundHistory.slice(0, MAX_SYNC_HISTORY);
    historyListHtml = '<div class="sync-history-section"><h3>과거 환불 동기화 내역 (최신순)</h3><div class="sync-history-list">' + items.map((entry) => {
      const syncedAtStr = entry.syncedAt ? (function () { const d = new Date(entry.syncedAt); return isNaN(d.getTime()) ? entry.syncedAt : d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0') + ' ' + String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0'); })() : '';
      const summary = syncedAtStr + ' · 총 ' + (entry.total || 0) + '건 조회, ' + (entry.sent || 0) + '건 전송';
      const detailTable = buildSyncResultTableHtml(entry, '환불 동기화 조회 결과', escR);
      return '<div class="sync-history-item"><details><summary>' + escR(summary) + '</summary><div class="sync-history-detail">' + detailTable + '</div></details></div>';
    }).join('') + '</div></div>';
  }
  const confirmSyncRefund = (t(locale, 'cr_confirm_sync_refund') || '').replace(/'/g, "\\'");
  const syncForm = '<form method="post" action="/admin/cancel-refund/sync-refund" style="display:inline;" onsubmit="return confirm(\'' + confirmSyncRefund + '\');"><input type="hidden" name="env" value="' + escR(env) + '" /><button type="submit" class="btn-email">' + t(locale, 'cr_btn_sync_refund') + '</button></form>';
  const { refundMap: refundSentMap } = buildVoidRefundNotiSentMaps(90);
  const filteredLogs = getEnvFilteredLogs(req);
  const reversed = [...filteredLogs].slice().reverse();
  const refundList = reversed.filter((log) => {
    const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
    const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : null);
    const isSuccess = body.PaymentStatus === 0 || body.PaymentStatus === '0' || body.PaymentStatus === 1 || body.PaymentStatus === '1'
      || (body.paymentStatus && String(body.paymentStatus).toLowerCase() === 'success')
      || body.status === 1;
    if (!txId || !isSuccess || !log.merchantId || !MERCHANTS.get(log.merchantId)) return false;
    const payDate = body.PaymentDate || body.paymentDate || log.receivedAtIso || log.receivedAt;
    // 환불거래 목록: 환불 가능 기간 내인 건만 노출 (PaymentDate 기준)
    return isWithinRefundWindow(payDate);
  });
  const rows = refundList.map((log) => {
    const realIndex = NOTI_LOGS.indexOf(log);
    const dt = formatDateAndTimeTHJP(log.receivedAtIso || log.receivedAt);
    const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
    const txId = body.TransactionId ?? body.transactionId ?? '-';
    const orderNo = body.OrderNo ?? body.orderNo ?? '-';
    const amount = body.Amount ?? body.amount ?? '-';
    const amountNum = parseFloat(String(amount).replace(/,/g, ''));
    let amountHuman = '-';
    if (Number.isFinite(amountNum)) {
      const op = cfgRefund.amountDisplayOp || DEFAULT_AMOUNT_DISPLAY_OP;
      const val = Number.isFinite(cfgRefund.amountDisplayValue) ? cfgRefund.amountDisplayValue : DEFAULT_AMOUNT_DISPLAY_VALUE;
      let res = amountNum;
      if (op === '*') res = amountNum * val;
      else if (op === '/') res = val !== 0 ? amountNum / val : amountNum;
      else if (op === '+') res = amountNum + val;
      else if (op === '-') res = amountNum - val;
      amountHuman = formatAmountWithSeparator(res);
    }
    const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
    const routeNoDisplay = getRouteNoDisplay(merchant, log.routeKey);
    const confirmRefund = (t(locale, 'cr_confirm_refund') || '').replace(/'/g, "\\'").replace(/"/g, '&quot;');
    const confirmRefund2 = (t(locale, 'cr_confirm_refund_second') || '정말 승인(환불)하시겠습니까? 가맹점과 전산에 환불 노티가 전송됩니다.').replace(/'/g, "\\'").replace(/"/g, '&quot;');
    const nowIso = new Date().toISOString();
    const canRefundNow = isWithinRefundWindow(body.PaymentDate || body.paymentDate || log.receivedAtIso || log.receivedAt, nowIso);
    const manageHtml = '-';
    const refundHtml = canRefundNow
      ? `<form method="post" action="/admin/cancel-refund/refund-request" style="display:inline;" onsubmit="return confirm('${confirmRefund}') && confirm('${confirmRefund2}');"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="env" value="${escR(env)}" /><button type="submit" class="btn-refund">${t(locale, 'cr_btn_refund_request')}</button></form>`
      : '<span class="btn-refund-disabled" title="환경설정에서 지정한 환불 가능 기간을 지났거나 아직 시작 전입니다.">환불 기간 아님</span>';
    const sentEntry = refundSentMap[String(txId).trim()];
    const sentDt = sentEntry ? formatDateAndTimeTHJP(sentEntry.sentAtIso || sentEntry.sentAt) : { date: '-', timeTh: '-', timeJp: '-' };
    return `<tr>
      <td class="col-date">${esc(dt.date)}</td>
      <td class="col-time">TH: ${esc(dt.timeTh)}<br><span class="time-jp">JP: ${esc(dt.timeJp)}</span></td>
      <td class="col-date">${esc(sentDt.date)}</td>
      <td class="col-time">TH: ${esc(sentDt.timeTh)}<br><span class="time-jp">JP: ${esc(sentDt.timeJp)}</span></td>
      <td class="col-narrow">${esc(routeNoDisplay)}</td>
      <td class="col-narrow">${esc(log.merchantId || '')}</td>
      <td class="col-narrow">${esc(txId)}</td>
      <td class="col-narrow">${esc(orderNo)}</td>
      <td class="col-narrow">${esc(amount)}</td>
      <td class="col-narrow">${esc(amountHuman)}</td>
      <td class="col-action">${manageHtml}</td>
      <td class="col-action">${refundHtml}</td>
    </tr>`;
  }).join('');
  const thead = '<thead><tr><th>' + t(locale, 'cr_th_received_date') + '</th><th>' + t(locale, 'cr_th_received_time') + '</th><th>전송일</th><th>전송시각</th><th>' + t(locale, 'cr_th_route_no') + '</th><th>' + t(locale, 'cr_th_merchant') + '</th><th>TransactionId</th><th>OrderNo</th><th>Amount</th><th>금액</th><th>관리</th><th>환불</th></tr></thead>';
  const tableContent = syncResultHtml + '<table>' + thead + '<tbody>' + rows + '</tbody></table>' + historyListHtml;
  res.send(renderCancelRefundPage(locale, adminUser, t(locale, 'nav_cancel_refund_refund') + ' (' + refundList.length + ')', tableContent, alertHtml, req.originalUrl, req.session.member, req, syncForm, env));
});

app.post('/admin/cancel-refund/sync-refund', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const locale = getLocale(req);
  const env = (req.body.env || 'live').toString().toLowerCase() === 'sandbox' ? 'sandbox' : 'live';
  try {
    const result = await syncChillPayRefundNoti();
    if (result.success) {
      const entry = {
        syncedAt: result.syncedAt || new Date().toISOString(),
        total: result.total || 0,
        sent: result.sent || 0,
        alreadySent: result.alreadySent || 0,
        noMatch: result.noMatch || 0,
        items: result.items || [],
      };
      req.session.lastSyncRefund = entry;
      req.session.lastSyncRefundHistory = req.session.lastSyncRefundHistory || [];
      req.session.lastSyncRefundHistory.unshift(entry);
      if (req.session.lastSyncRefundHistory.length > MAX_SYNC_HISTORY) req.session.lastSyncRefundHistory.length = MAX_SYNC_HISTORY;
      const params = new URLSearchParams({ sync: 'ok', sent: String(result.sent || 0), total: String(result.total || 0), env });
      if (result.alreadySent != null) params.set('alreadySent', String(result.alreadySent));
      if (result.noMatch != null) params.set('noMatch', String(result.noMatch));
      return res.redirect('/admin/cancel-refund/refund?' + params.toString());
    }
    return res.redirect('/admin/cancel-refund/refund?sync=fail&reason=' + encodeURIComponent(result.error || t(locale, 'cr_alert_sync_fail')) + '&env=' + env);
  } catch (err) {
    const msg = err && err.message ? err.message : String(err);
    return res.redirect('/admin/cancel-refund/refund?sync=fail&reason=' + encodeURIComponent(msg) + '&env=' + env);
  }
});

app.post('/admin/cancel-refund/force-void-request', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const locale = getLocale(req);
  const forceEnv = (req.body.env || req.query.env || 'live').toString().toLowerCase().trim() === 'sandbox' ? 'sandbox' : 'live';
  const index = parseInt(req.body.index, 10);
  if (Number.isNaN(index) || index < 0 || index >= NOTI_LOGS.length) {
    return res.redirect('/admin/cancel-refund/force-void?fail=1&reason=' + encodeURIComponent(t(locale, 'cr_err_invalid_index')) + '&env=' + encodeURIComponent(forceEnv));
  }
  const log = NOTI_LOGS[index];
  const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
  const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : null);
  const forceVoidEnv = (String(log.env || '').toLowerCase().trim() === 'sandbox' ? 'sandbox' : 'live');
  if (!txId) return res.redirect('/admin/cancel-refund/force-void?fail=1&reason=' + encodeURIComponent(t(locale, 'cr_err_no_transaction_id')) + '&env=' + encodeURIComponent(forceVoidEnv));
  const sendResult = await sendVoidOrRefundNoti(log, 'void');
  markVoidNotiSent(txId);
  const relay = sendResult.success ? 'ok' : (sendResult.error && sendResult.error.indexOf('가맹점') !== -1 ? 'skip' : 'fail');
  const internal = sendResult.internalOk === true ? 'ok' : (sendResult.internalOk === false ? 'fail' : 'skip');
  return res.redirect('/admin/cancel-refund/force-void?ok=1&relay=' + relay + '&internal=' + internal + '&env=' + encodeURIComponent(forceVoidEnv));
});

app.post('/admin/cancel-refund/void-request', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const locale = getLocale(req);
  const voidReqEnv = (req.body.env || req.query.env || 'live').toString().toLowerCase().trim() === 'sandbox' ? 'sandbox' : 'live';
  const index = parseInt(req.body.index, 10);
  if (Number.isNaN(index) || index < 0 || index >= NOTI_LOGS.length) {
    return res.redirect('/admin/cancel-refund/void?void=fail&reason=' + encodeURIComponent(t(locale, 'cr_err_invalid_index')) + '&env=' + encodeURIComponent(voidReqEnv));
  }
  const log = NOTI_LOGS[index];
  const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
  const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : null);
  if (!txId) return res.redirect('/admin/cancel-refund/void?void=fail&reason=' + encodeURIComponent(t(locale, 'cr_err_no_transaction_id')) + '&env=' + encodeURIComponent((String(log.env || '').toLowerCase().trim() === 'sandbox' ? 'sandbox' : 'live')));
  const cfg = loadChillPayTransactionConfig();
  const result = await chillPayRequestVoid(txId, false);
  const voidEnv = (String(log.env || '').toLowerCase().trim() === 'sandbox' ? 'sandbox' : 'live');
  if (!result.success) return res.redirect('/admin/cancel-refund/void?void=fail&reason=' + encodeURIComponent(result.error || t(locale, 'cr_alert_void_fail')) + '&env=' + encodeURIComponent(voidEnv));
  if (!hasVoidNotiSent(txId)) {
    await sendVoidOrRefundNoti(log, 'void');
    markVoidNotiSent(txId);
  }
  return res.redirect('/admin/cancel-refund/void?void=ok&env=' + encodeURIComponent(voidEnv));
});

app.post('/admin/cancel-refund/refund-request', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const locale = getLocale(req);
  const refundReqEnv = (req.body.env || req.query.env || 'live').toString().toLowerCase().trim() === 'sandbox' ? 'sandbox' : 'live';
  const index = parseInt(req.body.index, 10);
  if (Number.isNaN(index) || index < 0 || index >= NOTI_LOGS.length) {
    return res.redirect('/admin/cancel-refund/refund?refund=fail&reason=' + encodeURIComponent(t(locale, 'cr_err_invalid_index')) + '&env=' + encodeURIComponent(refundReqEnv));
  }
  const log = NOTI_LOGS[index];
  const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
  const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : null);
  if (!txId) return res.redirect('/admin/cancel-refund/refund?refund=fail&reason=' + encodeURIComponent(t(locale, 'cr_err_no_transaction_id')) + '&env=' + encodeURIComponent((String(log.env || '').toLowerCase().trim() === 'sandbox' ? 'sandbox' : 'live')));
  const cfg = loadChillPayTransactionConfig();
  const result = await chillPayRequestRefund(txId, false);
  const refundEnv = (String(log.env || '').toLowerCase().trim() === 'sandbox' ? 'sandbox' : 'live');
  if (!result.success) return res.redirect('/admin/cancel-refund/refund?refund=fail&reason=' + encodeURIComponent(result.error || t(locale, 'cr_alert_refund_fail')) + '&env=' + encodeURIComponent(refundEnv));
  await sendVoidOrRefundNoti(log, 'refund');
  markRefundNotiSent(txId);
  return res.redirect('/admin/cancel-refund/refund?refund=ok&env=' + encodeURIComponent(refundEnv));
});

app.post('/admin/logs/resend', requireAuth, requirePageAny(['pg_logs', 'pg_result']), async (req, res) => {
  const returnTo = (req.body.returnTo || '').trim() || 'logs';
  const base = returnTo === 'logs-result' ? '/admin/logs-result' : '/admin/logs';
  const index = parseInt(req.body.index, 10);
  if (Number.isNaN(index) || index < 0 || index >= NOTI_LOGS.length) {
    return res.redirect(base + '?err=invalid');
  }
  const log = NOTI_LOGS[index];
  const targetUrl = (log.targetUrl && String(log.targetUrl).trim()) || (findMerchantByRouteKey(log.routeKey) && findMerchantByRouteKey(log.routeKey).targetUrl) || '';
  if (!targetUrl) {
    return res.redirect(base + '?err=no_target&reason=' + encodeURIComponent('가맹점 URL 없음'));
  }
  const hasRawBody = log.rawBody !== undefined && log.rawBody !== null && String(log.rawBody).trim() !== '';
  const hasBody = log.body !== undefined && log.body !== null && (typeof log.body === 'object' || typeof log.body === 'string');
  if (!hasRawBody && !hasBody) {
    return res.redirect(base + '?err=no_body&reason=' + encodeURIComponent('노티 본문 없음'));
  }
  const forceJson = req.body.resendAsJson === '1' || req.body.resendAsJson === 'true';
  const forceForm = req.body.resendAsForm === '1' || req.body.resendAsForm === 'true';
  const contentType = forceJson ? 'application/json' : forceForm ? 'application/x-www-form-urlencoded' : ((log.contentType || '').toString().trim() || 'application/json');
  try {
    let relayRes;
    if (hasRawBody && !forceJson && !forceForm) {
      // ChillPay에서 받은 원문 그대로 전송 (가공 없음)
      relayRes = await relayToMerchant(targetUrl, null, { contentType, rawBody: log.rawBody });
    } else {
      // rawBody 없음 또는 재전송(JSON)/재전송(FORM) 선택 시: body 객체로 전송
      let bodyToSend = log.body;
      if (typeof bodyToSend === 'string') {
        try { bodyToSend = JSON.parse(bodyToSend); } catch { return res.redirect(base + '?err=no_body&reason=' + encodeURIComponent('노티 본문 파싱 실패')); }
      }
      if (!bodyToSend || typeof bodyToSend !== 'object' || Array.isArray(bodyToSend)) {
        return res.redirect(base + '?err=no_body&reason=' + encodeURIComponent('노티 본문 형식 오류'));
      }
      relayRes = await relayToMerchant(targetUrl, bodyToSend, { contentType });
    }
    const ok = relayRes.status >= 200 && relayRes.status < 300;
    const resendKind = (req.body.resendKind || '').toString().toLowerCase() || (() => {
      const b = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body || '{}'); } catch { return {}; } })() : {});
      return isCancelNotiBody(b) ? 'cancel' : 'payment';
    })();
    if (ok) {
      const formatUsed = forceJson ? 'json' : forceForm ? 'form' : 'raw';
      if (NOTI_LOGS[index]) {
        NOTI_LOGS[index].relayStatus = 'ok';
        NOTI_LOGS[index].relayFailReason = '';
        NOTI_LOGS[index].relayFormatUsed = formatUsed;
      }
      return res.redirect(base + '?resend=ok&resendKind=' + encodeURIComponent(resendKind));
    }
    const bodyPart = relayRes.data != null
      ? (typeof relayRes.data === 'string' ? String(relayRes.data) : JSON.stringify(relayRes.data))
      : '';
    const reason = `HTTP ${relayRes.status}` + (bodyPart ? ': ' + bodyPart.slice(0, 300) : '');
    return res.redirect(base + '?resend=fail&reason=' + encodeURIComponent(reason) + '&resendKind=' + encodeURIComponent(resendKind));
  } catch (err) {
    const reason = err.code || err.message || String(err);
    const returnTo = (req.body.returnTo || '').trim() || 'logs';
    const base = returnTo === 'logs-result' ? '/admin/logs-result' : '/admin/logs';
    const resendKind = (req.body.resendKind || '').toString().toLowerCase() || 'payment';
    return res.redirect(base + '?resend=fail&reason=' + encodeURIComponent(reason) + '&resendKind=' + encodeURIComponent(resendKind));
  }
});

// 피지결과 (요약) 페이지
app.get('/admin/logs-result', requireAuth, requirePage('pg_result'), (req, res) => {
  const locale = getLocale(req);
  const q = req.query || {};
  const txFilter = (q.txId || '').toString().trim();
  const resendKind = (q.resendKind || 'payment').toString().toLowerCase();
  const resendOkLabel = resendKind === 'cancel' ? '취소 재전송이 완료되었습니다.' : '결제 재전송이 완료되었습니다.';
  const resendFailLabel = resendKind === 'cancel' ? '취소 재전송 실패' : '결제 재전송 실패';
  const resendMsg =
    q.resend === 'ok'
      ? '<div class="alert alert-ok">' + resendOkLabel + '</div>'
      : q.resend === 'fail' && q.reason
      ? '<div class="alert alert-fail">' + resendFailLabel + ': ' + String(q.reason).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') + '</div>'
      : q.err === 'invalid'
      ? '<div class="alert alert-fail">잘못된 요청입니다.</div>'
      : q.err === 'no_target' || q.err === 'no_body'
      ? '<div class="alert alert-fail">' + (q.reason ? String(q.reason).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : (q.err === 'no_target' ? '가맹점 URL 없음' : '노티 본문 없음')) + '</div>'
      : '';
  const clientIp = (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() || req.ip || '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const liveLogs = NOTI_LOGS.filter((log) => (log.env || '') !== 'sandbox');
  let reversed = [...liveLogs].slice().reverse();
  if (txFilter) {
    reversed = reversed.filter((log) => {
      const body = parseNotiBody(log);
      const txId = body.TransactionId != null ? String(body.TransactionId) : (body.transactionId != null ? String(body.transactionId) : '');
      return txId && txId === txFilter;
    });
  }
  const rows = reversed
    .map((log) => {
      const realIndex = NOTI_LOGS.indexOf(log);
      const dt = formatDateAndTimeTHJP(log.receivedAtIso || log.receivedAt);
      const envLabel = (log.env && String(log.env).toLowerCase()) === 'sandbox' ? 'sandbox' : 'live';
      const routeNo = (log.routeKey && (log.routeKey.match(/\/(\d+)$/) || [null, log.routeKey])[1]) || log.routeKey || '-';
      const relayStatus = log.relayStatus || '-';
      const relayLabel = relayStatus === 'ok' ? '성공' : relayStatus === 'fail' ? '실패' : relayStatus === 'skip' ? '미전송' : relayStatus;
      const formatUsed = log.relayFormatUsed || (log.relaySentAsJson === true ? 'json' : 'raw');
      const relayClass = relayStatus === 'ok' ? (formatUsed === 'json' ? 'status-ok' : formatUsed === 'form' ? 'status-ok-form' : 'status-ok-normal') : relayStatus === 'fail' ? 'status-fail' : '';
      const failReason = (log.relayFailReason || '').trim();
      const canResend = (relayStatus === 'fail' || relayStatus === 'ok') && (log.targetUrl || findMerchantByRouteKey(log.routeKey)) && (log.body || log.rawBody);
      const body = parseNotiBody(log);
      const resendKindVal = isCancelNotiBody(body) ? 'cancel' : 'payment';
      const resendKindLabel = resendKindVal === 'cancel' ? '취소' : '결제';
      const resendBtn = canResend
        ? `<div class="resend-wrap" style="display:inline-flex;flex-direction:row;gap:6px;align-items:center;flex-wrap:nowrap;"><span class="resend-kind-label" style="font-size:11px;color:#6b7280;margin-right:4px;">${resendKindLabel}</span><form method="post" action="/admin/logs/resend" style="display:inline;"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="returnTo" value="logs-result" /><input type="hidden" name="resendKind" value="${resendKindVal}" /><button type="submit" class="btn-resend" onclick="return confirm('일반(원문) 형식으로 다시 전송하시겠습니까?');">일반</button></form><form method="post" action="/admin/logs/resend" style="display:inline;"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="returnTo" value="logs-result" /><input type="hidden" name="resendKind" value="${resendKindVal}" /><input type="hidden" name="resendAsJson" value="1" /><button type="submit" class="btn-resend-json" onclick="return confirm('JSON 형식으로 다시 전송하시겠습니까?');">JSON</button></form><form method="post" action="/admin/logs/resend" style="display:inline;"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="returnTo" value="logs-result" /><input type="hidden" name="resendKind" value="${resendKindVal}" /><input type="hidden" name="resendAsForm" value="1" /><button type="submit" class="btn-resend-form" onclick="return confirm('FORM 형식으로 다시 전송하시겠습니까?');">FORM</button></form></div>`
        : '-';
      const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : '-');
      const orderNo = body.OrderNo != null ? body.OrderNo : (body.orderNo != null ? body.orderNo : '-');
      const amtRaw = body.Amount != null ? body.Amount : (body.amount != null ? body.amount : '');
      const amtDisplay = amtRaw !== '' && amtRaw != null ? formatAmountWithSeparator(amtRaw) : '-';
      const currency = formatCurrencyForDisplay(body.Currency || body.currency) || '';
      // 금액 계산식 적용 (환경설정과 동일 로직) → 「금액」 컬럼
      const cfgAmount = loadChillPayTransactionConfig();
      const amtNum = parseFloat(String(amtRaw).replace(/,/g, ''));
      let amtHuman = '-';
      if (Number.isFinite(amtNum)) {
        const op = cfgAmount.amountDisplayOp || DEFAULT_AMOUNT_DISPLAY_OP;
        const val = Number.isFinite(cfgAmount.amountDisplayValue) ? cfgAmount.amountDisplayValue : DEFAULT_AMOUNT_DISPLAY_VALUE;
        let res = amtNum;
        if (op === '*') res = amtNum * val;
        else if (op === '/') res = val !== 0 ? amtNum / val : amtNum;
        else if (op === '+') res = amtNum + val;
        else if (op === '-') res = amtNum - val;
        amtHuman = formatAmountWithSeparator(res);
      }
      return `<tr>
        <td>${esc(dt.date)}</td>
        <td>TH: ${esc(dt.timeTh)}<br><span class="time-jp">JP: ${esc(dt.timeJp)}</span></td>
        <td>${esc(routeNo)}</td>
        <td>${esc(envLabel)}</td>
        <td>${esc(log.merchantId || '-')}</td>
        <td>${esc(String(txId))}</td>
        <td>${esc(String(orderNo))}</td>
        <td>${esc(amtDisplay)}</td>
        <td>${esc(currency || '-')}</td>
        <td>${esc(amtHuman)}</td>
        <td><span class="${relayClass}">${esc(relayLabel)}</span></td>
        <td class="col-fail-reason">${failReason ? esc(failReason) : '-'}</td>
        <td>${resendBtn}</td>
      </tr>`;
    })
    .join('');

  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'nav_pg_result') || '피지 결과'}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
    table { border-collapse: collapse; width: 100%; background:#fff; font-size: 13px; }
    th, td { border: 1px solid #e5e7eb; padding: 8px 10px; vertical-align: middle; text-align: center; }
    th { background: #e5f0ff; }
    tr:nth-child(even) { background:#f9fafb; }
    .status-ok { color: #059669; font-weight: 600; }
    .status-ok-normal { color: #2563eb; font-weight: 600; }
    .status-ok-form { color: #ca8a04; font-weight: 600; }
    .status-fail { color: #dc2626; font-weight: 600; }
    .time-jp { color: #2563eb; }
    .col-fail-reason { text-align: center; word-break: break-all; max-width: 200px; }
    .btn-resend { padding: 4px 10px; font-size: 12px; background: #2563eb; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
    .btn-resend-json { padding: 4px 10px; font-size: 12px; background: #059669; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
    .btn-resend-json:hover { background: #047857; }
    .btn-resend-form { padding: 4px 10px; font-size: 12px; background: #ca8a04; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
    .btn-resend-form:hover { background: #a16207; }
    .alert { padding: 10px 14px; border-radius: 8px; margin-bottom: 12px; font-size: 13px; }
    .alert-ok { background: #d1fae5; color: #065f46; border: 1px solid #6ee7b7; }
    .alert-fail { background: #fee2e2; color: #991b1b; border: 1px solid #fca5a5; }
    .layout { display:flex; min-height:100vh; width:100%; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
    .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); border:1px solid #e5e7eb; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card">
      ${resendMsg}
      <h1>${t(locale, 'nav_pg_result') || '피지 결과'} (${liveLogs.length})</h1>
      <p style="font-size:13px;color:#555;">노티 수신·가맹점 전달 결과 요약 (성공/실패·실패 사유·재전송) ㅣ 성공 <span style="color:#2563eb;font-weight:600;">파란색</span>=일반, <span style="color:#059669;font-weight:600;">녹색</span>=JSON, <span style="color:#ca8a04;font-weight:600;">노란색</span>=FORM</p>
      <form method="get" action="/admin/logs-result" style="margin-bottom:10px;font-size:12px;display:flex;flex-wrap:wrap;gap:6px;align-items:center;">
        <label style="display:flex;align-items:center;gap:4px;">
          <span>TransactionId 검색</span>
          <input type="text" name="txId" value="${esc(txFilter)}" placeholder="예: 30119992" style="padding:4px 8px;font-size:12px;border-radius:4px;border:1px solid #d1d5db;min-width:140px;" />
        </label>
        <button type="submit" style="padding:4px 10px;font-size:12px;background:#2563eb;color:#fff;border:none;border-radius:4px;cursor:pointer;">검색</button>
        ${txFilter ? `<a href="/admin/logs-result" style="margin-left:4px;font-size:12px;color:#2563eb;text-decoration:none;">검색 초기화</a>` : ''}
      </form>
      <table>
        <thead>
          <tr>
            <th>수신일</th>
            <th>수신시각</th>
            <th>route</th>
            <th>환경</th>
            <th>merchant id</th>
            <th>TransactionId</th>
            <th>OrderNo</th>
            <th>Amount</th>
            <th>Currency</th>
            <th>금액</th>
            <th>성공유무</th>
            <th>실패원인</th>
            <th>재전송</th>
          </tr>
        </thead>
        <tbody>
          ${rows || '<tr><td colspan="11" style="text-align:center;color:#777;">데이터 없음</td></tr>'}
        </tbody>
      </table>
      </div>
    </main>
  </div>
</body>
</html>`);
});

// 전산 노티 대상 관리 페이지
app.get('/admin/internal-targets', requireAuth, requirePage('internal_targets'), (req, res) => {
  const locale = getLocale(req);
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });

  const rows = Array.from(INTERNAL_TARGETS.values())
    .map(
      (target) =>
        `<tr>
          <td>${target.id}</td>
          <td>${target.name}</td>
          <td>${target.callbackUrl}</td>
          <td>${target.resultUrl}</td>
          <td class="actions-cell">
            <form method="post" action="/admin/internal-targets/delete" onsubmit="return confirm('${String(t(locale, 'internal_targets_confirm_delete')).replace(/'/g, "\\'")}');">
              <input type="hidden" name="id" value="${target.id}" />
              <button type="submit" style="padding:4px 8px;font-size:12px;background:#dc2626;color:#fff;border:none;border-radius:4px;cursor:pointer;">${t(locale, 'common_delete')}</button>
            </form>
          </td>
        </tr>`,
    )
    .join('');

  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'internal_targets_title')}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
    h1 { margin-bottom: 8px; }
    h2 { margin-top: 32px; }
    table { border-collapse: collapse; width: 100%; background:#fff; }
    th, td { border: 1px solid #e5e7eb; padding: 8px 10px; font-size: 14px; }
    th { background: #e5f0ff; text-align: center; }
    td { text-align: center; }
    tr:nth-child(even) { background:#f9fafb; }
    .actions-cell { text-align:center; }
    label { display:block; margin-top:8px; font-size: 14px; }
    input[type="text"] { width: 100%; padding: 8px 10px; margin-top:4px; box-sizing:border-box; border-radius:6px; border:1px solid #d1d5db; background:#f9fafb; }
    input[type="text"]:focus { outline:none; border-color:#60a5fa; box-shadow:0 0 0 1px #bfdbfe; background:#ffffff; }
    button { margin-top: 12px; padding: 9px 16px; background:#60a5fa; color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:14px; }
    button:hover { background:#3b82f6; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:8px; border:1px solid #e5e7eb; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card">
      <h1>${t(locale, 'internal_targets_title')}</h1>
      <p style="font-size:13px;color:#555;">${t(locale, 'internal_targets_desc')}</p>
      <h2>${t(locale, 'internal_targets_register')}</h2>
      <form method="post" action="/admin/internal-targets" onsubmit="return confirm('${(t(locale, 'merchants_confirm_save') || '저장(적용)하시겠습니까?').replace(/'/g, "\\'")}');">
        <label>${t(locale, 'internal_targets_id')}<input type="text" name="id" required /></label>
        <label>${t(locale, 'internal_targets_name')}<input type="text" name="name" required /></label>
        <label>${t(locale, 'internal_targets_callback_url')}<input type="text" name="callbackUrl" required /></label>
        <label>${t(locale, 'internal_targets_result_url')}<input type="text" name="resultUrl" /></label>
        <button type="submit">${t(locale, 'common_save')}</button>
      </form>
    </div>
    <div class="card">
      <h2>${t(locale, 'internal_targets_list')}</h2>
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>${t(locale, 'internal_targets_name')}</th>
            <th>${t(locale, 'internal_targets_callback_url')}</th>
            <th>${t(locale, 'internal_targets_result_url')}</th>
            <th class="actions-cell">${t(locale, 'internal_targets_manage')}</th>
          </tr>
        </thead>
        <tbody>
          ${rows || `<tr><td colspan="5" style="text-align:center;color:#777;">${t(locale, 'internal_targets_empty')}</td></tr>`}
        </tbody>
      </table>
      </div>
    </main>
  </div>
</body>
</html>`);
});

// ---------- 전산 노티 설정: 통화별 금액 가공, RouteNo, CustomerId, 오리지널 ----------
const CURRENCY_LABELS = { '392': 'JPY', '840': 'USD', '410': 'KRW', '764': 'THB' };
const AMOUNT_RULE_KEYS = [
  { value: 'X100', key: 'internal_noti_rule_x' },
  { value: '/100', key: 'internal_noti_rule_div' },
  { value: '=', key: 'internal_noti_rule_eq' },
];

app.get('/admin/internal-noti-settings', requireAuth, requirePage('internal_noti_settings'), (req, res) => {
  const locale = getLocale(req);
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });
  const full = loadInternalNotiSettingsFull();
  const amountRuleOptions = AMOUNT_RULE_KEYS.map((o) => ({ value: o.value, label: t(locale, o.key) }));

  const confirmMsg = (t(locale, 'internal_noti_confirm_save') || '저장하시겠습니까?').replace(/'/g, "\\'");
  const rows = CURRENCY_CODES
    .map((code) => {
      const currentRule = full.amountRules[code] || '=';
      const currentLabel = amountRuleOptions.find((o) => o.value === currentRule)?.label || t(locale, 'internal_noti_rule_eq');
      const routeNoVal = full.routeNoMode && full.routeNoMode[code] || 'current';
      const customerIdVal = full.customerIdMode && full.customerIdMode[code] || 'current';
      const originalChecked = full.original && full.original[code];
      return `
    <tr>
      <td>${CURRENCY_LABELS[code]} (${code})</td>
      <td>
        <form class="cell-form" method="post" action="/admin/internal-noti-settings" onsubmit="return confirm('${confirmMsg}');">
          <input type="hidden" name="currency" value="${code}" />
          <input type="hidden" name="routeNo" value="${routeNoVal}" />
          <input type="hidden" name="customerId" value="${customerIdVal}" />
          <input type="hidden" name="original" value="${originalChecked ? 'on' : ''}" />
          <select name="rule" class="cell-select">${amountRuleOptions.map((opt) => `<option value="${opt.value}" ${currentRule === opt.value ? 'selected' : ''}>${opt.label}</option>`).join('')}</select>
          <button type="submit" class="btn-save-row">저장</button>
        </form>
      </td>
      <td><span class="current-state">${currentLabel}</span></td>
      <td>
        <form class="cell-form" method="post" action="/admin/internal-noti-settings" onsubmit="return confirm('${confirmMsg}');">
          <input type="hidden" name="currency" value="${code}" />
          <input type="hidden" name="rule" value="${currentRule}" />
          <input type="hidden" name="customerId" value="${customerIdVal}" />
          <input type="hidden" name="original" value="${originalChecked ? 'on' : ''}" />
          <select name="routeNo" class="cell-select" title="현재 RouteNo(전산용) / 삭제">
            <option value="current" ${routeNoVal === 'current' ? 'selected' : ''}>현재</option>
            <option value="delete" ${routeNoVal === 'delete' ? 'selected' : ''}>삭제</option>
          </select>
          <button type="submit" class="btn-save-row">저장</button>
        </form>
      </td>
      <td>
        <form class="cell-form" method="post" action="/admin/internal-noti-settings" onsubmit="return confirm('${confirmMsg}');">
          <input type="hidden" name="currency" value="${code}" />
          <input type="hidden" name="rule" value="${currentRule}" />
          <input type="hidden" name="routeNo" value="${routeNoVal}" />
          <input type="hidden" name="original" value="${originalChecked ? 'on' : ''}" />
          <select name="customerId" class="cell-select" title="가맹점 전산용 CustomerId / 삭제">
            <option value="current" ${customerIdVal === 'current' ? 'selected' : ''}>가맹점값</option>
            <option value="delete" ${customerIdVal === 'delete' ? 'selected' : ''}>삭제</option>
          </select>
          <button type="submit" class="btn-save-row">저장</button>
        </form>
      </td>
      <td>
        <form class="cell-form cell-form-inline" method="post" action="/admin/internal-noti-settings" onsubmit="return confirm('${confirmMsg}');">
          <input type="hidden" name="currency" value="${code}" />
          <input type="hidden" name="rule" value="${currentRule}" />
          <input type="hidden" name="routeNo" value="${routeNoVal}" />
          <input type="hidden" name="customerId" value="${customerIdVal}" />
          <label class="cell-label"><input type="checkbox" name="original" ${originalChecked ? 'checked' : ''} value="on" /> 오리지널</label>
          <button type="submit" class="btn-save-row">저장</button>
        </form>
      </td>
    </tr>`;
    })
    .join('');

  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'internal_noti_title')}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
    h1 { margin-bottom: 8px; }
    h2 { margin-top: 24px; }
    table { border-collapse: collapse; width: 100%; table-layout: fixed; background:#fff; }
    th, td { border: 1px solid #e5e7eb; padding: 6px 8px; font-size: 13px; text-align: center; }
    th { background: #e5f0ff; color:#1f2937; }
    tr:nth-child(even) { background:#f9fafb; }
    th:nth-child(1), td:nth-child(1) { width: 72px; }
    th:nth-child(2), td:nth-child(2) { width: 150px; }
    th:nth-child(3), td:nth-child(3) { width: 90px; white-space: nowrap; }
    th:nth-child(4), td:nth-child(4) { width: 100px; }
    th:nth-child(5), td:nth-child(5) { width: 95px; }
    th:nth-child(6), td:nth-child(6) { width: 120px; }
    .cell-form { display: flex; align-items: center; justify-content: center; gap: 4px; margin: 0; }
    .cell-form-inline { flex-wrap: nowrap; }
    .cell-select { width: 100%; max-width: 82px; min-width: 52px; padding: 4px 6px; font-size: 12px; border-radius: 4px; border: 1px solid #d1d5db; box-sizing: border-box; }
    .cell-label { display: inline-flex; align-items: center; gap: 4px; margin: 0; white-space: nowrap; font-size: 12px; }
    .current-state { display: inline-block; padding: 4px 8px; border-radius: 4px; background:#dc2626; color:#fff; font-size: 12px; font-weight: 500; white-space: nowrap; }
    button { margin-top: 12px; padding: 9px 16px; background:#60a5fa; color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:14px; }
    button:hover { background:#3b82f6; }
    .btn-save-row { padding: 4px 8px; font-size: 11px; background:#2563eb; color:#fff; border:none; border-radius: 4px; cursor: pointer; white-space: nowrap; flex-shrink: 0; }
    .btn-save-row:hover { background:#1d4ed8; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
    .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; margin-bottom:16px; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:18px 22px; border-radius:10px; box-shadow:0 10px 25px rgba(15,23,42,0.06); margin-bottom:8px; border:1px solid #e5e7eb; }
    .hint { font-size:12px; color:#6b7280; margin-top:8px; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card">
        <h1>${t(locale, 'internal_noti_title')}</h1>
        <p style="font-size:13px;color:#555;">${t(locale, 'internal_noti_desc')}</p>
        <ul style="font-size:13px;color:#374151;margin:8px 0;">
          <li><strong>X 100</strong>: ${t(locale, 'internal_noti_rule_x')} · <strong>/ 100</strong>: ${t(locale, 'internal_noti_rule_div')} · <strong>=</strong>: ${t(locale, 'internal_noti_rule_eq')}</li>
          <li><strong>RouteNo 삭제</strong>: 해당 구문과 값을 전산 전송에서 제외. 기본은 현재 전산용 루트번호(가맹점 설정).</li>
          <li><strong>CustomerId 삭제</strong>: 해당 구문과 값을 전산 전송에서 제외. 기본은 가맹점 설정의 전산용 CustomerId.</li>
          <li><strong>오리지널</strong>: PG 노티를 가공 없이 그대로 전산으로 전달 (PG 노티 로그와 동일).</li>
        </ul>
        <table>
            <thead>
              <tr>
                <th>통화</th>
                <th>가공규칙</th>
                <th>현상태</th>
                <th>RouteNo</th>
                <th>CustomerId</th>
                <th>오리지널</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        <p class="hint">${t(locale, 'internal_noti_hint')}</p>
      </div>
    </main>
  </div>
</body>
</html>`);
});

// ---------- 개발 노티 설정 페이지 ----------
app.get('/admin/dev-internal-noti-settings', requireAuth, requirePage('dev_internal_noti_settings'), (req, res) => {
  const locale = getLocale(req);
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });
  const full = loadDevInternalNotiSettingsFull();
  const amountRuleOptions = AMOUNT_RULE_KEYS.map((o) => ({ value: o.value, label: t(locale, o.key) }));

  const confirmMsg = (t(locale, 'internal_noti_confirm_save') || '저장하시겠습니까?').replace(/'/g, "\\'");
  const rows = CURRENCY_CODES
    .map((code) => {
      const currentRule = full.amountRules[code] || '=';
      const currentLabel =
        amountRuleOptions.find((o) => o.value === currentRule)?.label || t(locale, 'internal_noti_rule_eq');
      const routeNoVal = (full.routeNoMode && full.routeNoMode[code]) || 'current';
      const customerIdVal = (full.customerIdMode && full.customerIdMode[code]) || 'current';
      const originalChecked = full.original && full.original[code];
      return `
    <tr>
      <td>${CURRENCY_LABELS[code]} (${code})</td>
      <td>
        <form class="cell-form" method="post" action="/admin/dev-internal-noti-settings" onsubmit="return confirm('${confirmMsg}');">
          <input type="hidden" name="currency" value="${code}" />
          <input type="hidden" name="routeNo" value="${routeNoVal}" />
          <input type="hidden" name="customerId" value="${customerIdVal}" />
          <input type="hidden" name="original" value="${originalChecked ? 'on' : ''}" />
          <select name="rule" class="cell-select">${amountRuleOptions
            .map(
              (opt) =>
                `<option value="${opt.value}" ${currentRule === opt.value ? 'selected' : ''}>${opt.label}</option>`,
            )
            .join('')}</select>
          <button type="submit" class="btn-save-row">저장</button>
        </form>
      </td>
      <td><span class="current-state">${currentLabel}</span></td>
      <td>
        <form class="cell-form" method="post" action="/admin/dev-internal-noti-settings" onsubmit="return confirm('${confirmMsg}');">
          <input type="hidden" name="currency" value="${code}" />
          <input type="hidden" name="rule" value="${currentRule}" />
          <input type="hidden" name="customerId" value="${customerIdVal}" />
          <input type="hidden" name="original" value="${originalChecked ? 'on' : ''}" />
          <select name="routeNo" class="cell-select" title="현재 RouteNo(전산용) / 삭제">
            <option value="current" ${routeNoVal === 'current' ? 'selected' : ''}>현재</option>
            <option value="delete" ${routeNoVal === 'delete' ? 'selected' : ''}>삭제</option>
          </select>
          <button type="submit" class="btn-save-row">저장</button>
        </form>
      </td>
      <td>
        <form class="cell-form" method="post" action="/admin/dev-internal-noti-settings" onsubmit="return confirm('${confirmMsg}');">
          <input type="hidden" name="currency" value="${code}" />
          <input type="hidden" name="rule" value="${currentRule}" />
          <input type="hidden" name="routeNo" value="${routeNoVal}" />
          <input type="hidden" name="original" value="${originalChecked ? 'on' : ''}" />
          <select name="customerId" class="cell-select" title="가맹점 전산용 CustomerId / 삭제">
            <option value="current" ${customerIdVal === 'current' ? 'selected' : ''}>가맹점값</option>
            <option value="delete" ${customerIdVal === 'delete' ? 'selected' : ''}>삭제</option>
          </select>
          <button type="submit" class="btn-save-row">저장</button>
        </form>
      </td>
      <td>
        <form class="cell-form cell-form-inline" method="post" action="/admin/dev-internal-noti-settings" onsubmit="return confirm('${confirmMsg}');">
          <input type="hidden" name="currency" value="${code}" />
          <input type="hidden" name="rule" value="${currentRule}" />
          <input type="hidden" name="routeNo" value="${routeNoVal}" />
          <input type="hidden" name="customerId" value="${customerIdVal}" />
          <label class="cell-label"><input type="checkbox" name="original" ${originalChecked ? 'checked' : ''} value="on" /> 오리지널</label>
          <button type="submit" class="btn-save-row">저장</button>
        </form>
      </td>
    </tr>`;
    })
    .join('');

  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'nav_dev_internal_noti_settings') || '개발 환경설정'}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
    h1 { margin-bottom: 8px; }
    h2 { margin-top: 24px; }
    table { border-collapse: collapse; width: 100%; table-layout: fixed; background:#fff; }
    th, td { border: 1px solid #e5e7eb; padding: 6px 8px; font-size: 13px; text-align: center; }
    th { background: #e5f0ff; color:#1f2937; }
    tr:nth-child(even) { background:#f9fafb; }
    th:nth-child(1), td:nth-child(1) { width: 72px; }
    th:nth-child(2), td:nth-child(2) { width: 150px; }
    th:nth-child(3), td:nth-child(3) { width: 90px; white-space: nowrap; }
    th:nth-child(4), td:nth-child(4) { width: 100px; }
    th:nth-child(5), td:nth-child(5) { width: 95px; }
    th:nth-child(6), td:nth-child(6) { width: 120px; }
    .cell-form { display: flex; align-items: center; justify-content: center; gap: 4px; margin: 0; }
    .cell-form-inline { flex-wrap: nowrap; }
    .cell-select { width: 100%; max-width: 82px; min-width: 52px; padding: 4px 6px; font-size: 12px; border-radius: 4px; border: 1px solid #d1d5db; box-sizing: border-box; }
    .cell-label { display: inline-flex; align-items: center; gap: 4px; margin: 0; white-space: nowrap; font-size: 12px; }
    .current-state { display: inline-block; padding: 4px 8px; border-radius: 4px; background:#dc2626; color:#fff; font-size: 12px; font-weight: 500; white-space: nowrap; }
    button { margin-top: 12px; padding: 9px 16px; background:#60a5fa; color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:14px; }
    button:hover { background:#3b82f6; }
    .btn-save-row { padding: 4px 8px; font-size: 11px; background:#2563eb; color:#fff; border:none; border-radius: 4px; cursor: pointer; white-space: nowrap; flex-shrink: 0; }
    .btn-save-row:hover { background:#1d4ed8; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
    .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; margin-bottom:16px; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:18px 22px; border-radius:10px; box-shadow:0 10px 25px rgba(15,23,42,0.06); margin-bottom:8px; border:1px solid #e5e7eb; }
    .hint { font-size:12px; color:#6b7280; margin-top:8px; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card">
        <h1>${t(locale, 'nav_dev_internal_noti_settings') || '개발 환경설정'}</h1>
        <p style="font-size:13px;color:#555;">${t(locale, 'internal_noti_desc')}</p>
        <ul style="font-size:13px;color:#374151;margin:8px 0;">
          <li><strong>X 100</strong>: ${t(locale, 'internal_noti_rule_x')} · <strong>/ 100</strong>: ${t(locale, 'internal_noti_rule_div')} · <strong>=</strong>: ${t(locale, 'internal_noti_rule_eq')}</li>
          <li><strong>RouteNo 삭제</strong>: 해당 구문과 값을 전산 전송에서 제외. 기본은 현재 전산용 루트번호(가맹점 설정).</li>
          <li><strong>CustomerId 삭제</strong>: 해당 구문과 값을 전산 전송에서 제외. 기본은 가맹점 설정의 전산용 CustomerId.</li>
          <li><strong>오리지널</strong>: PG 노티를 가공 없이 그대로 개발 전산으로 전달 (PG 노티 로그와 동일).</li>
        </ul>
        <table>
            <thead>
              <tr>
                <th>통화</th>
                <th>가공규칙</th>
                <th>현상태</th>
                <th>RouteNo</th>
                <th>CustomerId</th>
                <th>오리지널</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        <p class="hint">${t(locale, 'internal_noti_hint')}</p>
      </div>
    </main>
  </div>
</body>
</html>`);
});

app.post('/admin/dev-internal-noti-settings', requireAuth, requirePage('dev_internal_noti_settings'), (req, res) => {
  const currency = (req.body.currency || '').trim();

  if (currency && CURRENCY_CODES.includes(currency)) {
    const full = loadDevInternalNotiSettingsFull();
    let r = (req.body.rule || '=').trim() || '=';
    if (r !== 'X100' && r !== '/100') r = '=';
    full.amountRules[currency] = r;
    const rn = (req.body.routeNo || 'current').trim();
    full.routeNoMode[currency] = rn === 'delete' ? 'delete' : 'current';
    const cid = (req.body.customerId || 'current').trim();
    full.customerIdMode[currency] = cid === 'delete' ? 'delete' : 'current';
    full.original[currency] = req.body.original === 'on';
    saveDevInternalNotiSettings(full);
    if (typeof appendConfigChangeLog === 'function') {
      appendConfigChangeLog({
        action: 'dev_internal_noti_settings',
        detail: `개발 노티 설정 변경 (통화 ${currency} 개별 저장)`,
        actor: req.session.adminUser || 'unknown',
        clientIp:
          (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
          req.ip ||
          '',
        payload: { currency, rule: req.body.rule, routeNo: req.body.routeNo, customerId: req.body.customerId, original: req.body.original },
      });
    }
  } else {
    const amountRules = {};
    const routeNoMode = {};
    const customerIdMode = {};
    const original = {};
    CURRENCY_CODES.forEach((code) => {
      let r = (req.body['rule_' + code] || '=').trim() || '=';
      if (r !== 'X100' && r !== '/100') r = '=';
      amountRules[code] = r;
      const rn = (req.body['routeNo_' + code] || 'current').trim();
      routeNoMode[code] = rn === 'delete' ? 'delete' : 'current';
      const cid = (req.body['customerId_' + code] || 'current').trim();
      customerIdMode[code] = cid === 'delete' ? 'delete' : 'current';
      original[code] = req.body['original_' + code] === 'on';
    });
    saveDevInternalNotiSettings({
      amountRules,
      routeNoMode,
      customerIdMode,
      original,
    });
    if (typeof appendConfigChangeLog === 'function') {
      appendConfigChangeLog({
        action: 'dev_internal_noti_settings',
        detail: '개발 노티 설정 변경 (금액/RouteNo/CustomerId/오리지널)',
        actor: req.session.adminUser || 'unknown',
        clientIp:
          (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
          req.ip ||
          '',
        payload: { amountRules, routeNoMode, customerIdMode, original },
      });
    }
  }
  return res.redirect('/admin/dev-internal-noti-settings');
});
app.post('/admin/internal-noti-settings', requireAuth, requirePage('internal_noti_settings'), (req, res) => {
  const currency = (req.body.currency || '').trim();

  if (currency && CURRENCY_CODES.includes(currency)) {
    const full = loadInternalNotiSettingsFull();
    let r = (req.body.rule || '=').trim() || '=';
    if (r !== 'X100' && r !== '/100') r = '=';
    full.amountRules[currency] = r;
    const rn = (req.body.routeNo || 'current').trim();
    full.routeNoMode[currency] = rn === 'delete' ? 'delete' : 'current';
    const cid = (req.body.customerId || 'current').trim();
    full.customerIdMode[currency] = cid === 'delete' ? 'delete' : 'current';
    full.original[currency] = req.body.original === 'on';
    saveInternalNotiSettings(full);
    if (typeof appendConfigChangeLog === 'function') {
      appendConfigChangeLog({
        action: 'internal_noti_settings',
        detail: `전산 노티 설정 변경 (통화 ${currency} 개별 저장)`,
        actor: req.session.adminUser || 'unknown',
        clientIp: (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() || req.ip || '',
        payload: { currency, rule: req.body.rule, routeNo: req.body.routeNo, customerId: req.body.customerId, original: req.body.original },
      });
    }
  } else {
    const amountRules = {};
    const routeNoMode = {};
    const customerIdMode = {};
    const original = {};
    CURRENCY_CODES.forEach((code) => {
      let r = (req.body['rule_' + code] || '=').trim() || '=';
      if (r !== 'X100' && r !== '/100') r = '=';
      amountRules[code] = r;
      const rn = (req.body['routeNo_' + code] || 'current').trim();
      routeNoMode[code] = rn === 'delete' ? 'delete' : 'current';
      const cid = (req.body['customerId_' + code] || 'current').trim();
      customerIdMode[code] = cid === 'delete' ? 'delete' : 'current';
      original[code] = req.body['original_' + code] === 'on';
    });
    saveInternalNotiSettings({
      amountRules,
      routeNoMode,
      customerIdMode,
      original,
    });
    if (typeof appendConfigChangeLog === 'function') {
      appendConfigChangeLog({
        action: 'internal_noti_settings',
        detail: '전산 노티 설정 변경 (금액/RouteNo/CustomerId/오리지널)',
        actor: req.session.adminUser || 'unknown',
        clientIp: (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() || req.ip || '',
        payload: { amountRules, routeNoMode, customerIdMode, original },
      });
    }
  }
  return res.redirect('/admin/internal-noti-settings');
});

app.post('/admin/internal-targets', requireAuth, requirePage('internal_targets'), (req, res) => {
  const { id, name, callbackUrl, resultUrl } = req.body;
  if (!id || !name || !callbackUrl) {
    return res
      .status(400)
      .send('id, name, callbackUrl 은 필수입니다.');
  }
  const actor = req.session.adminUser || 'unknown';
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const before = INTERNAL_TARGETS.get(id) || null;

  INTERNAL_TARGETS.set(id, {
    id,
    name,
    callbackUrl,
    resultUrl: resultUrl || '',
  });
  saveInternalTargets();
  console.log('[관리자] 전산 대상 저장:', id, INTERNAL_TARGETS.get(id));
  appendConfigChangeLog({
    type: 'internal_target_update',
    actor,
    clientIp,
    targetId: id,
    before,
    after: INTERNAL_TARGETS.get(id),
  });
  return res.redirect('/admin/internal-targets');
});

// 테스트 결제 환경 설정 페이지
app.get('/admin/test-configs', requireAuth, requirePage('test_config'), (req, res) => {
  const locale = getLocale(req);
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });

  const editId = (req.query.id || '').toString();
  const editingConfig = editId && TEST_CONFIGS.get(editId) ? TEST_CONFIGS.get(editId) : null;

  // 가맹점 추가에 이미 등록된 Route No 목록 (경고 표시용)
  const usedRouteNos = [
    ...new Set(
      [...MERCHANTS.values()]
        .filter((m) => m && (m.routeNo != null && m.routeNo !== ''))
        .map((m) => String(m.routeNo).trim()),
    ),
  ];
  const routeNoInUse =
    editingConfig && usedRouteNos.includes(String(editingConfig.routeNo || '').trim());

  // 설정 ID 중복 검사용: 이미 등록된 ID 목록 (수정 시에는 현재 id 제외)
  const existingConfigIds = editingConfig
    ? [...TEST_CONFIGS.keys()].filter((k) => k !== editingConfig.id)
    : [...TEST_CONFIGS.keys()];
  const idDuplicate = false;

  const rows = Array.from(TEST_CONFIGS.values())
    .map(
      (cfg) =>
        `<tr>
          <td>
            <form method="get" action="/admin/test-pay" style="margin:0;">
              <input type="hidden" name="configId" value="${cfg.id}" />
              <button type="submit" style="padding:4px 8px;font-size:12px;background:#6b7280;color:#fff;border:none;border-radius:4px;cursor:pointer;">${t(locale, 'test_run_title')}</button>
            </form>
          </td>
          <td>${cfg.id}</td>
          <td>${cfg.name}</td>
          <td>${cfg.environment}</td>
          <td>${cfg.merchantCode}</td>
          <td>${cfg.routeNo}</td>
          <td class="td-long">${cfg.apiKey}</td>
          <td class="td-long">${t.md5Key}</td>
          <td>${
            cfg.currency === '392'
              ? 'JPY (392)'
              : cfg.currency === '840'
              ? 'USD (840)'
              : cfg.currency === '764'
              ? 'THB (764)'
              : cfg.currency === '410'
              ? 'KOR (410)'
              : cfg.currency
          }</td>
          <td class="td-long">${cfg.paymentApiUrl || ''}</td>
          <td class="td-long">${cfg.useTestResultPage ? '[테스트 결과 페이지 사용]' : (cfg.returnUrl || '')}</td>
          <td class="actions-cell">
            <div style="display:flex;flex-direction:column;gap:4px;align-items:center;">
              <form method="get" action="/admin/test-configs" style="margin:0;" onsubmit="return confirm('수정 하시겠습니까?');">
                <input type="hidden" name="id" value="${cfg.id}" />
                <button type="submit" style="padding:4px 8px;font-size:12px;background:#facc15;color:#111827;border:none;border-radius:4px;cursor:pointer;">${t(locale, 'common_edit')}</button>
              </form>
              <form method="post" action="/admin/test-configs/delete" onsubmit="return confirm('${String(t(locale, 'test_config_confirm_delete')).replace(/'/g, "\\'")}');" style="margin:0;">
                <input type="hidden" name="id" value="${cfg.id}" />
                <button type="submit" style="padding:4px 8px;font-size:12px;background:#dc2626;color:#fff;border:none;border-radius:4px;cursor:pointer;">${t(locale, 'common_delete')}</button>
              </form>
            </div>
          </td>
        </tr>`,
    )
    .join('');

  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'test_config_title')}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
    h1 { margin-bottom: 8px; }
    h2 { margin-top: 32px; }
    table { border-collapse: collapse; width: 100%; background:#fff; border-radius:8px; overflow:hidden; table-layout:fixed; }
    th, td { border: 1px solid #e5e7eb; padding: 8px 10px; font-size: 14px; vertical-align:top; }
    th { background: #e5f0ff; text-align: center; }
    td { text-align: center; }
    tr:nth-child(even) { background:#f9fafb; }
    .td-long { word-break:break-all; white-space:normal; font-size:12px; line-height:1.4; }
    .actions-cell { text-align:center; }
    label { display:block; margin-top:8px; font-size: 14px; }
    input[type="text"], select { width: 100%; padding: 8px 10px; margin-top:4px; box-sizing:border-box; border-radius:6px; border:1px solid #d1d5db; background:#f9fafb; }
    input[type="text"]:focus, select:focus { outline:none; border-color:#60a5fa; box-shadow:0 0 0 1px #bfdbfe; background:#ffffff; }
    button { margin-top: 12px; padding: 9px 16px; background:#60a5fa; color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:14px; }
    button:hover { background:#3b82f6; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:8px; border:1px solid #e5e7eb; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card">
        <h1>${t(locale, 'test_config_title')}</h1>
        <p style="font-size:13px;color:#555;">${t(locale, 'test_config_desc')}</p>
        <h2>등록/수정</h2>
        <form method="post" action="/admin/test-configs" id="test-config-form" onsubmit="return (function(){ var dup=document.getElementById('test-config-id-dup'); if(dup&&dup.style.display!=='none'){ alert('동일한 이름이 있습니다. 설정 ID를 다른 값으로 변경해 주세요.'); return false; } return confirm('${(t(locale, 'merchants_confirm_save') || '저장(적용)하시겠습니까?').replace(/'/g, "\\'")}'); })();">
          ${editingConfig ? `<input type="hidden" name="originalId" value="${editingConfig.id}" />` : ''}
          <label>
            설정 ID (영문/숫자)
            <input type="text" name="id" id="test-config-id" value="${editingConfig ? editingConfig.id : ''}" required />
            <div id="test-config-id-dup" style="color:#dc2626;font-size:13px;margin-top:6px;font-weight:500;display:${idDuplicate ? 'block' : 'none'};">
              동일한 이름이 있습니다. 다른 ID를 입력해 주세요.
            </div>
          </label>
          <label>
            설정 이름 (예: Sandbox JPY, Prod USD)
            <input type="text" name="name" value="${editingConfig ? editingConfig.name : ''}" required />
          </label>
          <label>
            환경 (Environment)
            <select name="environment" required>
              <option value="SANDBOX" ${!editingConfig || editingConfig.environment === 'SANDBOX' ? 'selected' : ''}>Sandbox</option>
              <option value="PRODUCTION" ${editingConfig && editingConfig.environment === 'PRODUCTION' ? 'selected' : ''}>Production</option>
            </select>
          </label>
          <label>
            Merchant Code
            <input type="text" name="merchantCode" value="${editingConfig ? editingConfig.merchantCode : ''}" required />
          </label>
          <label>
            Route No
            <input type="text" name="routeNo" id="test-config-routeNo" value="${editingConfig ? editingConfig.routeNo : ''}" required />
            <div id="route-no-warning" style="color:#dc2626;font-size:13px;margin-top:6px;font-weight:500;display:${routeNoInUse ? 'block' : 'none'};">
              ⚠ 이미 가맹점 추가에 등록되어 사용 중인 번호입니다. 해당 번호로 테스트 시 정식 가맹점 노티와 혼동될 수 있으니 주의하세요. (테스트는 가능합니다.)
            </div>
          </label>
          <script>
          (function(){
            var usedRouteNos = ${JSON.stringify(usedRouteNos)};
            var input = document.getElementById('test-config-routeNo');
            var warning = document.getElementById('route-no-warning');
            if (!input || !warning) return;
            function check() {
              var val = (input.value || '').trim();
              warning.style.display = usedRouteNos.indexOf(val) !== -1 ? 'block' : 'none';
            }
            input.addEventListener('input', check);
            input.addEventListener('change', check);
          })();
          <\/script>
          <script>
          (function(){
            var existingIds = ${JSON.stringify(existingConfigIds)};
            var idInput = document.getElementById('test-config-id');
            var dupMsg = document.getElementById('test-config-id-dup');
            if (!idInput || !dupMsg) return;
            function checkId() {
              var val = (idInput.value || '').trim();
              dupMsg.style.display = existingIds.indexOf(val) !== -1 ? 'block' : 'none';
            }
            idInput.addEventListener('input', checkId);
            idInput.addEventListener('change', checkId);
          })();
          <\/script>
          <label>
            API KEY
            <input type="text" name="apiKey" value="${editingConfig ? editingConfig.apiKey : ''}" required />
          </label>
          <label>
            MD5 Key
            <input type="text" name="md5Key" value="${editingConfig ? editingConfig.md5Key : ''}" required />
          </label>
          <label>
            Currency
            <select name="currency" required>
              <option value="392" ${!editingConfig || editingConfig.currency === 'JPY' || editingConfig.currency === '392' ? 'selected' : ''}>JPY (392)</option>
              <option value="840" ${editingConfig && (editingConfig.currency === 'USD' || editingConfig.currency === '840') ? 'selected' : ''}>USD (840)</option>
              <option value="764" ${editingConfig && (editingConfig.currency === 'THB' || editingConfig.currency === '764') ? 'selected' : ''}>THB (764)</option>
              <option value="410" ${editingConfig && (editingConfig.currency === 'KRW' || editingConfig.currency === 'KOR' || editingConfig.currency === '410') ? 'selected' : ''}>KOR (410)</option>
            </select>
          </label>
          <label>
            결제 API URL (이 설정에서 사용할 Payment API 엔드포인트)
            <input type="text" name="paymentApiUrl" value="${editingConfig ? (editingConfig.paymentApiUrl || '') : ''}" placeholder="예: https://sandbox-bankdemo3.chillpay.co/ChillCredit/..." />
          </label>
          <label>
            ReturnUrl (PG 결제 완료 후 돌아올 URL)
            <input type="text" name="returnUrl" value="${editingConfig ? (editingConfig.returnUrl || '') : ''}" placeholder="예: https://tapi.soonpay.co.kr/pay/chillResult" />
          </label>
          <label style="margin-top:12px; display:flex; align-items:center; gap:8px;">
            <input type="checkbox" name="useTestResultPage" ${editingConfig && editingConfig.useTestResultPage ? 'checked' : ''} />
            테스트결과페이지 사용하기
          </label>
          <p style="font-size:12px;color:#6b7280;margin-top:4px;">체크 시: PG 결제 완료 후 우리 서버의 테스트 결과 페이지로 이동합니다. 체크 해제 시: 위 주소 입력창에 입력한 URL로 이동합니다.</p>
          ${editingConfig && editingConfig.useTestResultPage ? `<p style="font-size:12px;color:#0369a1;margin-top:4px;">ChillPay <strong>URL Result(노티 수신)</strong> 등록: <code style="background:#e0f2fe;padding:2px 6px;border-radius:4px;">${req.protocol}://${req.get('host') || req.hostname}/noti/result/test_${editingConfig.id}</code></p><p style="font-size:12px;color:#0369a1;margin-top:2px;">(고객 리다이렉트용) <strong>Return URL</strong> 등록: <code style="background:#e0f2fe;padding:2px 6px;border-radius:4px;">${req.protocol}://${req.get('host') || req.hostname}/admin/test-pay/return</code></p>` : ''}
          <button type="submit">저장</button>
        </form>
      </div>
      <div class="card">
        <h2>등록된 테스트 환경</h2>
        <table>
          <thead>
            <tr>
              <th>결제</th>
              <th>ID</th>
              <th>이름</th>
              <th>환경</th>
              <th>Merchant Code</th>
              <th>Route No</th>
              <th>API KEY</th>
              <th>MD5 Key</th>
              <th>Currency</th>
              <th>Payment API URL</th>
              <th>ReturnUrl</th>
              <th class="actions-cell">관리</th>
            </tr>
          </thead>
          <tbody>
            ${
              rows ||
              '<tr><td colspan="11" style="text-align:center;color:#777;">등록된 테스트 환경이 없습니다.</td></tr>'
            }
          </tbody>
        </table>
      </div>
    </main>
  </div>
</body>
</html>`);
});

app.post('/admin/test-configs', requireAuth, requirePage('test_config'), (req, res) => {
  const {
    id,
    originalId,
    name,
    environment,
    merchantCode,
    routeNo,
    apiKey,
    md5Key,
    currency,
    paymentApiUrl,
    returnUrl,
    useTestResultPage,
  } = req.body;
  if (!id || !name || !environment || !merchantCode || !routeNo || !apiKey || !md5Key || !currency) {
    return res.status(400).send('모든 필드는 필수입니다.');
  }

  const idTrim = String(id).trim();
  const originalIdTrim = originalId ? String(originalId).trim() : '';

  if (originalIdTrim && idTrim !== originalIdTrim) {
    if (TEST_CONFIGS.has(idTrim)) {
      return res.status(400).send('동일한 이름이 있습니다. 설정 ID를 다른 값으로 변경해 주세요.');
    }
    TEST_CONFIGS.delete(originalIdTrim);
  } else if (!originalIdTrim && TEST_CONFIGS.has(idTrim)) {
    return res.status(400).send('동일한 이름이 있습니다. 설정 ID를 다른 값으로 변경해 주세요.');
  }

  const actor = req.session.adminUser || 'unknown';
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const before = TEST_CONFIGS.get(idTrim) || null;

  TEST_CONFIGS.set(idTrim, {
    id: idTrim,
    name,
    environment,
    merchantCode,
    routeNo,
    apiKey,
    md5Key,
    currency,
    paymentApiUrl: paymentApiUrl || '',
    returnUrl: returnUrl || '',
    useTestResultPage: useTestResultPage === 'on',
  });
  saveTestConfigs();

  appendConfigChangeLog({
    type: 'test_config_update',
    actor,
    clientIp,
    testConfigId: idTrim,
    before,
    after: TEST_CONFIGS.get(idTrim),
  });

  return res.redirect('/admin/test-configs');
});

app.post('/admin/test-configs/delete', requireAuth, requirePage('test_config'), (req, res) => {
  const { id } = req.body;
  if (!id) {
    return res.status(400).send('id 는 필수입니다.');
  }

  const actor = req.session.adminUser || 'unknown';
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const before = TEST_CONFIGS.get(id) || null;

  if (!before) {
    return res.redirect('/admin/test-configs');
  }

  TEST_CONFIGS.delete(id);
  saveTestConfigs();

  appendConfigChangeLog({
    type: 'test_config_delete',
    actor,
    clientIp,
    testConfigId: id,
    before,
  });

  return res.redirect('/admin/test-configs');
});

// 테스트 결제 실행 페이지 (환경 선택 + 주문 정보 입력)
app.get('/admin/test-pay', requireAuth, requirePage('test_run'), (req, res) => {
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });

  const locale = getLocale(req);
  const selectedId = (req.query.configId || '').toString();

  const options = Array.from(TEST_CONFIGS.values())
    .map(
      (cfg) =>
        `<option value="${cfg.id}" ${selectedId === cfg.id ? 'selected' : ''}>${cfg.id} - ${cfg.name} (${cfg.environment}/${cfg.currency})</option>`,
    )
    .join('');

  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'test_run_title')}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
    h1 { margin-bottom: 8px; }
    h2 { margin-top: 32px; }
    label { display:block; margin-top:8px; font-size: 14px; }
    input[type="text"], input[type="number"], select { width: 100%; padding: 8px 10px; margin-top:4px; box-sizing:border-box; border-radius:6px; border:1px solid #d1d5db; background:#f9fafb; }
    input[type="text"]:focus, input[type="number"]:focus, select:focus { outline:none; border-color:#60a5fa; box-shadow:0 0 0 1px #bfdbfe; background:#ffffff; }
    button { margin-top: 12px; padding: 9px 16px; background:#60a5fa; color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:14px; }
    button:hover { background:#3b82f6; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:8px; border:1px solid #e5e7eb; }
    .row { display:flex; gap:16px; }
    .row > div { flex:1; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card">
        <h1>${t(locale, 'test_run_title')}</h1>
        <p style="font-size:13px;color:#555;">${t(locale, 'test_config_desc')}</p>
        <form method="post" action="/admin/test-pay/start">
          <label>
            테스트 환경 선택
            <select name="configId" required>
              <option value="">-- 테스트 환경 선택 --</option>
              ${options}
            </select>
          </label>
          <div class="row">
            <div>
              <label>
                Order No
                <input type="text" name="orderNo" value="TEST-${Date.now()}" required />
              </label>
            </div>
            <div>
              <label>
                Customer Id
                <input type="text" name="customerId" value="CUST-001" required />
              </label>
            </div>
          </div>
          <label>
            Amount
            <input type="number" name="amount" step="0.01" value="1500.00" required />
          </label>
          <button type="submit">테스트 결제 페이지로 이동</button>
        </form>
      </div>
    </main>
  </div>
</body>
</html>`);
});

// 테스트 결제용 카드 입력 페이지 (Inline 스크립트)
app.post('/admin/test-pay/start', requireAuth, requirePage('test_run'), (req, res) => {
  const { configId, orderNo, customerId, amount } = req.body;
  const cfg = TEST_CONFIGS.get(configId);
  if (!cfg) {
    return res.status(400).send('선택한 테스트 환경을 찾을 수 없습니다.');
  }

  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const now = new Date().toLocaleString('ko-KR', { hour12: false });

  const scriptSrc =
    cfg.environment === 'SANDBOX'
      ? 'https://sandbox-bankdemo3.chillpay.co/js/ccdpayment.js'
      : 'https://cdn.chill.credit/js/ccdpayment.js';

  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Test Payment - ${cfg.name}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin:0; background:#111827; color:#f9fafb; }
    .layout { display:flex; min-height:100vh; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; padding:24px; background:#edf2f7; box-sizing:border-box; display:flex; justify-content:center; align-items:flex-start; }
    .card { width:100%; max-width:720px; background:#ffffff; border-radius:12px; padding:20px 24px; box-shadow:0 20px 40px rgba(15,23,42,0.15); border:1px solid #e5e7eb; color:#111827; }
    h1 { margin:0 0 8px; font-size:20px; }
    p { font-size:13px; color:#374151; margin:4px 0 12px; }
    label { display:block; margin-top:10px; font-size:13px; color:#111827; }
    input[type="text"], input[type="number"] { width: 100%; padding: 8px 10px; margin-top:4px; box-sizing:border-box; border-radius:6px; border:1px solid #d1d5db; background:#ffffff; color:#111827; }
    input[type="text"]:focus, input[type="number"]:focus { outline:none; border-color:#60a5fa; box-shadow:0 0 0 1px #bfdbfe; background:#ffffff; }
    button { margin-top: 18px; padding: 10px 18px; background:#2563eb; color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:14px; }
    button:hover { background:#1d4ed8; }
    .field-row { display:flex; gap:12px; margin-top:8px; }
    .field-row > div { flex:1; }
    .summary { font-size:12px; color:#374151; margin-top:6px; }
  </style>
</head>
<body>
  <div class="layout">
    <aside class="sidebar">
      <div class="sidebar-title">PG 노티 관리자</div>
      <div class="sidebar-sub">Test Payment Inline</div>
      <div class="sidebar-user">사용자: ${adminUser || '-'}</div>
      <div class="nav">
        <div class="nav-section-title">테스트</div>
        <a href="/admin/test-configs">테스트 설정</a>
        <a href="/admin/test-pay">테스트 실행</a>
        <a href="/admin/test-logs">테스트 내역</a>
        <div class="nav-section-title">로그</div>
        <a href="/admin/logs">PG 노티 로그</a>
        <a href="/admin/internal">전산 노티 로그</a>
      </div>
      <div style="margin-top:16px;font-size:11px;color:#64748b;">
        <div>환경: ${cfg.environment}</div>
        <div>Currency: ${cfg.currency}</div>
        <div>Merchant: ${cfg.merchantCode}</div>
        <div>RouteNo: ${cfg.routeNo}</div>
      </div>
    </aside>
    <main class="main">
      <div class="card">
        <h1>테스트 결제 - ${cfg.name}</h1>
        <p>아래 카드 정보를 입력하고 Submit 을 누르면 결제 테스트용 API가 호출됩니다.</p>
        <div class="summary">
          OrderNo: ${orderNo} / CustomerId: ${customerId} / Amount: ${amount} / Currency: ${
            cfg.currency === '392'
              ? 'JPY (392)'
              : cfg.currency === '840'
              ? 'USD (840)'
              : cfg.currency === '764'
              ? 'THB (764)'
              : cfg.currency === '410'
              ? 'KOR (410)'
              : cfg.currency
          }
        </div>
        <div id="CreditCardData">
          <br />
          <div style="font-size:18px;margin-bottom:8px;">Card Information</div>
          <div>
            <label>Cardholder Name</label>
            <div id="ccdinline-card-name"></div>
            <label>Card Number</label>
            <div id="ccdinline-card-number"></div>
            <label>Expiry Date</label>
            <div id="ccdinline-card-expiry"></div>
            <label>Security Code</label>
            <div id="ccdinline-card-cvv"></div>
            <label>Remember Card</label>
            <div id="ccdinline-card-remember"></div>
          </div>
          <br />
          <form id="payment-form" method="POST" action="/admin/test-pay/submit">
            <script
              src="${scriptSrc}"
              data-merchant-code="${cfg.merchantCode}"
              data-api-key="${cfg.apiKey}">
            </script>
            <input type="hidden" name="configId" value="${configId}" />
            <input type="hidden" name="RouteNo" value="${cfg.routeNo}" />
            <input type="hidden" name="Currency" value="${cfg.currency}" />
            <input type="hidden" name="Env" value="${cfg.environment}" />
            <label>Order No
              <input id="OrderNo" name="OrderNo" type="text" value="${orderNo}" maxlength="20" />
            </label>
            <label>Customer Id
              <input id="CustomerId" name="CustomerId" type="text" value="${customerId}" maxlength="100" />
            </label>
            <label>Amount
              <input id="Amount" name="Amount" type="number" step="0.01" value="${amount}" />
            </label>
            <button type="submit">Submit (토큰 생성 및 서버 전송)</button>
          </form>
        </div>
      </div>
    </main>
  </div>
</body>
</html>`);
});

// 테스트 결제 토큰 수신 및 ChillCredit 결제 API 호출
app.post('/admin/test-pay/submit', requireAuth, requirePage('test_run'), async (req, res) => {
  const data = req.body || {};
  const locale = getLocale(req);
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });

  const {
    configId,
    OrderNo,
    CustomerId,
    Amount,
    RouteNo,
    Currency,
    Env,
  } = data;

  const cfg = configId ? TEST_CONFIGS.get(configId) : null;

  const paymentToken =
    data.PaymentCreditToken ||
    data.paymentCreditToken ||
    data.paymentCredittoken ||
    '';

  let paymentApiUrl = cfg && cfg.paymentApiUrl ? cfg.paymentApiUrl : '';

  let paymentRequest = null;
  let paymentResponse = null;
  let paymentError = null;
  let paymentSummaryHtml = '';

  if (!cfg) {
    paymentError = '선택한 테스트 환경(configId)을 찾을 수 없습니다.';
  } else if (!paymentToken) {
    paymentError =
      'PaymentCreditToken 값이 폼 데이터에 포함되지 않았습니다. Inline 스크립트 설정과 MerchantCode/API Key 를 확인하세요.';
  } else if (!paymentApiUrl) {
    paymentError = '결제 API URL이 테스트 결제 설정에 입력되지 않았습니다.';
  } else {
    try {
      const merchantCode = (cfg.merchantCode || '').trim();
      const routeNo = (RouteNo || cfg.routeNo || '').toString().trim();
      const orderNo = OrderNo || '';
      const customerId = CustomerId || '';
      const currency = normalizeCurrencyCode((Currency || cfg.currency || '').toString().trim()); // 문자열 코드 유지 (예: "392")
      const md5Key = (cfg.md5Key || '').trim();
      const channelCode = 'creditcard';
      const ipAddress =
        (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
        req.ip ||
        '';
      const apiKey = (cfg.apiKey || '').trim();
      const description = data.Description || 'TEST';
      const langCode = 'EN';
      const tokenType = 'DT';

      // DirectCredit 매뉴얼 기준 부가 필드
      const phoneNumber = data.PhoneNumber || '0911111111';
      const creditToken = data.CreditToken || '';
      const creditMonth = data.CreditMonth || '';
      const shopId = data.ShopID || '';
      const custEmail = data.CustEmail || 'test@sample.com';
      const saveCard = data.SaveCard || 'N';

      // Amount는 사용자가 입력한 값을 그대로 숫자로 사용
      const amountForApi = parseFloat(Amount || '0');
      const amountStr = String(amountForApi);

      // ----- CheckSum 계산 (매뉴얼 Table 1.3 + 제공해주신 예제 코드 준수) -----
      // RouteNo는 숫자로 사용 (예: 2)
      const routeNoForApi = parseInt(routeNo || '0', 10) || 0;

      // ----- CheckSum 계산 (제공된 generateCheckSum 과 동일한 방식) -----
      const payloadForChecksum = {
        OrderNo: orderNo,
        CustomerId: customerId,
        Amount: amountForApi,
        PhoneNumber: phoneNumber,
        Description: description,
        ChannelCode: channelCode,
        Currency: currency,
        LangCode: langCode,
        RouteNo: routeNoForApi,
        IPAddress: ipAddress,
        TokenType: tokenType,
        CreditToken: creditToken || null,
        DirectCreditToken: paymentToken,
        CreditMonth: creditMonth || null,
        ShopID: shopId || null,
        CustEmail: custEmail,
        SaveCard: saveCard,
      };

      const merchantCheckSum = generateDirectCreditCheckSum(
        payloadForChecksum,
        md5Key || '',
      );

      paymentRequest = {
        OrderNo: orderNo,
        CustomerId: customerId,
        // Amount는 JSON에서는 숫자 타입으로 전송 (예: 1500)
        Amount: amountForApi,
        PhoneNumber: phoneNumber,
        Description: description,
        ChannelCode: channelCode,
        // Currency는 문자열 코드로 전송 (예: "392", "764")
        Currency: currency,
        LangCode: langCode,
        RouteNo: routeNoForApi,
        IPAddress: ipAddress,
        TokenType: tokenType,
        CreditToken: creditToken || null,
        DirectCreditToken: paymentToken,
        CreditMonth: creditMonth || null,
        ShopID: shopId || null,
        CustEmail: custEmail,
        SaveCard: saveCard,
        CheckSum: merchantCheckSum,
      };

      const apiRes = await axios.post(paymentApiUrl, paymentRequest, {
        headers: {
          'Content-Type': 'application/json',
          'CHILLPAY-MerchantCode': merchantCode,
          'CHILLPAY-ApiKey': apiKey,
        },
        timeout: 15000,
        validateStatus: () => true,
      });

      paymentResponse = {
        status: apiRes.status,
        headers: apiRes.headers,
        data: apiRes.data,
      };

      // 3DS(OTP) 페이지 URL 추출 (있을 경우)
      let threeDSUrl = '';
      if (apiRes && apiRes.data && apiRes.data.data && apiRes.data.data.paymentUrl) {
        threeDSUrl = String(apiRes.data.data.paymentUrl);
      }

      // 응답에 data 객체가 있으면 요약 카드 HTML 구성
      if (apiRes && apiRes.data && apiRes.data.data) {
        const body = apiRes.data || {};
        const d = body.data || {};
        const paymentStatus = d.paymentStatus || '';
        const amountResp = typeof d.amount !== 'undefined' ? d.amount : '';
        const orderNoResp = d.orderNo || '';
        const transactionId = d.transactionId || '';
        const channelCodeResp = d.channelCode || '';
        const createdDate = d.createdDate || '';

        paymentSummaryHtml = `<div style="margin-top:8px;padding:10px 12px;border-radius:8px;border:1px solid #d1d5db;background:#f9fafb;font-size:13px;line-height:1.5;">
  <div><strong>status</strong>: ${body.status}</div>
  <div><strong>message</strong>: ${body.message || ''}</div>
  <div style="margin-top:4px;">
    <div><strong>paymentStatus</strong>: ${paymentStatus}</div>
    <div><strong>amount</strong>: ${amountResp}</div>
    <div><strong>orderNo</strong>: ${orderNoResp}</div>
    <div><strong>transactionId</strong>: ${transactionId}</div>
    <div><strong>channelCode</strong>: ${channelCodeResp}</div>
    <div><strong>createdDate</strong>: ${createdDate}</div>
  </div>
</div>`;
      }

      appendTestLog({
        loggedAt: new Date().toISOString(),
        configId,
        environment: cfg.environment,
        currency,
        merchantCode,
        routeNo,
        orderNo,
        amount: amountForApi,
        clientIp,
        request: paymentRequest,
        response: paymentResponse,
        error: null,
        form: data,
      });
    } catch (e) {
      paymentError = e.message || String(e);
      appendTestLog({
        loggedAt: new Date().toISOString(),
        configId,
        environment: cfg.environment,
        currency: Currency || cfg.currency || '',
        merchantCode: cfg.merchantCode || '',
        routeNo: RouteNo || cfg.routeNo || '',
        orderNo: OrderNo || '',
        amount: Amount || '',
        clientIp,
        request: paymentRequest,
        response: null,
        error: paymentError,
        form: data,
      });
    }
  }

  res.send(`<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <title>테스트 결제 결과</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
    h1 { margin-bottom: 8px; }
    h2 { margin-top: 20px; margin-bottom: 8px; font-size:16px; }
    pre { background:#020617; color:#e5e7eb; padding:12px; border-radius:8px; font-size:12px; white-space:pre-wrap; word-break:break-all; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:11px; font-weight:600; color:#6b7280; padding:4px 6px 2px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group-items { padding-left:0; padding-bottom:4px; }
    .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:8px; border:1px solid #e5e7eb; }
    .error { color:#b91c1c; font-size:13px; margin-top:4px; }
    .success { color:#166534; font-size:13px; margin-top:4px; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      <div class="topbar">
        <span>접속 IP: ${clientIp || '-'}</span>
        <span>시간: ${nowKr}</span>
        <span>아이디: ${adminUser || '-'}</span>
      </div>
      <div class="card">
        <h1>테스트 결제 결과</h1>
        <p style="font-size:13px;color:#555;">테스트 결제 요청의 전체 흐름과 결제 API 응답을 확인할 수 있습니다.</p>

        ${
          !paymentError && paymentResponse && paymentResponse.data && paymentResponse.data.data && paymentResponse.data.data.paymentUrl
            ? (() => {
                const otpUrl = paymentResponse.data.data.paymentUrl;
                return `<div style="margin:10px 0 16px;padding:10px 12px;border-radius:8px;border:1px solid #d1d5db;background:#ecfeff;font-size:13px;line-height:1.5;">
                <div style="font-weight:600;color:#0369a1;margin-bottom:4px;">3DS(OTP) 인증 페이지</div>
                <div style="word-break:break-all;margin-bottom:6px;">
                  <code style="font-size:12px;background:#e5e7eb;padding:2px 4px;border-radius:4px;color:#111827;">${otpUrl}</code>
                </div>
                <a href="${otpUrl}" target="_blank" rel="noopener noreferrer"
                   style="display:inline-block;margin-top:4px;padding:6px 12px;border-radius:6px;background:#0ea5e9;color:#f9fafb;text-decoration:none;font-size:13px;">
                  새 탭에서 3DS 페이지 열기
                </a>
                <p style="margin-top:8px;font-size:12px;color:#0369a1;">아래 링크를 클릭하거나, 팝업이 차단된 경우 새 탭에서 3DS 페이지 열기 버튼을 눌러주세요.</p>
              </div>
              <script>
              (function(){
                var u = ${JSON.stringify(otpUrl)};
                if (u) window.open(u, '_blank', 'noopener,noreferrer');
              })();
              <\/script>`;
              })()
            : ''
        }

        <h2>1. Inline 스크립트 전달 폼 데이터</h2>
        <pre>${JSON.stringify(data, null, 2)}</pre>

        <h2>2. 결제 API 요청</h2>
        ${
          paymentRequest
            ? `<pre>${JSON.stringify(paymentRequest, null, 2)}</pre>`
            : '<div style="font-size:13px;color:#6b7280;">결제 API 요청을 만들지 못했습니다.</div>'
        }

        <h2>3. 결제 API 응답</h2>
        ${
          paymentError
            ? `<div class="error">결제 API 호출 오류: ${paymentError}</div>`
            : paymentResponse
            ? `${paymentSummaryHtml || ''}<pre>${JSON.stringify(paymentResponse, null, 2)}</pre>`
            : '<div style="font-size:13px;color:#6b7280;">결제 API가 아직 호출되지 않았습니다.</div>'
        }

        <form method="get" action="/admin/test-pay">
          <button type="submit">다른 테스트 실행</button>
        </form>
      </div>
    </main>
  </div>
</body>
</html>`);
});

// PG 결제 완료 후 돌아올 테스트 결과 페이지 (ReturnUrl/Result 노티 릴레이 대상)
// POST: ChillPay Result 노티를 미들웨어가 릴레이할 때 호출 (가맹점 resultUrl 과 동일 방식), 인증 없음
app.post('/admin/test-pay/return', (req, res) => {
  // 릴레이 수신만 처리, 200 반환 (실제 결과 표시는 GET 또는 ChillPay 고객 리다이렉트로)
  res.status(200).json({ ok: true });
});

// GET: 고객이 ChillPay 리다이렉트로 접근하거나 관리자가 직접 접근 시 결과 페이지 표시
app.get('/admin/test-pay/return', requireAuth, requirePage('test_run'), (req, res) => {
  const locale = getLocale(req);
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });

  res.send(`<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <title>테스트 결제 완료</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:24px 28px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:8px; border:1px solid #e5e7eb; }
    .success-msg { font-size:18px; color:#166534; font-weight:600; margin:16px 0; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      <div class="topbar">
        <span>접속 IP: ${clientIp || '-'}</span>
        <span>시간: ${nowKr}</span>
        <span>아이디: ${adminUser || '-'}</span>
      </div>
      <div class="card">
        <h1>테스트 결제 완료</h1>
        <p class="success-msg">테스트는 정상적으로 완료 되었습니다. 축하합니다.</p>
        <p style="font-size:13px;color:#555;">PG 결제 완료 후 ReturnUrl 로 이동한 페이지입니다.</p>
        <a href="/admin/test-pay" style="display:inline-block;margin-top:12px;padding:8px 16px;background:#2563eb;color:#fff;text-decoration:none;border-radius:6px;font-size:14px;">테스트 실행으로 돌아가기</a>
      </div>
    </main>
  </div>
</body>
</html>`);
});

// 테스트 결제 내역 페이지 (최근 2건, 로그 시각 4타임존)
app.get('/admin/test-logs', requireAuth, requirePage('test_history'), (req, res) => {
  const locale = getLocale(req);
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

  const recentLogs = [...TEST_LOGS].reverse().slice(0, 2);
  const rows = recentLogs
    .map((log) => {
      const tz = formatTimeMultiTZ(log.loggedAt);
      const timeHtml =
        typeof tz === 'object' && tz.th != null
          ? `TH: ${esc(tz.th)}<br>JP: ${esc(tz.jp)}<br>SG: ${esc(tz.sg)}<br>US: ${esc(tz.us)}`
          : esc(log.loggedAt || '-');
      const status =
        log.response && typeof log.response.status !== 'undefined'
          ? log.response.status
          : '-';
      return `<tr>
        <td style="white-space:nowrap;font-size:11px;">${timeHtml}</td>
        <td>${esc(log.configId || '')}</td>
        <td>${esc(log.environment || '')}</td>
        <td>${esc(log.currency || '')}</td>
        <td>${esc(log.orderNo || '')}</td>
        <td>${esc(log.amount ?? '')}</td>
        <td>${esc(String(status))}</td>
        <td><pre style="margin:0;white-space:pre-wrap;font-size:11px;">${esc(log.error || '')}</pre></td>
        <td><pre style="margin:0;white-space:pre-wrap;font-size:11px;">${esc(log.request ? JSON.stringify(log.request, null, 2) : '')}</pre></td>
        <td><pre style="margin:0;white-space:pre-wrap;font-size:11px;">${esc(log.response ? JSON.stringify(log.response, null, 2) : '')}</pre></td>
      </tr>`;
    })
    .join('');

  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'test_history_title')}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
    h1 { margin-bottom: 8px; }
    table { border-collapse: collapse; width: 100%; background:#fff; }
    th, td { border: 1px solid #e5e7eb; padding: 8px 10px; font-size: 12px; vertical-align: top; }
    th { background: #e5f0ff; text-align: center; }
    tr:nth-child(even) { background:#f9fafb; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:11px; font-weight:600; color:#6b7280; padding:4px 6px 2px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group-items { padding-left:0; padding-bottom:4px; }
    .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:8px; border:1px solid #e5e7eb; }
    pre { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; white-space:pre-wrap; word-break:break-all; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card">
        <h1>${t(locale, 'test_history_title')}</h1>
        <p style="font-size:13px;color:#555;">${t(locale, 'test_history_desc')}</p>
        <table>
          <thead>
            <tr>
              <th>${t(locale, 'test_history_time')}</th>
              <th>${t(locale, 'test_history_config_id')}</th>
              <th>${t(locale, 'test_history_env')}</th>
              <th>${t(locale, 'test_history_currency')}</th>
              <th>${t(locale, 'test_history_order_no')}</th>
              <th>${t(locale, 'test_history_amount')}</th>
              <th>${t(locale, 'test_history_http')}</th>
              <th>${t(locale, 'test_history_error')}</th>
              <th>${t(locale, 'test_history_request')}</th>
              <th>${t(locale, 'test_history_response')}</th>
            </tr>
          </thead>
          <tbody>
            ${rows || `<tr><td colspan="10" style="text-align:center;color:#777;">${t(locale, 'test_history_empty')}</td></tr>`}
          </tbody>
        </table>
      </div>
    </main>
  </div>
</body>
</html>`);
});

// 전산 노티 대상 삭제 처리
app.post('/admin/internal-targets/delete', requireAuth, requirePage('internal_targets'), (req, res) => {
  const { id } = req.body;
  if (!id) {
    return res.status(400).send('id 는 필수입니다.');
  }

  const actor = req.session.adminUser || 'unknown';
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const before = INTERNAL_TARGETS.get(id) || null;

  if (!before) {
    return res.redirect('/admin/internal-targets');
  }

  INTERNAL_TARGETS.delete(id);
  saveInternalTargets();

  appendConfigChangeLog({
    type: 'internal_target_delete',
    actor,
    clientIp,
    targetId: id,
    before,
  });

  return res.redirect('/admin/internal-targets');
});

// 헬스 체크
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'ok' });
});

// 전산 노티 수신 (자체 사용용 엔드포인트 예시)
// INTERNAL_NOTI_URL 을 http://localhost:3000/internal/noti 로 설정하면
// transformForInternal 결과가 이곳에도 저장됩니다.
app.post('/internal/noti', (req, res) => {
  const body = req.body;
  const entry = {
    storedAt: new Date().toISOString(),
    payload: body,
  };
  appendInternalLog(entry);
  console.log('[전산 수신] /internal/noti', JSON.stringify(body));
  res.status(200).json({ ok: true });
});

// 전산 노티 로그 페이지
app.get('/admin/internal', requireAuth, requirePage('internal_logs'), (req, res) => {
  const locale = getLocale(req);
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const reversedInternal = [...INTERNAL_LOGS].slice().reverse();
  const rows = reversedInternal
    .map((log, i) => {
      const realIndex = INTERNAL_LOGS.length - 1 - i;
      const dt = formatDateAndTimeTHJP(log.storedAtIso || log.storedAt);
      const payload = log.payload || {};
      const jsonHeader = Object.keys(payload).join(', ');
      const jsonValue = JSON.stringify(payload, null, 2);
      const internalStatus = log.internalDeliveryStatus || '-';
      const internalLabel = internalStatus === 'ok' ? '성공' : internalStatus === 'fail' ? '실패' : internalStatus === 'skip' ? '미전송' : internalStatus;
      const internalClass = internalStatus === 'ok' ? 'status-ok' : internalStatus === 'fail' ? 'status-fail' : '';
      const canResend = log.internalTargetUrl && log.payload;
      const internalResendKind = isCancelNotiBody(payload) ? 'cancel' : 'payment';
      const internalResendLabel = internalResendKind === 'cancel' ? '취소 재전송' : '결제 재전송';
      const resendBtn = canResend
        ? `<form method="post" action="/admin/internal/resend" style="display:inline;" onsubmit="return confirm('해당 노티를 전산으로 다시 전송하시겠습니까?');"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="resendKind" value="${internalResendKind}" /><button type="submit" class="btn-resend">${internalResendLabel}</button></form>`
        : '<span class="label-none">노티없음</span>';
      const internalTargetName = getInternalTargetName(log.internalTargetId);
      return `<tr>
        <td class="col-date">${esc(dt.date)}</td>
        <td class="col-time">TH: ${esc(dt.timeTh)}<br><span class="time-jp">JP: ${esc(dt.timeJp)}</span></td>
        <td class="col-narrow">${esc(internalTargetName)}</td>
        <td class="col-status"><span class="${internalClass}">${esc(internalLabel)}</span></td>
        <td class="col-header"><pre>${esc(jsonHeader)}</pre></td>
        <td class="col-json"><pre>${esc(jsonValue)}</pre></td>
        <td class="col-action">${resendBtn}</td>
      </tr>`;
    })
    .join('');

  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'internal_logs_title')}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
    h1 { margin-bottom: 8px; }
    table { border-collapse: collapse; width: 100%; background:#fff; table-layout: fixed; }
    th, td { border: 1px solid #e5e7eb; padding: 8px 10px; font-size: 13px; vertical-align: middle; text-align: center; }
    th { background: #e5f0ff; }
    tr:nth-child(even) { background:#f9fafb; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:8px; border:1px solid #e5e7eb; }
    .col-header, .col-json { text-align: left; }
    pre { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; white-space:pre-wrap; font-size:12px; text-align: left; margin:0; }
    .time-jp { color: #2563eb; }
    .status-ok { color: #059669; font-weight: 600; }
    .status-fail { color: #dc2626; font-weight: 600; }
    .label-none { color:#b91c1c; font-weight:700; }
    .btn-resend { padding: 4px 10px; font-size: 12px; background: #2563eb; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
    .btn-resend:hover { background: #1d4ed8; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card">
      <h1>${t(locale, 'internal_logs_title')} (${INTERNAL_LOGS.length})</h1>
      <p style="font-size:13px;color:#555;">${t(locale, 'internal_logs_desc')}</p>
      <table>
        <colgroup><col style="width:7%;" /><col style="width:9%;" /><col style="width:12%;" /><col style="width:5%;" /><col style="width:20%;" /><col style="width:43%;" /><col style="width:6%;" /></colgroup>
        <thead>
          <tr>
            <th>수신일</th>
            <th>수신시각</th>
            <th>전산 대상</th>
            <th>전산 수신</th>
            <th>${t(locale, 'internal_logs_header')}</th>
            <th>${t(locale, 'internal_logs_value')}</th>
            <th>재전송</th>
          </tr>
        </thead>
        <tbody>
          ${rows || `<tr><td colspan="7" style="text-align:center;color:#777;">${t(locale, 'internal_logs_empty')}</td></tr>`}
        </tbody>
      </table>
      </div>
    </main>
  </div>
</body>
</html>`);
});

// 정산 노티 재전송 (전산으로 수동 재전송)
app.post('/admin/internal/resend', requireAuth, requirePageAny(['internal_logs', 'internal_result']), async (req, res) => {
  const returnTo = (req.body.returnTo || '').trim() || 'internal';
  const base = returnTo === 'internal-result' ? '/admin/internal-result' : '/admin/internal';
  const index = parseInt(req.body.index, 10);
  if (Number.isNaN(index) || index < 0 || index >= INTERNAL_LOGS.length) {
    return res.redirect(base + '?err=invalid');
  }
  const log = INTERNAL_LOGS[index];
  const url = log.internalTargetUrl;
  if (!url || !log.payload) {
    return res.redirect(base + '?err=no_target');
  }
  const resendKind = (req.body.resendKind || '').toString().toLowerCase() || (isCancelNotiBody(log.payload || {}) ? 'cancel' : 'payment');
  try {
    const result = await sendToInternal(url, log.payload);
    if (result.success) return res.redirect(base + '?resend=ok&resendKind=' + encodeURIComponent(resendKind));
    const reason = result.status ? 'HTTP ' + result.status : (result.error || '');
    return res.redirect(base + '?resend=fail&reason=' + encodeURIComponent(reason) + '&resendKind=' + encodeURIComponent(resendKind));
  } catch (err) {
    return res.redirect(base + '?resend=fail&reason=' + encodeURIComponent(err.message || String(err)) + '&resendKind=' + encodeURIComponent(resendKind));
  }
});

// 전산결과 (요약) 페이지
app.get('/admin/internal-result', requireAuth, requirePage('internal_result'), (req, res) => {
  const locale = getLocale(req);
  const q = req.query || {};
  const resendKind = (q.resendKind || 'payment').toString().toLowerCase();
  const resendOkLabel = resendKind === 'cancel' ? '취소 재전송이 완료되었습니다.' : '결제 재전송이 완료되었습니다.';
  const resendFailLabel = resendKind === 'cancel' ? '취소 재전송 실패' : '결제 재전송 실패';
  const resendMsg =
    q.resend === 'ok'
      ? '<div class="alert alert-ok">' + resendOkLabel + '</div>'
      : q.resend === 'fail'
      ? '<div class="alert alert-fail">' + resendFailLabel + (q.reason ? ': ' + String(q.reason).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '') + '</div>'
      : q.err ? '<div class="alert alert-fail">' + (q.err === 'invalid' ? '잘못된 요청입니다.' : '대상 URL 없음') + '</div>'
      : '';
  const clientIp = (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() || req.ip || '';
  const adminUser = req.session.adminUser || '';
  const nowKr = new Date().toLocaleString('ko-KR', { hour12: false });
  const nowTh = new Date().toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });
  const envLabel = APP_ENV === 'test' ? 'sandbox' : 'live';
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const reversed = [...INTERNAL_LOGS].slice().reverse();
  const rows = reversed
    .map((log, i) => {
      const realIndex = INTERNAL_LOGS.length - 1 - i;
      const dt = formatDateAndTimeTHJP(log.storedAtIso || log.storedAt);
      const status = log.internalDeliveryStatus || '-';
      const label = status === 'ok' ? '성공' : status === 'fail' ? '실패' : status === 'skip' ? '미전송' : status;
      const statusClass = status === 'ok' ? 'status-ok' : status === 'fail' ? 'status-fail' : '';
      const canResend = log.internalTargetUrl && log.payload;
      const payload = log.payload || {};
      const internalResendKind = isCancelNotiBody(payload) ? 'cancel' : 'payment';
      const internalResendLabel = internalResendKind === 'cancel' ? '취소 재전송' : '결제 재전송';
      const resendBtn = canResend
        ? `<form method="post" action="/admin/internal/resend" style="display:inline;"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="returnTo" value="internal-result" /><input type="hidden" name="resendKind" value="${internalResendKind}" /><button type="submit" class="btn-resend" onclick="return confirm('해당 노티를 전산으로 다시 전송하시겠습니까?');">${internalResendLabel}</button></form>`
        : '-';
      const txId = payload.TransactionId != null ? payload.TransactionId : (payload.transactionId != null ? payload.transactionId : '-');
      const payStatus = payload.PaymentStatus != null ? payload.PaymentStatus : '-';
      const statusDesc = payStatus === '2' || payStatus === 2 ? '무효' : payStatus === '9' || payStatus === 9 ? '환불' : payStatus === '1' || payStatus === 1 ? '결제' : payStatus;
      const internalTargetName = getInternalTargetName(log.internalTargetId);
      return `<tr>
        <td>${esc(dt.date)}</td>
        <td>TH: ${esc(dt.timeTh)}<br><span class="time-jp">JP: ${esc(dt.timeJp)}</span></td>
        <td>${esc(String(txId))}</td>
        <td>${esc(statusDesc)}</td>
        <td>${esc(log.routeNo || '-')}</td>
        <td>${esc(internalTargetName)}</td>
        <td>${esc(envLabel)}</td>
        <td>${esc(log.merchantId || '-')}</td>
        <td><span class="${statusClass}">${esc(label)}</span></td>
        <td class="col-fail-reason">-</td>
        <td>${resendBtn}</td>
      </tr>`;
    })
    .join('');

  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'nav_internal_result') || '전산 결과'}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
    table { border-collapse: collapse; width: 100%; background:#fff; font-size: 13px; }
    th, td { border: 1px solid #e5e7eb; padding: 8px 10px; vertical-align: middle; text-align: center; }
    th { background: #e5f0ff; }
    tr:nth-child(even) { background:#f9fafb; }
    .status-ok { color: #059669; font-weight: 600; }
    .status-fail { color: #dc2626; font-weight: 600; }
    .time-jp { color: #2563eb; }
    .col-fail-reason { text-align: center; word-break: break-all; max-width: 200px; }
    .btn-resend { padding: 4px 10px; font-size: 12px; background: #2563eb; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
    .alert { padding: 10px 14px; border-radius: 8px; margin-bottom: 12px; font-size: 13px; }
    .alert-ok { background: #d1fae5; color: #065f46; border: 1px solid #6ee7b7; }
    .alert-fail { background: #fee2e2; color: #991b1b; border: 1px solid #fca5a5; }
    .layout { display:flex; min-height:100vh; width:100%; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
    .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); border:1px solid #e5e7eb; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card">
      ${resendMsg}
      <h1>${t(locale, 'nav_internal_result') || '전산 결과'} (${INTERNAL_LOGS.length})</h1>
      <p style="font-size:13px;color:#555;">전산 노티 전달 결과 요약 (성공/실패·재전송). 결제 노티와 무효/환불 노티를 전산으로 <strong>실제 전송한 건</strong>만 표시됩니다. 전산에 보내지 않은 건(전산 URL 미설정 등)은 이 목록에 없으며, 무효/환불 건은 <a href="/admin/cancel-refund/noti">노티 결과</a>에서 「전산 수신: 미전송」으로 확인할 수 있습니다.</p>
      <table>
        <thead>
          <tr>
            <th>수신일</th>
            <th>수신시각</th>
            <th>TransactionId</th>
            <th>구분</th>
            <th>route</th>
            <th>전산 대상</th>
            <th>환경</th>
            <th>merchant id</th>
            <th>전산 전달</th>
            <th>실패원인</th>
            <th>재전송</th>
          </tr>
        </thead>
        <tbody>
          ${rows || '<tr><td colspan="11" style="text-align:center;color:#777;">데이터 없음</td></tr>'}
        </tbody>
      </table>
      </div>
    </main>
  </div>
</body>
</html>`);
});

// 개발 노티 로그 페이지
app.get('/admin/dev-internal', requireAuth, requirePage('dev_internal_logs'), (req, res) => {
  const locale = getLocale(req);
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const reversedDev = [...DEV_INTERNAL_LOGS].slice().reverse();
  const rows = reversedDev
    .map((log, i) => {
      const realIndex = DEV_INTERNAL_LOGS.length - 1 - i;
      const dt = formatDateAndTimeTHJP(log.storedAtIso || log.storedAt);
      const payload = log.payload || {};
      const jsonHeader = Object.keys(payload).join(', ');
      const jsonValue = JSON.stringify(payload, null, 2);
      const internalStatus = log.internalDeliveryStatus || '-';
      const internalLabel =
        internalStatus === 'ok'
          ? '성공'
          : internalStatus === 'fail'
          ? '실패'
          : internalStatus === 'skip'
          ? '미전송'
          : internalStatus;
      const internalClass = internalStatus === 'ok' ? 'status-ok' : internalStatus === 'fail' ? 'status-fail' : '';
      const canResend = log.internalTargetUrl && log.payload;
      const devResendKind = isCancelNotiBody(payload) ? 'cancel' : 'payment';
      const devResendLabel = devResendKind === 'cancel' ? '취소 재전송' : '결제 재전송';
      const resendBtn = canResend
        ? `<form method="post" action="/admin/dev-internal/resend" style="display:inline;" onsubmit="return confirm('해당 노티를 개발 전산으로 다시 전송하시겠습니까?');"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="resendKind" value="${devResendKind}" /><button type="submit" class="btn-resend">${devResendLabel}</button></form>`
        : '<span class="label-none">노티없음</span>';
      return `<tr>
        <td class="col-date">${esc(dt.date)}</td>
        <td class="col-time">TH: ${esc(dt.timeTh)}<br><span class="time-jp">JP: ${esc(dt.timeJp)}</span></td>
        <td class="col-status"><span class="${internalClass}">${esc(internalLabel)}</span></td>
        <td class="col-header"><pre>${esc(jsonHeader)}</pre></td>
        <td class="col-json"><pre>${esc(jsonValue)}</pre></td>
        <td class="col-action">${resendBtn}</td>
      </tr>`;
    })
    .join('');

  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'nav_dev_internal_noti_log') || '개발 노티 로그'}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
    h1 { margin-bottom: 8px; }
    table { border-collapse: collapse; width: 100%; background:#fff; table-layout: fixed; }
    th, td { border: 1px solid #e5e7eb; padding: 8px 10px; font-size: 13px; vertical-align: middle; text-align: center; }
    th { background: #e5f0ff; }
    tr:nth-child(even) { background:#f9fafb; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:8px; border:1px solid #e5e7eb; }
    .col-header, .col-json { text-align: left; }
    pre { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; white-space:pre-wrap; font-size:12px; text-align: left; margin:0; }
    .time-jp { color: #2563eb; }
    .status-ok { color: #059669; font-weight: 600; }
    .status-fail { color: #dc2626; font-weight: 600; }
    .label-none { color:#b91c1c; font-weight:700; }
    .btn-resend { padding: 4px 10px; font-size: 12px; background: #2563eb; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
    .btn-resend:hover { background: #1d4ed8; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card">
      <h1>${t(locale, 'nav_dev_internal_noti_log') || '개발 노티 로그'} (${DEV_INTERNAL_LOGS.length})</h1>
      <p style="font-size:13px;color:#555;">${t(locale, 'internal_logs_desc')}</p>
      <table>
        <colgroup><col style="width:8%;" /><col style="width:10%;" /><col style="width:6%;" /><col style="width:22%;" /><col style="width:46%;" /><col style="width:6%;" /></colgroup>
        <thead>
          <tr>
            <th>수신일</th>
            <th>수신시각</th>
            <th>전산 수신</th>
            <th>${t(locale, 'internal_logs_header')}</th>
            <th>${t(locale, 'internal_logs_value')}</th>
            <th>재전송</th>
          </tr>
        </thead>
        <tbody>
          ${rows || `<tr><td colspan="6" style="text-align:center;color:#777;">${t(locale, 'internal_logs_empty')}</td></tr>`}
        </tbody>
      </table>
      </div>
    </main>
  </div>
</body>
</html>`);
});

// 개발 노티 재전송
app.post('/admin/dev-internal/resend', requireAuth, requirePageAny(['dev_internal_logs', 'dev_result']), async (req, res) => {
  const returnTo = (req.body.returnTo || '').trim() || 'dev-internal';
  const base = returnTo === 'dev-internal-result' ? '/admin/dev-internal-result' : '/admin/dev-internal';
  const index = parseInt(req.body.index, 10);
  if (Number.isNaN(index) || index < 0 || index >= DEV_INTERNAL_LOGS.length) {
    return res.redirect(base + '?err=invalid');
  }
  const log = DEV_INTERNAL_LOGS[index];
  const url = log.internalTargetUrl;
  if (!url || !log.payload) {
    return res.redirect(base + '?err=no_target');
  }
  const resendKind = (req.body.resendKind || '').toString().toLowerCase() || (isCancelNotiBody(log.payload || {}) ? 'cancel' : 'payment');
  try {
    const result = await sendToInternal(url, log.payload);
    if (result.success) return res.redirect(base + '?resend=ok&resendKind=' + encodeURIComponent(resendKind));
    const reason = result.status ? 'HTTP ' + result.status : (result.error || '');
    return res.redirect(base + '?resend=fail&reason=' + encodeURIComponent(reason) + '&resendKind=' + encodeURIComponent(resendKind));
  } catch (err) {
    return res.redirect(base + '?resend=fail&reason=' + encodeURIComponent(err.message || String(err)) + '&resendKind=' + encodeURIComponent(resendKind));
  }
});

// 개발결과 (요약) 페이지
app.get('/admin/dev-internal-result', requireAuth, requirePage('dev_result'), (req, res) => {
  const locale = getLocale(req);
  const q = req.query || {};
  const resendKind = (q.resendKind || 'payment').toString().toLowerCase();
  const resendOkLabel = resendKind === 'cancel' ? '취소 재전송이 완료되었습니다.' : '결제 재전송이 완료되었습니다.';
  const resendFailLabel = resendKind === 'cancel' ? '취소 재전송 실패' : '결제 재전송 실패';
  const resendMsg =
    q.resend === 'ok'
      ? '<div class="alert alert-ok">' + resendOkLabel + '</div>'
      : q.resend === 'fail'
      ? '<div class="alert alert-fail">' + resendFailLabel + (q.reason ? ': ' + String(q.reason).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '') + '</div>'
      : q.err ? '<div class="alert alert-fail">' + (q.err === 'invalid' ? '잘못된 요청입니다.' : '대상 URL 없음') + '</div>'
      : '';
  const clientIp = (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() || req.ip || '';
  const adminUser = req.session.adminUser || '';
  const nowKr = new Date().toLocaleString('ko-KR', { hour12: false });
  const nowTh = new Date().toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });
  const envLabel = APP_ENV === 'test' ? 'sandbox' : 'live';
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const reversed = [...DEV_INTERNAL_LOGS].slice().reverse();
  const rows = reversed
    .map((log, i) => {
      const realIndex = DEV_INTERNAL_LOGS.length - 1 - i;
      const dt = formatDateAndTimeTHJP(log.storedAtIso || log.storedAt);
      const status = log.internalDeliveryStatus || '-';
      const label = status === 'ok' ? '성공' : status === 'fail' ? '실패' : status === 'skip' ? '미전송' : status;
      const statusClass = status === 'ok' ? 'status-ok' : status === 'fail' ? 'status-fail' : '';
      const canResend = log.internalTargetUrl && log.payload;
      const payload = log.payload || {};
      const devResendKind = isCancelNotiBody(payload) ? 'cancel' : 'payment';
      const devResendLabel = devResendKind === 'cancel' ? '취소 재전송' : '결제 재전송';
      const resendBtn = canResend
        ? `<form method="post" action="/admin/dev-internal/resend" style="display:inline;"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="returnTo" value="dev-internal-result" /><input type="hidden" name="resendKind" value="${devResendKind}" /><button type="submit" class="btn-resend" onclick="return confirm('해당 노티를 개발 전산으로 다시 전송하시겠습니까?');">${devResendLabel}</button></form>`
        : '-';
      return `<tr>
        <td>${esc(dt.date)}</td>
        <td>TH: ${esc(dt.timeTh)}<br><span class="time-jp">JP: ${esc(dt.timeJp)}</span></td>
        <td>${esc(log.routeNo || '-')}</td>
        <td>${esc(envLabel)}</td>
        <td>${esc(log.merchantId || '-')}</td>
        <td><span class="${statusClass}">${esc(label)}</span></td>
        <td class="col-fail-reason">-</td>
        <td>${resendBtn}</td>
      </tr>`;
    })
    .join('');

  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'nav_dev_result') || '개발 결과'}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
    table { border-collapse: collapse; width: 100%; background:#fff; font-size: 13px; }
    th, td { border: 1px solid #e5e7eb; padding: 8px 10px; vertical-align: middle; text-align: center; }
    th { background: #e5f0ff; }
    tr:nth-child(even) { background:#f9fafb; }
    .status-ok { color: #059669; font-weight: 600; }
    .status-fail { color: #dc2626; font-weight: 600; }
    .time-jp { color: #2563eb; }
    .col-fail-reason { text-align: center; word-break: break-all; max-width: 200px; }
    .btn-resend { padding: 4px 10px; font-size: 12px; background: #2563eb; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
    .alert { padding: 10px 14px; border-radius: 8px; margin-bottom: 12px; font-size: 13px; }
    .alert-ok { background: #d1fae5; color: #065f46; border: 1px solid #6ee7b7; }
    .alert-fail { background: #fee2e2; color: #991b1b; border: 1px solid #fca5a5; }
    .layout { display:flex; min-height:100vh; width:100%; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav-group-items a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:13px; border-radius:6px; }
    .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); border:1px solid #e5e7eb; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card">
      ${resendMsg}
      <h1>${t(locale, 'nav_dev_result') || '개발 결과'} (${DEV_INTERNAL_LOGS.length})</h1>
      <p style="font-size:13px;color:#555;">개발 전산 노티 전달 결과 요약 (성공/실패·재전송)</p>
      <table>
        <thead>
          <tr>
            <th>수신일</th>
            <th>수신시각</th>
            <th>route</th>
            <th>환경</th>
            <th>merchant id</th>
            <th>성공유무</th>
            <th>실패원인</th>
            <th>재전송</th>
          </tr>
        </thead>
        <tbody>
          ${rows || '<tr><td colspan="8" style="text-align:center;color:#777;">데이터 없음</td></tr>'}
        </tbody>
      </table>
      </div>
    </main>
  </div>
</body>
</html>`);
});

// 트래픽 분석 (일자별 / 월간별 / 시간별)
function aggregateTraffic() {
  const byDay = {};
  const byMonth = {};
  const byHour = {};
   const byDowHour = Array.from({ length: 7 }, () => Array(24).fill(0));
  for (let i = 0; i < TRAFFIC_HITS.length; i++) {
    const hit = TRAFFIC_HITS[i];
    const d = new Date(hit.at);
    if (Number.isNaN(d.getTime())) continue;
    const dayKey = d.toISOString().slice(0, 10);
    const monthKey = d.toISOString().slice(0, 7);
    const hourKey = d.getUTCHours();
    const dow = d.getUTCDay();
    byDay[dayKey] = (byDay[dayKey] || 0) + 1;
    byMonth[monthKey] = (byMonth[monthKey] || 0) + 1;
    byHour[hourKey] = (byHour[hourKey] || 0) + 1;
    byDowHour[dow][hourKey] = (byDowHour[dow][hourKey] || 0) + 1;
  }
  const dayEntries = Object.entries(byDay).sort((a, b) => b[0].localeCompare(a[0])).slice(0, 31);
  const monthEntries = Object.entries(byMonth).sort((a, b) => b[0].localeCompare(a[0])).slice(0, 12);
  const hourEntries = Array.from({ length: 24 }, (_, h) => [h, byHour[h] || 0]);
  const maxDay = Math.max(1, ...dayEntries.map((e) => e[1]));
  const maxMonth = Math.max(1, ...monthEntries.map((e) => e[1]));
  const maxHour = Math.max(1, ...hourEntries.map((e) => e[1]));
  let maxHeat = 0;
  for (let d = 0; d < 7; d++) {
    for (let h = 0; h < 24; h++) {
      if (byDowHour[d][h] > maxHeat) maxHeat = byDowHour[d][h];
    }
  }
  return { dayEntries, monthEntries, hourEntries, maxDay, maxMonth, maxHour, byDowHour, maxHeat };
}

app.get('/admin/traffic', requireAuth, requirePage('traffic_analysis'), (req, res) => {
  const locale = getLocale(req);
  const clientIp = (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() || req.ip || '';
  const adminUser = req.session.adminUser || '';
  const nowDate = new Date();
  const nowKr = nowDate.toLocaleString('ko-KR', { hour12: false });
  const nowTh = nowDate.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok', hour12: false });
  const { dayEntries, monthEntries, hourEntries, maxDay, maxMonth, maxHour, byDowHour, maxHeat } = aggregateTraffic();
  const total = TRAFFIC_HITS.length;
  const dayRows = dayEntries.map(([day, count]) => {
    const pct = maxDay ? Math.round((count / maxDay) * 100) : 0;
    return `<tr><td>${day}</td><td>${count}</td><td><div class="bar-wrap"><div class="bar" style="width:${pct}%;"></div></div></td></tr>`;
  }).join('');
  const monthRows = monthEntries.map(([month, count]) => {
    const pct = maxMonth ? Math.round((count / maxMonth) * 100) : 0;
    return `<tr><td>${month}</td><td>${count}</td><td><div class="bar-wrap"><div class="bar" style="width:${pct}%;"></div></div></td></tr>`;
  }).join('');
  const hourRows = hourEntries.map(([hour, count]) => {
    const pct = maxHour ? Math.round((count / maxHour) * 100) : 0;
    return `<tr><td class="hour-label">${hour}시</td><td class="hour-count">${count}</td><td class="hour-bar"><div class="bar-wrap"><div class="bar" style="width:${pct}%;"></div></div></td></tr>`;
  }).join('');
  const hourGridCells = hourEntries.map(([hour, count]) => {
    const pct = maxHour ? Math.round((count / maxHour) * 100) : 0;
    return `<div class="hour-cell" title="${hour}시: ${count}건"><div class="hour-cell-label">${hour}시</div><div class="hour-cell-bar-wrap"><div class="hour-cell-bar" style="height:${pct}%;"></div></div><div class="hour-cell-count">${count}</div></div>`;
  }).join('');
  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'nav_traffic_analysis')}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; color:#111827; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav-group { margin-bottom:2px; border:1px solid transparent; border-radius:6px; }
    .nav-group-summary { font-size:14px; font-weight:600; color:#e5e7eb; padding:6px 8px; cursor:pointer; list-style:none; text-transform:uppercase; letter-spacing:0.06em; display:flex; align-items:center; justify-content:space-between; border-radius:6px; }
    .nav-group-summary::-webkit-details-marker { display:none; }
    .nav-group-summary::marker { content:""; }
    .nav-group-summary::after { content:"\\25BC"; font-size:14px; opacity:0.7; transition:transform 0.15s ease; flex-shrink:0; }
    .nav-group[open] .nav-group-summary::after { transform:rotate(-180deg); }
    .nav-group-items { padding-left:4px; padding-bottom:4px; }
    .nav a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(59, 130, 246, 0.35); color:#dbeafe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; min-width:0; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; margin-bottom:0; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:18px 22px; border-radius:10px; box-shadow:0 10px 25px rgba(15,23,42,0.06); border:1px solid #e5e7eb; margin-bottom:16px; width:100%; box-sizing:border-box; }
    .traffic-card { overflow-x:auto; }
    h1 { margin-bottom:8px; }
    h2 { margin-top:20px; margin-bottom:12px; font-size:16px; }
    table { border-collapse:collapse; width:100%; table-layout:auto; }
    th, td { border:1px solid #e5e7eb; padding:10px 12px; font-size:14px; text-align:center; }
    th { background:#e5f0ff; color:#1f2937; }
    tr:nth-child(even) { background:#f9fafb; }
    .bar-wrap { background:#e5e7eb; border-radius:4px; height:22px; min-width:120px; overflow:hidden; }
    .bar { background:#2563eb; height:100%; border-radius:4px; }
    .stat { font-size:18px; font-weight:600; color:#1e293b; margin-bottom:12px; }
    .hour-label { width:70px; }
    .hour-count { width:80px; }
    .hour-bar { min-width:200px; }
    .hour-grid { display:grid; grid-template-columns:repeat(24, 1fr); gap:6px; margin-top:12px; min-width:800px; }
    .hour-cell { display:flex; flex-direction:column; align-items:center; padding:8px 4px; background:#f9fafb; border-radius:6px; border:1px solid #e5e7eb; min-height:80px; }
    .hour-cell-label { font-size:11px; color:#6b7280; margin-bottom:4px; }
    .hour-cell-bar-wrap { width:100%; height:36px; background:#e5e7eb; border-radius:4px; overflow:hidden; display:flex; align-items:flex-end; }
    .hour-cell-bar { width:100%; min-height:2px; background:#2563eb; border-radius:0 0 4px 4px; transition:height 0.2s; }
    .hour-cell-count { font-size:12px; font-weight:600; color:#1e293b; margin-top:4px; }
    .traffic-tables { display:grid; grid-template-columns:1fr 1fr; gap:24px; }
    @media (max-width: 1200px) { .traffic-tables { grid-template-columns:1fr; } }
    .charts-row { display:grid; grid-template-columns:1fr 1fr; gap:24px; margin:12px 0 4px; }
    @media (max-width: 1200px) { .charts-row { grid-template-columns:1fr; } }
    .chart-block canvas { width:100%; max-height:260px; }
    .heatmap-grid { display:grid; grid-template-columns:repeat(25, minmax(32px, 1fr)); gap:4px; margin-top:8px; font-size:11px; }
    .heatmap-cell { padding:4px 2px; text-align:center; border-radius:4px; border:1px solid #e5e7eb; box-sizing:border-box; }
    .heatmap-cell.header { background:#e5f0ff; font-weight:600; color:#1f2937; }
    .heatmap-cell.label { background:#f9fafb; font-weight:600; color:#4b5563; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member, req.originalUrl)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, req.originalUrl)}
      <div class="card traffic-card">
        <h1>${t(locale, 'nav_traffic_analysis')}</h1>
        <p class="stat">총 요청 수 (최근 ${TRAFFIC_HITS_MAX}건 기준): <strong>${total}</strong></p>
        <div class="chart-block">
          <h2>시간별 추이 (라인/영역)</h2>
          <canvas id="chart-hour-line"></canvas>
        </div>
        <div class="charts-row">
          <div class="chart-block">
            <h2>일자별</h2>
            <canvas id="chart-day-line"></canvas>
          </div>
          <div class="chart-block">
            <h2>월간</h2>
            <canvas id="chart-month-bar"></canvas>
          </div>
        </div>
        <h2>시간별 (UTC 0~23시) 상세</h2>
        <div class="hour-grid">${hourGridCells}</div>
        <table style="margin-top:16px;"><thead><tr><th class="hour-label">시간</th><th class="hour-count">건수</th><th class="hour-bar">비율</th></tr></thead><tbody>${hourRows}</tbody></table>
        <h2 style="margin-top:20px;">요일·시간 히트맵</h2>
        <div id="traffic-heatmap" class="heatmap-grid"></div>
      </div>
      <div class="card traffic-card traffic-tables">
        <div>
          <h2>일자별</h2>
          <table><thead><tr><th>일자</th><th>건수</th><th>비율</th></tr></thead><tbody>${dayRows || '<tr><td colspan="3">데이터 없음</td></tr>'}</tbody></table>
        </div>
        <div>
          <h2>월간별</h2>
          <table><thead><tr><th>월</th><th>건수</th><th>비율</th></tr></thead><tbody>${monthRows || '<tr><td colspan="3">데이터 없음</td></tr>'}</tbody></table>
        </div>
      </div>
    </main>
  </div>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <script>
    (function () {
      if (typeof Chart === 'undefined') return;
      var dayEntries = ${JSON.stringify(dayEntries)};
      var monthEntries = ${JSON.stringify(monthEntries)};
      var hourEntries = ${JSON.stringify(hourEntries)};
      var heatmap = ${JSON.stringify(byDowHour)};
      var heatMax = ${maxHeat};
      var locale = ${JSON.stringify(locale)};

      function createHourLineAreaChart() {
        var ctx = document.getElementById('chart-hour-line');
        if (!ctx) return;
        var labels = hourEntries.map(function (e) { return e[0] + '시'; });
        var data = hourEntries.map(function (e) { return e[1]; });
        new Chart(ctx.getContext('2d'), {
          type: 'line',
          data: {
            labels: labels,
            datasets: [
              {
                label: '시간별 트래픽 (라인)',
                data: data,
                borderColor: 'rgba(37, 99, 235, 1)',
                backgroundColor: 'rgba(37, 99, 235, 0.0)',
                tension: 0.25,
                fill: false,
                pointRadius: 2,
              },
              {
                label: '시간별 트래픽 (영역)',
                data: data,
                borderColor: 'rgba(129, 140, 248, 1)',
                backgroundColor: 'rgba(129, 140, 248, 0.25)',
                tension: 0.25,
                fill: true,
                pointRadius: 0,
              },
            ],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              legend: { display: true, labels: { boxWidth: 14 } },
            },
            scales: {
              x: { title: { display: true, text: '시간(UTC)' } },
              y: { beginAtZero: true, title: { display: true, text: '요청 수' } },
            },
          },
        });
      }

      function createDayLineChart() {
        var ctx = document.getElementById('chart-day-line');
        if (!ctx) return;
        var entries = dayEntries.slice().reverse();
        var labels = entries.map(function (e) { return e[0]; });
        var data = entries.map(function (e) { return e[1]; });
        new Chart(ctx.getContext('2d'), {
          type: 'line',
          data: {
            labels: labels,
            datasets: [
              {
                label: '일자별 트래픽',
                data: data,
                borderColor: 'rgba(34, 197, 94, 1)',
                backgroundColor: 'rgba(34, 197, 94, 0.15)',
                tension: 0.2,
                fill: true,
                pointRadius: 2,
              },
            ],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              legend: { display: true },
            },
            scales: {
              x: { title: { display: true, text: '일자' } },
              y: { beginAtZero: true, title: { display: true, text: '요청 수' } },
            },
          },
        });
      }

      function createMonthBarChart() {
        var ctx = document.getElementById('chart-month-bar');
        if (!ctx) return;
        var entries = monthEntries.slice().reverse();
        var labels = entries.map(function (e) { return e[0]; });
        var data = entries.map(function (e) { return e[1]; });
        new Chart(ctx.getContext('2d'), {
          type: 'bar',
          data: {
            labels: labels,
            datasets: [
              {
                label: '월간 트래픽',
                data: data,
                backgroundColor: 'rgba(59, 130, 246, 0.7)',
              },
            ],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              legend: { display: true },
            },
            scales: {
              x: { title: { display: true, text: '월(YYYY-MM)' } },
              y: { beginAtZero: true, title: { display: true, text: '요청 수' } },
            },
          },
        });
      }

      function colorForHeat(value, max) {
        if (!max || !value) return 'rgb(249, 250, 251)';
        var ratio = value / max;
        if (ratio < 0) ratio = 0;
        if (ratio > 1) ratio = 1;
        var baseR = 249, baseG = 250, baseB = 251;
        var targetR = 239, targetG = 68, targetB = 68;
        var r = Math.round(baseR + (targetR - baseR) * ratio);
        var g = Math.round(baseG + (targetG - baseG) * ratio);
        var b = Math.round(baseB + (targetB - baseB) * ratio);
        return 'rgb(' + r + ',' + g + ',' + b + ')';
      }

      function buildHeatmap() {
        var container = document.getElementById('traffic-heatmap');
        if (!container || !heatmap || !heatmap.length) return;
        var weekdayNamesMap = {
          ko: ['일', '월', '화', '수', '목', '금', '토'],
          ja: ['日', '月', '火', '水', '木', '金', '土'],
          en: ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
          th: ['อา', 'จ', 'อ', 'พ', 'พฤ', 'ศ', 'ส'],
          zh: ['日', '一', '二', '三', '四', '五', '六'],
        };
        var names = weekdayNamesMap[locale] || weekdayNamesMap.ko;
        var dowOrder = [1, 2, 3, 4, 5, 6, 0]; // 월~일 순서

        function makeCell(text, extraClass, bgColor) {
          var div = document.createElement('div');
          div.className = 'heatmap-cell' + (extraClass ? ' ' + extraClass : '');
          if (text) div.textContent = text;
          if (bgColor) div.style.backgroundColor = bgColor;
          return div;
        }

        container.innerHTML = '';
        container.appendChild(makeCell('', 'header', null));
        for (var h = 0; h < 24; h++) {
          container.appendChild(makeCell(h + '시', 'header', null));
        }

        for (var i = 0; i < dowOrder.length; i++) {
          var dow = dowOrder[i];
          container.appendChild(makeCell(names[dow], 'label', null));
          for (var h = 0; h < 24; h++) {
            var v = (heatmap[dow] && heatmap[dow][h]) ? heatmap[dow][h] : 0;
            var color = colorForHeat(v, heatMax);
            container.appendChild(makeCell(v ? String(v) : '', '', color));
          }
        }
      }

      createHourLineAreaChart();
      createDayLineChart();
      createMonthBarChart();
      buildHeatmap();
    })();
  </script>
</body>
</html>`);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`[${APP_ENV}] PG 노티 미들웨어 서버 listening on port ${PORT} (config: ${CONFIG_DIR})`);
  console.log('POST /noti/:merchantId');
  if (!INTERNAL_NOTI_URL) {
    console.log('INTERNAL_NOTI_URL 미설정 → 전산 전송 비활성');
  }
  startPgTransactionFetchInterval();
});
