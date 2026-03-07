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

// ========== ChillPay Transaction API (무효/환불) + 취소 가능 시간 ==========
// 기본: 한국·일본 시간 당일 22:30까지 자동화 Void, 22:31~01:30 수동, 01:30 이후 환불(자동화 가능)
const DEFAULT_VOID_CUTOFF_HOUR = 23;
const DEFAULT_VOID_CUTOFF_MINUTE = 30;
const DEFAULT_REFUND_START_HOUR = 1;
const DEFAULT_REFUND_START_MINUTE = 30;
const DEFAULT_CHILLPAY_TIMEZONE = 'Asia/Tokyo';

function loadChillPayTransactionConfig() {
  try {
    const raw = fs.readFileSync(CHILLPAY_TRANSACTION_CONFIG_PATH, 'utf8');
    const o = JSON.parse(raw);
    return {
      sandbox: (o && o.sandbox) ? { mid: String(o.sandbox.mid || '').trim(), apiKey: String(o.sandbox.apiKey || '').trim(), md5: String(o.sandbox.md5 || '').trim() } : { mid: '', apiKey: '', md5: '' },
      production: (o && o.production) ? { mid: String(o.production.mid || '').trim(), apiKey: String(o.production.apiKey || '').trim(), md5: String(o.production.md5 || '').trim() } : { mid: '', apiKey: '', md5: '' },
      voidCutoffHour: Number.isFinite(o && o.voidCutoffHour) ? o.voidCutoffHour : DEFAULT_VOID_CUTOFF_HOUR,
      voidCutoffMinute: Number.isFinite(o && o.voidCutoffMinute) ? o.voidCutoffMinute : DEFAULT_VOID_CUTOFF_MINUTE,
      refundStartHour: Number.isFinite(o && o.refundStartHour) ? o.refundStartHour : DEFAULT_REFUND_START_HOUR,
      refundStartMinute: Number.isFinite(o && o.refundStartMinute) ? o.refundStartMinute : DEFAULT_REFUND_START_MINUTE,
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
    timezone: (o && o.timezone != null && String(o.timezone).trim()) ? String(o.timezone).trim() : cur.timezone,
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
const INTERNAL_LOG_PATH = path.join(DATA_DIR, 'internal-noti.log');
const DEV_INTERNAL_LOG_PATH = path.join(DATA_DIR, 'dev-internal-noti.log');
const TEST_LOG_PATH = path.join(DATA_DIR, 'test-payments.log');
const CONFIG_LOG_PATH = path.join(DATA_DIR, 'config-change.log');
const VOID_REFUND_NOTI_LOG_PATH = path.join(DATA_DIR, 'void-refund-noti.log');

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

// Search Void Transaction: ChillPay에서 이미 무효 처리된 건 목록 조회 (수동 무효 포함)
// 문서: OrderBy + OrderDir + PageSize + PageNumber + SearchKeyword + MerchantCode + OrderNo + Status + TransactionDateFrom + TransactionDateTo + MD5 → Checksum
async function chillPaySearchVoid(useSandbox, params) {
  const cfg = loadChillPayTransactionConfig();
  const cred = useSandbox ? cfg.sandbox : cfg.production;
  const base = useSandbox ? CHILLPAY_TRANSACTION_SANDBOX_BASE : CHILLPAY_TRANSACTION_PROD_BASE;
  if (!cred.mid || !cred.apiKey || !cred.md5) {
    return { success: false, error: 'ChillPay Transaction API 미설정 (mid/apiKey/md5)', data: [] };
  }
  const orderBy = String(params.orderBy ?? '').trim();
  const orderDir = String(params.orderDir ?? '').trim();
  const pageSize = String(params.pageSize ?? '100').trim();
  const pageNumber = String(params.pageNumber ?? '1').trim();
  const searchKeyword = String(params.searchKeyword ?? '').trim();
  const merchantCode = String(cred.mid).trim();
  const orderNo = String(params.orderNo ?? '').trim();
  const status = String(params.status ?? '').trim();
  const transactionDateFrom = String(params.transactionDateFrom ?? '').trim();
  const transactionDateTo = String(params.transactionDateTo ?? '').trim();
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
  const orderBy = String(params.orderBy ?? '').trim();
  const orderDir = String(params.orderDir ?? '').trim();
  const pageSize = String(params.pageSize ?? '100').trim();
  const pageNumber = String(params.pageNumber ?? '1').trim();
  const searchKeyword = String(params.searchKeyword ?? '').trim();
  const merchantCode = String(cred.mid).trim();
  const orderNo = String(params.orderNo ?? '').trim();
  const status = String(params.status ?? '').trim();
  const transactionDateFrom = String(params.transactionDateFrom ?? '').trim();
  const transactionDateTo = String(params.transactionDateTo ?? '').trim();
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

// ChillPay에서 이미 무효 처리된 건 조회 후, 우리 로그와 매칭해 미전송 건만 무효 노티 전송 (수동 무효 동기화)
async function syncChillPayVoidNoti() {
  const cfg = loadChillPayTransactionConfig();
  const cred = cfg.useSandbox ? cfg.sandbox : cfg.production;
  if (!cred.mid || !cred.apiKey || !cred.md5) {
    return { success: false, error: 'ChillPay Transaction API 미설정', sent: 0, total: 0 };
  }
  const now = new Date();
  const toDate = new Date(now.getTime());
  const fromDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
  const transactionDateFrom = fromDate.toISOString().slice(0, 10);
  const transactionDateTo = toDate.toISOString().slice(0, 10);
  const searchResult = await chillPaySearchVoid(cfg.useSandbox, {
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
  for (const item of list) {
    const txId = item.transactionId != null ? String(item.transactionId) : (item.TransactionId != null ? String(item.TransactionId) : '');
    if (!txId || hasVoidNotiSent(txId)) continue;
    const log = NOTI_LOGS.find((l) => {
      const body = l.body && typeof l.body === 'object' ? l.body : (typeof l.body === 'string' ? (() => { try { return JSON.parse(l.body); } catch { return {}; } })() : {});
      const logTxId = body.TransactionId != null ? String(body.TransactionId) : (body.transactionId != null ? String(body.transactionId) : '');
      if (logTxId !== txId) return false;
      const isSuccess = body.PaymentStatus === 1 || body.PaymentStatus === '1' || body.paymentStatus === 'Success' || body.status === 1;
      return isSuccess && l.merchantId && MERCHANTS.get(l.merchantId);
    });
    if (!log) continue;
    try {
      await sendVoidOrRefundNoti(log, 'void');
      markVoidNotiSent(txId);
      sent += 1;
    } catch (_) {}
  }
  return { success: true, sent, total: list.length };
}

// ChillPay에서 이미 환불 처리된 건 조회 후, 우리 로그와 매칭해 미전송 건만 환불 노티 전송 (수동 환불 동기화)
async function syncChillPayRefundNoti() {
  const cfg = loadChillPayTransactionConfig();
  const cred = cfg.useSandbox ? cfg.sandbox : cfg.production;
  if (!cred.mid || !cred.apiKey || !cred.md5) {
    return { success: false, error: 'ChillPay Transaction API 미설정', sent: 0, total: 0 };
  }
  const now = new Date();
  const toDate = new Date(now.getTime());
  const fromDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
  const transactionDateFrom = fromDate.toISOString().slice(0, 10);
  const transactionDateTo = toDate.toISOString().slice(0, 10);
  const searchResult = await chillPaySearchRefund(cfg.useSandbox, {
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
  for (const item of list) {
    const txId = item.transactionId != null ? String(item.transactionId) : (item.TransactionId != null ? String(item.TransactionId) : '');
    if (!txId || hasRefundNotiSent(txId)) continue;
    const log = NOTI_LOGS.find((l) => {
      const body = l.body && typeof l.body === 'object' ? l.body : (typeof l.body === 'string' ? (() => { try { return JSON.parse(l.body); } catch { return {}; } })() : {});
      const logTxId = body.TransactionId != null ? String(body.TransactionId) : (body.transactionId != null ? String(body.transactionId) : '');
      if (logTxId !== txId) return false;
      const isSuccess = body.PaymentStatus === 1 || body.PaymentStatus === '1' || body.paymentStatus === 'Success' || body.status === 1;
      return isSuccess && l.merchantId && MERCHANTS.get(l.merchantId);
    });
    if (!log) continue;
    try {
      await sendVoidOrRefundNoti(log, 'refund');
      markRefundNotiSent(txId);
      sent += 1;
    } catch (_) {}
  }
  return { success: true, sent, total: list.length };
}

// 결제 시각 기준 자동화 구간: 'void_auto' | 'void_manual' | 'refund'
// 기준: 당일 22:30까지 void_auto, 22:31~다음날 01:29 void_manual, 01:30 이후 refund (한국·일본 시간).
function getVoidRefundWindow(paymentDateOrIso) {
  const cfg = loadChillPayTransactionConfig();
  const tz = cfg.timezone || 'Asia/Tokyo';
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
  const parts = new Intl.DateTimeFormat('en-CA', { timeZone: tz, hour: '2-digit', minute: '2-digit', hour12: false }).formatToParts(date);
  const get = (name) => (parts.find((p) => p.type === name) || {}).value || '0';
  const hour = parseInt(get('hour'), 10) || 0;
  const minute = parseInt(get('minute'), 10) || 0;
  const payMins = hour * 60 + minute;
  const cutoffMins = cfg.voidCutoffHour * 60 + cfg.voidCutoffMinute;
  const refundStartMins = cfg.refundStartHour * 60 + cfg.refundStartMinute;
  if (payMins >= refundStartMins && payMins < 120) return 'refund';
  if (payMins >= 120 && payMins <= cutoffMins) return 'void_auto';
  return 'void_manual';
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
async function sendVoidOrRefundNoti(log, type) {
  const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
  if (!merchant || !log.body) return { success: false, error: '가맹점 또는 노티 본문 없음' };
  const paymentStatus = type === 'refund' ? '9' : '2';
  const payload = typeof log.body === 'object' ? { ...log.body, PaymentStatus: paymentStatus } : { ...JSON.parse(log.body || '{}'), PaymentStatus: paymentStatus };
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
      const internalRes = await sendToInternal(internalUrl, internalPayload);
      internalOk = internalRes.success;
    } catch (_) {}
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
  appendVoidRefundNotiLog({
    type: type,
    transactionId: body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : ''),
    orderNo: body.OrderNo != null ? body.OrderNo : (body.orderNo != null ? body.orderNo : ''),
    merchantId: log.merchantId || '',
    routeNo: merchant ? (merchant.routeNo || '') : '',
    relayStatus: relayOk ? 'ok' : 'fail',
    internalStatus: internalUrl ? (internalOk ? 'ok' : 'fail') : 'skip',
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
 */
async function sendToInternal(internalUrl, payload) {
  if (!internalUrl) return { success: false, status: 0 };
  try {
    const res = await axios.post(internalUrl, payload, {
      headers: { 'Content-Type': 'application/json' },
      timeout: INTERNAL_TIMEOUT_MS,
      validateStatus: () => true,
    });
    const ok = res.status >= 200 && res.status < 300;
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

// 테스트 결제 환경 중 useTestResultPage 인 config 의 Result 노티용 routeKey: result/test_<configId>
function findTestResultByRouteKey(routeKey) {
  if (typeof routeKey !== 'string' || !routeKey.startsWith('result/test_')) return null;
  const configId = routeKey.slice('result/test_'.length);
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
        relaySuccess = relayRes.status >= 200 && relayRes.status < 300;
        if (relaySuccess) console.log('[포워딩 성공] 테스트 결과 페이지 status=', relayRes.status);
        else relayFailReason = `HTTP ${relayRes.status}`;
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
  const nav = [];
  if (can('merchants')) {
    nav.push(`<div class="nav-section-title">${t(locale, 'nav_merchant')}</div>${link('/admin/merchants', t(locale, 'nav_merchant_settings'))}`);
  }
  if (can('pg_logs') || can('internal_logs') || can('dev_internal_logs') || can('pg_result') || can('internal_result') || can('dev_result') || can('traffic_analysis')) {
    nav.push(`<div class="nav-section-title">${t(locale, 'nav_logs')}</div>`);
    if (can('pg_result')) nav.push(link('/admin/logs-result', t(locale, 'nav_pg_result') || '피지 결과'));
    if (can('internal_result')) nav.push(link('/admin/internal-result', t(locale, 'nav_internal_result') || '전산 결과'));
    if (can('dev_result')) nav.push(link('/admin/dev-internal-result', t(locale, 'nav_dev_result') || '개발 결과'));
    if (can('pg_logs')) nav.push(link('/admin/logs', t(locale, 'nav_pg_noti_log')));
    if (can('internal_logs')) nav.push(link('/admin/internal', t(locale, 'nav_internal_noti_log')));
    if (can('dev_internal_logs')) nav.push(link('/admin/dev-internal', t(locale, 'nav_dev_internal_noti_log') || '개발 노티로그'));
    if (can('traffic_analysis')) nav.push(link('/admin/traffic', t(locale, 'nav_traffic_analysis') || '트래픽분석'));
  }
  if (can('internal_targets') || can('internal_noti_settings') || can('dev_internal_noti_settings')) {
    nav.push(`<div class="nav-section-title">${t(locale, 'nav_internal')}</div>`);
    if (can('internal_targets')) nav.push(link('/admin/internal-targets', t(locale, 'nav_internal_targets')));
    if (can('internal_noti_settings')) nav.push(link('/admin/internal-noti-settings', t(locale, 'nav_internal_noti_settings')));
    if (can('dev_internal_noti_settings')) nav.push(link('/admin/dev-internal-noti-settings', t(locale, 'nav_dev_internal_noti_settings')));
  }
  if (can('cancel_refund')) {
    nav.push(`<div class="nav-section-title">${t(locale, 'nav_cancel_refund')}</div>`);
    nav.push(link('/admin/cancel-refund/cancel', t(locale, 'nav_cancel_refund_cancel')));
    nav.push(link('/admin/cancel-refund/void', t(locale, 'nav_cancel_refund_void')));
    nav.push(link('/admin/cancel-refund/force-void', t(locale, 'nav_cancel_refund_force_void')));
    nav.push(link('/admin/cancel-refund/refund', t(locale, 'nav_cancel_refund_refund')));
    nav.push(link('/admin/cancel-refund/noti', t(locale, 'nav_cancel_refund_noti')));
  }
  if (can('test_config') || can('test_run') || can('test_history')) {
    nav.push(`<div class="nav-section-title">${t(locale, 'nav_test')}</div>`);
    if (can('test_config')) nav.push(link('/admin/test-configs', t(locale, 'nav_test_config')));
    if (can('test_run')) nav.push(link('/admin/test-pay', t(locale, 'nav_test_run')));
    if (can('test_history')) nav.push(link('/admin/test-logs', t(locale, 'nav_test_history')));
  }
  if (canSeeMembers || can('settings') || can('account') || can('account_reset')) {
    nav.push(`<div class="nav-section-title">${t(locale, 'nav_system')}</div>`);
    nav.push(link('/admin/transactions', t(locale, 'nav_transaction_list')));
    if (canSeeMembers) nav.push(link('/admin/members', t(locale, 'nav_account_manage')));
    if (can('account_reset')) nav.push(link('/admin/account-reset', t(locale, 'nav_account_reset')));
    if (can('settings')) nav.push(link('/admin/settings', t(locale, 'nav_settings')));
    if (can('account')) nav.push(link('/admin/account', t(locale, 'nav_account')));
  }
  const titleText = (site.sidebarTitle || '').replace(/</g, '&lt;').replace(/"/g, '&quot;') || DEFAULT_SIDEBAR_TITLE;
  const subText = (site.sidebarSub || '').replace(/</g, '&lt;').replace(/"/g, '&quot;') || DEFAULT_SIDEBAR_SUB;
  return `
    <aside class="sidebar">
      <a href="/admin/merchants" style="color:inherit;text-decoration:none;display:block;">
        <div class="sidebar-title" style="font-size:18px;">${titleText}</div>
        <div class="sidebar-sub" style="font-size:12px;">${subText}</div>
      </a>
      <div class="sidebar-user" style="font-size:13px;">${t(locale, 'user_label')}: ${adminUser || '-'}</div>
      <div class="nav" style="font-size:14px;">${nav.join('')}</div>
      <div style="margin-top:12px;font-size:12px;color:#9ca3af;">${t(locale, 'lang_switch')}: ${langLinks}</div>
    </aside>`;
}
function getAdminTopbar(locale, clientIp, nowLocal, nowTh, adminUser, currentPath) {
  const back = currentPath || '/admin/merchants';
  const langLinks = SUPPORTED_LOCALES.map((l) => {
    const label = l === 'zh' ? 'CH' : l.toUpperCase();
    return `<a href="/admin/set-locale?lang=${l}&back=${encodeURIComponent(back)}" style="color:#0369a1;text-decoration:none;margin:0 4px;">${label}</a>`;
  }).join(' ');
  const logoutLink = '<a href="/admin/logout" style="color:#0369a1;text-decoration:none;margin-left:8px;">로그아웃</a>';
  return `<div class="topbar">
    <span>${t(locale, 'topbar_ip')}: ${clientIp || '-'}</span>
    <span>ㅣ ${t(locale, 'topbar_time')}: ${nowLocal}</span>
    <span>ㅣ ${t(locale, 'topbar_time_th')}: ${nowTh}</span>
    <span style="margin-left:auto;">${t(locale, 'lang_switch')}: ${langLinks}</span>
    <span>${t(locale, 'user_label')}: ${adminUser || '-'}</span>
    <span>${logoutLink}</span>
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
  const escAttr = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const escJs = (s) => String(s ?? '').replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\r?\n/g, ' ');
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
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
    .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; margin-bottom:0; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:18px 22px; border-radius:10px; box-shadow:0 10px 25px rgba(15,23,42,0.06); border:1px solid #e5e7eb; }
    label { display:block; margin-top:12px; font-size:14px; }
    input[type="text"] { width:100%; max-width:400px; padding:10px 12px; margin-top:4px; box-sizing:border-box; border-radius:6px; border:1px solid #d1d5db; }
    button { margin-top:16px; padding:10px 18px; background:#2563eb; color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:14px; }
    button:hover { background:#1d4ed8; }
    .hint { font-size:12px; color:#6b7280; margin-top:6px; }
    .card-chillpay { margin-top: 24px; }
    .card-chillpay h2 { margin-bottom: 8px; font-size: 1.25rem; }
    .chillpay-desc { margin-bottom: 20px; color: #4b5563; }
    .chillpay-section { margin-bottom: 28px; padding-bottom: 24px; border-bottom: 1px solid #e5e7eb; }
    .chillpay-section:last-of-type { border-bottom: none; }
    .chillpay-section h3 { margin: 0 0 8px 0; font-size: 1rem; color: #374151; }
    .chillpay-hint { font-size: 12px; color: #6b7280; margin: 0 0 12px 0; }
    .chillpay-row { display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end; margin-bottom: 0; }
    .chillpay-row-md5 { margin-top: 12px; }
    .chillpay-cell { display: flex; flex-direction: column; margin-top: 0; }
    .chillpay-cell input { max-width: none; }
    .chillpay-cell-mid { flex: 0 0 auto; width: 10em; min-width: 8em; }
    .chillpay-cell-mid input { width: 100%; }
    .chillpay-cell-api { flex: 1; min-width: 120px; }
    .chillpay-cell-md5 { flex: 1; min-width: 100px; width: 100%; }
    .chillpay-time-th { color: #2563eb; font-weight: 600; }
    .chillpay-label { font-size: 12px; font-weight: 600; color: #374151; margin-bottom: 4px; }
    .chillpay-time-grid { display: flex; flex-wrap: wrap; gap: 24px 32px; margin-bottom: 0; }
    .chillpay-time-grid .chillpay-time-row { flex: 1; min-width: 260px; margin-bottom: 0; }
    .chillpay-time-row { margin-bottom: 14px; }
    .chillpay-time-field { display: flex; flex-wrap: wrap; align-items: baseline; gap: 8px 12px; }
    .chillpay-time-field .chillpay-label { display: block; width: 100%; margin-bottom: 4px; }
    .chillpay-time-inputs input { width: 56px; padding: 6px 8px; margin: 0 2px; box-sizing: border-box; }
    .chillpay-time-desc { font-size: 11px; color: #6b7280; }
    .chillpay-sandbox-check { margin-bottom: 16px; }
    .chillpay-checkbox-wrap { display: inline-flex; align-items: center; gap: 8px; cursor: pointer; font-weight: 600; }
    .chillpay-checkbox-desc { font-size: 12px; color: #6b7280; margin: 10px 0 0 0; padding: 10px 12px; background: #f3f4f6; border-radius: 6px; line-height: 1.5; }
    .chillpay-email-fixed-note { margin-bottom: 14px; }
    .chillpay-email-sender-row { display: flex; flex-wrap: wrap; gap: 16px; align-items: flex-end; margin-bottom: 14px; }
    .chillpay-email-sender-cell { flex: 0 0 auto; width: 220px; }
    .chillpay-email-sender-cell .chillpay-label { margin-bottom: 6px; font-size: 13px; }
    .chillpay-email-sender-cell input { width: 100%; box-sizing: border-box; padding: 8px 10px; border-radius: 6px; border: 1px solid #d1d5db; font-size: 14px; }
    .chillpay-email-input-same { width: 220px; max-width: 100%; box-sizing: border-box; padding: 8px 10px; border-radius: 6px; border: 1px solid #d1d5db; font-size: 14px; }
    .chillpay-email-to-row { margin-bottom: 14px; }
    .chillpay-email-to-row .chillpay-label { margin-bottom: 6px; font-size: 13px; }
    .chillpay-email-body-row { margin-top: 4px; }
    .chillpay-email-body-row .chillpay-label { margin-bottom: 6px; font-size: 13px; }
    .chillpay-submit { margin-top: 8px; }
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
      <div class="card card-chillpay">
        <h2>ChillPay 무효/환불 (Transaction API) 및 취소 가능 시간</h2>
        <p class="hint chillpay-desc">무효·환불 API 호출에 사용할 키와, 자동 무효/수동/환불 구간을 정하는 취소 가능 시간을 설정합니다.</p>

        <form method="post" action="/admin/settings/chillpay" onsubmit="return confirm('ChillPay 설정을 저장하시겠습니까?');">
          <section class="chillpay-section">
            <h3>Sandbox (테스트 환경)</h3>
            <p class="chillpay-hint">1줄: Mid · ApiKey / 2줄: MD5</p>
            <div class="chillpay-row">
              <label class="chillpay-cell chillpay-cell-mid"><span class="chillpay-label">Mid</span><input type="text" name="sandboxMid" value="${(loadChillPayTransactionConfig().sandbox.mid || '').replace(/"/g, '&quot;')}" placeholder="MerchantCode" maxlength="20" size="10" /></label>
              <label class="chillpay-cell chillpay-cell-api"><span class="chillpay-label">ApiKey</span><input type="text" name="sandboxApiKey" value="${(loadChillPayTransactionConfig().sandbox.apiKey || '').replace(/"/g, '&quot;')}" placeholder="ApiKey" /></label>
            </div>
            <div class="chillpay-row chillpay-row-md5">
              <label class="chillpay-cell chillpay-cell-md5"><span class="chillpay-label">MD5</span><input type="text" name="sandboxMd5" value="${(loadChillPayTransactionConfig().sandbox.md5 || '').replace(/"/g, '&quot;')}" placeholder="MD5 Secret Key" /></label>
            </div>
          </section>

          <section class="chillpay-section">
            <h3>Production (운영 환경)</h3>
            <p class="chillpay-hint">1줄: Mid · ApiKey / 2줄: MD5</p>
            <div class="chillpay-row">
              <label class="chillpay-cell chillpay-cell-mid"><span class="chillpay-label">Mid</span><input type="text" name="productionMid" value="${(loadChillPayTransactionConfig().production.mid || '').replace(/"/g, '&quot;')}" placeholder="MerchantCode" maxlength="20" size="10" /></label>
              <label class="chillpay-cell chillpay-cell-api"><span class="chillpay-label">ApiKey</span><input type="text" name="productionApiKey" value="${(loadChillPayTransactionConfig().production.apiKey || '').replace(/"/g, '&quot;')}" placeholder="ApiKey" /></label>
            </div>
            <div class="chillpay-row chillpay-row-md5">
              <label class="chillpay-cell chillpay-cell-md5"><span class="chillpay-label">MD5</span><input type="text" name="productionMd5" value="${(loadChillPayTransactionConfig().production.md5 || '').replace(/"/g, '&quot;')}" placeholder="MD5 Secret Key" /></label>
            </div>
          </section>

          <section class="chillpay-section">
            <h3>취소 가능 시간 (기준: 일본 시간)</h3>
            <p class="chillpay-hint">모든 시각은 <strong>일본 시간(JST)</strong> 기준입니다. 동일 시각의 <strong class="chillpay-time-th">태국 시간(ICT)</strong>은 일본보다 2시간 느립니다 (예: 일본 23:30 = <span class="chillpay-time-th">태국 21:30</span>).</p>
            <div class="chillpay-time-grid">
              <div class="chillpay-time-row">
                <label class="chillpay-time-field">
                  <span class="chillpay-label">자동 무효 마감 (당일)</span>
                  <span class="chillpay-time-inputs"><input type="number" name="voidCutoffHour" min="0" max="23" value="${loadChillPayTransactionConfig().voidCutoffHour}" /> 시 <input type="number" name="voidCutoffMinute" min="0" max="59" value="${loadChillPayTransactionConfig().voidCutoffMinute}" /> 분</span>
                  <span class="chillpay-time-desc">일본 기준. 기본 23:30 (<span class="chillpay-time-th">태국 21:30</span>)</span>
                </label>
              </div>
              <div class="chillpay-time-row">
                <label class="chillpay-time-field">
                  <span class="chillpay-label">환불 구간 시작 (다음날)</span>
                  <span class="chillpay-time-inputs"><input type="number" name="refundStartHour" min="0" max="23" value="${loadChillPayTransactionConfig().refundStartHour}" /> 시 <input type="number" name="refundStartMinute" min="0" max="59" value="${loadChillPayTransactionConfig().refundStartMinute}" /> 분</span>
                  <span class="chillpay-time-desc">일본 기준. 기본 01:30 (<span class="chillpay-time-th">태국 전날 23:30</span>)</span>
                </label>
              </div>
            </div>
            <input type="hidden" name="timezone" value="Asia/Tokyo" />
          </section>

          <section class="chillpay-section">
            <h3>수동 무효 이메일 (ChillPay 취소 요청)</h3>
            <p class="chillpay-hint chillpay-email-fixed-note">보내는 정보와 받는 분은 한 번 저장하면 고정값으로 사용됩니다. 아래 <strong>내용</strong>만 건마다 자동 치환됩니다.</p>
            <div class="chillpay-row chillpay-email-sender-row">
              <label class="chillpay-cell chillpay-email-sender-cell"><span class="chillpay-label">보내는 사람 주소</span><input type="email" name="emailFrom" value="${(loadChillPayTransactionConfig().emailFrom || '').replace(/"/g, '&quot;')}" placeholder="발신 이메일" /></label>
              <label class="chillpay-cell chillpay-email-sender-cell"><span class="chillpay-label">회사명</span><input type="text" name="companyName" value="${(loadChillPayTransactionConfig().companyName || '').replace(/"/g, '&quot;')}" placeholder="회사명" /></label>
              <label class="chillpay-cell chillpay-email-sender-cell"><span class="chillpay-label">담당자 성명</span><input type="text" name="contactName" value="${(loadChillPayTransactionConfig().contactName || '').replace(/"/g, '&quot;')}" placeholder="담당자 성명" /></label>
            </div>
            <div class="chillpay-email-to-row">
              <label class="chillpay-cell"><span class="chillpay-label">받는 분 (수신 이메일 주소)</span><input type="email" name="emailTo" class="chillpay-email-input-same" value="${(loadChillPayTransactionConfig().emailTo || 'help@chillpay.co.th').replace(/"/g, '&quot;')}" placeholder="help@chillpay.co.th" /></label>
            </div>
            <div class="chillpay-email-body-row">
              <label class="chillpay-cell" style="width:100%;"><span class="chillpay-label">내용 (본문 템플릿 · {{transNo}}, {{orderNo}}, {{amount}}, {{routeNo}}, {{paymentDate}}, {{mid}} 건마다 자동 치환)</span><textarea name="emailBodyTemplate" rows="5" style="width:100%;max-width:600px;padding:8px;border-radius:6px;border:1px solid #d1d5db;box-sizing:border-box;">${(loadChillPayTransactionConfig().emailBodyTemplate || '아래 거래에 대해 취소를 요청합니다.\n\nTransactionId(transNo): {{transNo}}\nOrderNo: {{orderNo}}\nAmount: {{amount}}\nRoute No. {{routeNo}}\nPaymentDate: {{paymentDate}}\nMID: {{mid}}\n').replace(/</g, '&lt;').replace(/"/g, '&quot;')}</textarea></label>
            </div>
          </section>

          <section class="chillpay-section chillpay-sandbox-check">
            <label class="chillpay-checkbox-wrap">
              <input type="checkbox" name="useSandbox" ${loadChillPayTransactionConfig().useSandbox ? 'checked' : ''} />
              <span>API 호출 시 Sandbox 사용 (테스트 시 체크)</span>
            </label>
            <p class="chillpay-checkbox-desc">체크하면 무효/환불 요청이 <strong>Sandbox(테스트)</strong> 환경으로 전송됩니다. 실제 결제 건에는 적용되지 않습니다. 해제하면 <strong>Production(운영)</strong> 환경으로 전송되어 실제 무효/환불이 처리됩니다.</p>
          </section>

          <div class="chillpay-submit"><button type="submit">ChillPay 설정 저장</button></div>
        </form>
      </div>
    </main>
  </div>
</body>
</html>`);
});

app.post('/admin/settings/chillpay', requireAuth, requirePage('settings'), (req, res) => {
  const sandboxMid = (req.body && req.body.sandboxMid != null) ? String(req.body.sandboxMid).trim() : '';
  const sandboxApiKey = (req.body && req.body.sandboxApiKey != null) ? String(req.body.sandboxApiKey).trim() : '';
  const sandboxMd5 = (req.body && req.body.sandboxMd5 != null) ? String(req.body.sandboxMd5).trim() : '';
  const productionMid = (req.body && req.body.productionMid != null) ? String(req.body.productionMid).trim() : '';
  const productionApiKey = (req.body && req.body.productionApiKey != null) ? String(req.body.productionApiKey).trim() : '';
  const productionMd5 = (req.body && req.body.productionMd5 != null) ? String(req.body.productionMd5).trim() : '';
  const timezone = (req.body && req.body.timezone != null) ? String(req.body.timezone).trim() : 'Asia/Tokyo';
  const voidCutoffHour = parseInt(req.body.voidCutoffHour, 10);
  const voidCutoffMinute = parseInt(req.body.voidCutoffMinute, 10);
  const refundStartHour = parseInt(req.body.refundStartHour, 10);
  const refundStartMinute = parseInt(req.body.refundStartMinute, 10);
  const useSandbox = req.body.useSandbox === 'on' || req.body.useSandbox === true;
  const emailFrom = (req.body && req.body.emailFrom != null) ? String(req.body.emailFrom).trim() : '';
  const companyName = (req.body && req.body.companyName != null) ? String(req.body.companyName).trim() : '';
  const contactName = (req.body && req.body.contactName != null) ? String(req.body.contactName).trim() : '';
  const emailTo = (req.body && req.body.emailTo != null) ? String(req.body.emailTo).trim() : 'help@chillpay.co.th';
  const emailBodyTemplate = (req.body && req.body.emailBodyTemplate != null) ? String(req.body.emailBodyTemplate).trim() : '';
  saveChillPayTransactionConfig({
    sandbox: { mid: sandboxMid, apiKey: sandboxApiKey, md5: sandboxMd5 },
    production: { mid: productionMid, apiKey: productionApiKey, md5: productionMd5 },
    timezone: timezone || 'Asia/Tokyo',
    voidCutoffHour: Number.isFinite(voidCutoffHour) ? voidCutoffHour : 23,
    voidCutoffMinute: Number.isFinite(voidCutoffMinute) ? voidCutoffMinute : 30,
    refundStartHour: Number.isFinite(refundStartHour) ? refundStartHour : 1,
    refundStartMinute: Number.isFinite(refundStartMinute) ? refundStartMinute : 30,
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
  cancel_refund: '취소환불',
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
    .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
    .nav a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
            <form method="post" action="/admin/merchants/delete" onsubmit="return confirm('${confirmDel}');" style="margin:0;">
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
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
          <input type="text" name="resultUrl" />
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
  const resendMsg =
    q.resend === 'ok'
      ? '<div class="alert alert-ok">재전송이 완료되었습니다.</div>'
      : q.resend === 'fail' && q.reason
      ? '<div class="alert alert-fail">재전송 실패: ' + escQ(q.reason) + '</div>'
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

  const reversed = [...NOTI_LOGS].slice().reverse();
  const rows = reversed
    .map((log, i) => {
      const realIndex = NOTI_LOGS.length - 1 - i;
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
      const canResend = (relayStatus === 'fail' || relayStatus === 'ok') && relayHasTarget && (log.body || log.rawBody);
      const resendBtn = canResend
        ? `<div class="resend-wrap"><form method="post" action="/admin/logs/resend" style="display:inline;" onsubmit="return confirm('일반(원문) 형식으로 다시 전송하시겠습니까?');"><input type="hidden" name="index" value="${realIndex}" /><button type="submit" class="btn-resend">일반</button></form><form method="post" action="/admin/logs/resend" style="display:inline;" onsubmit="return confirm('JSON 형식으로 다시 전송하시겠습니까?');"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="resendAsJson" value="1" /><button type="submit" class="btn-resend-json">JSON</button></form><form method="post" action="/admin/logs/resend" style="display:inline;" onsubmit="return confirm('FORM 형식으로 다시 전송하시겠습니까?');"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="resendAsForm" value="1" /><button type="submit" class="btn-resend-form">FORM</button></form></div>`
        : !relayHasTarget
        ? '<span class="label-none">노티없음</span>'
        : '-';
      const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
      const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : null);
      const isSuccess = body.PaymentStatus === 1 || body.PaymentStatus === '1' || body.paymentStatus === 'Success' || body.status === 1;
      const payDate = body.PaymentDate || body.paymentDate || log.receivedAtIso || log.receivedAt;
      const windowType = txId && isSuccess && log.merchantId && MERCHANTS.get(log.merchantId) ? getVoidRefundWindow(payDate) : null;
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
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
      <h1>${t(locale, 'pg_logs_title')} (${NOTI_LOGS.length})</h1>
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
  const result = await chillPayRequestVoid(txId, cfg.useSandbox);
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
  const result = await chillPayRequestRefund(txId, cfg.useSandbox);
  if (!result.success) return res.redirect('/admin/logs?refund=fail&reason=' + encodeURIComponent(result.error || '환불 요청 실패'));
  const sendResult = await sendVoidOrRefundNoti(log, 'refund');
  markRefundNotiSent(txId);
  return res.redirect('/admin/logs?refund=ok' + (sendResult.success ? '' : '&noti=partial'));
});

// ----- 취소환불 전용 메뉴 (거래내역 / 무효거래 / 환불거래 / 거래노티) -----
const cancelRefundLayoutCss = `
  body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; }
  h1 { margin-bottom: 8px; }
  table { border-collapse: collapse; width: 100%; background:#fff; table-layout: fixed; }
  th, td { border: 1px solid #e5e7eb; padding: 8px 10px; font-size: 13px; vertical-align: middle; text-align: center; }
  th { background: #e5f0ff; }
  tr:nth-child(even) { background:#f9fafb; }
  .tx-status-fail { color: #dc2626; font-weight: 600; }
  .tx-status-success { color: #2563eb; font-weight: 600; }
  .tx-status-cancel { color: #059669; font-weight: 600; }
  .tx-status-void-manual { color: #7c3aed; font-weight: 600; }
  .tx-status-refund-manual { color: #84cc16; font-weight: 600; }
  .col-date { width: 10%; min-width: 70px; }
  .col-time, .col-narrow { width: 9%; min-width: 75px; }
  .col-action { width: 12%; min-width: 90px; }
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
  .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
  .sidebar { width:195px; flex-shrink:0; background:#111827; padding:6px 12px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
  .sidebar-title { font-weight:700; margin-bottom:1px; color:#f9fafb; font-size:18px; }
  .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:4px; }
  .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:6px; padding:4px 8px; background:#1f2937; border-radius:6px; }
  .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:3px 4px 1px; text-transform:uppercase; letter-spacing:0.08em; }
  .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
  .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
  .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
  .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
  .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:8px; border:1px solid #e5e7eb; }
`;

function renderCancelRefundPage(locale, adminUser, title, mainContent, alertHtml, currentUrl, member) {
  const clientIp = '';
  const nowKr = '';
  const nowTh = '';
  const raw = mainContent && String(mainContent).trim() ? mainContent : '';
  const isFullMarkup = raw.indexOf('<table') === 0 || raw.indexOf('<form') === 0 || raw.includes('</table>');
  const noDataLabel = t(locale, 'cr_no_data');
  const mainInner = raw || '<table><tbody><tr><td colspan="6" style="text-align:center;color:#777;">' + noDataLabel + '</td></tr></tbody></table>';
  const wrapped = isFullMarkup ? mainInner : '<table>' + mainInner + '</table>';
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
      ${getAdminTopbar(locale, clientIp, nowKr, nowTh, adminUser, '')}
      <div class="card">
        ${alertHtml || ''}
        <h1>${title}</h1>
        ${wrapped}
      </div>
    </main>
  </div>
</body>
</html>`;
}

// ----- 시스템 > 거래내역 (지정 컬럼만 표시) -----
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
const TRANSACTION_LIST_COLUMNS = [
  { type: 'fixed', id: 'received_date' },
  { type: 'fixed', id: 'received_time' },
  { type: 'fixed', id: 'route_no' },
  { type: 'fixed', id: 'merchant' },
  { type: 'body', keys: ['TransactionId', 'transactionId'] },
  { type: 'body', keys: ['OrderNo', 'orderNo'] },
  { type: 'body', keys: ['Amount', 'amount'] },
  { type: 'body', keys: ['status'] },
  { type: 'body', keys: ['PaymentDate', 'paymentDate'] },
  { type: 'body', keys: ['Currency', 'currency'], display: 'currency' },
  { type: 'body', keys: ['CustomerId', 'customerId'] },
  { type: 'body', keys: ['PaymentDescription', 'paymentDescription', 'Description', 'description'] },
  { type: 'fixed', id: 'status' },
  { type: 'fixed', id: 'noti' },
];
app.get('/admin/transactions', requireAuth, (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const reversed = [...NOTI_LOGS].slice().reverse();
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
    t(locale, 'cr_th_received_date'),
    t(locale, 'cr_th_received_time'),
    t(locale, 'cr_th_route_no'),
    t(locale, 'cr_th_merchant'),
    'TransactionId', 'OrderNo', 'Amount', 'status', 'PaymentDate', 'Currency',
    'CustomerId', 'Description',
    t(locale, 'tx_th_status'),
    t(locale, 'tx_th_noti'),
  ];
  const thead = '<thead><tr>' + thLabels.map((l) => '<th class="col-body-key">' + esc(l) + '</th>').join('') + '</tr></thead>';
  const rows = reversed.map((log) => {
    const body = parseNotiBody(log);
    const ps = body.PaymentStatus ?? body.paymentStatus ?? body.status;
    const isCancel = ps === 0 || ps === '0' || ps === 'Cancel' || ps === 'Canceled' || ps === 'Cancelled' || String(ps).toLowerCase() === 'cancel';
    const isSuccess = ps === 1 || ps === '1' || body.paymentStatus === 'Success' || body.status === 1;
    let statusKey = 'tx_status_fail';
    let statusClass = 'tx-status-fail';
    if (isCancel) {
      statusKey = 'tx_status_cancel';
      statusClass = 'tx-status-cancel';
    } else if (isSuccess) {
      const payDate = body.PaymentDate || body.paymentDate || log.receivedAtIso || log.receivedAt;
      const w = getVoidRefundWindow(payDate);
      if (w === 'void_auto') {
        statusKey = 'tx_status_void_auto';
        statusClass = 'tx-status-success';
      } else if (w === 'void_manual') {
        statusKey = 'tx_status_void_manual';
        statusClass = 'tx-status-void-manual';
      } else {
        statusKey = 'tx_status_refund_auto';
        statusClass = 'tx-status-refund-manual';
      }
    }
    const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : '');
    let notiLabel = '-';
    if (txId) {
      if (hasVoidNotiSent(txId)) notiLabel = t(locale, 'tx_status_force_void');
      else if (hasRefundNotiSent(txId)) notiLabel = t(locale, 'tx_status_force_refund');
    }
    const dt = formatDateAndTimeTHJP(log.receivedAtIso || log.receivedAt);
    const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
    const routeNoDisplay = getRouteNoDisplay(merchant, log.routeKey);
    const cells = [];
    for (const col of TRANSACTION_LIST_COLUMNS) {
      if (col.type === 'fixed') {
        if (col.id === 'received_date') cells.push('<td class="col-date">' + esc(dt.date) + '</td>');
        else if (col.id === 'received_time') cells.push('<td class="col-time">TH: ' + esc(dt.timeTh) + '<br><span class="time-jp">JP: ' + esc(dt.timeJp) + '</span></td>');
        else if (col.id === 'route_no') cells.push('<td class="col-narrow">' + esc(routeNoDisplay) + '</td>');
        else if (col.id === 'merchant') cells.push('<td class="col-narrow">' + esc(log.merchantId || '') + '</td>');
        else if (col.id === 'status') cells.push('<td class="col-narrow ' + statusClass + '">' + esc(t(locale, statusKey)) + '</td>');
        else if (col.id === 'noti') cells.push('<td class="col-narrow">' + esc(notiLabel) + '</td>');
      } else {
        let v = getBodyVal(body, col.keys);
        if (col.display === 'currency') v = formatCurrencyForDisplay(v) ?? v;
        cells.push('<td class="col-body-val" style="max-width:140px;word-break:break-all;font-size:11px;">' + formatVal(v) + '</td>');
      }
    }
    return '<tr>' + cells.join('') + '</tr>';
  }).join('');
  const tableContent = '<table>' + thead + '<tbody>' + rows + '</tbody></table>';
  res.send(renderCancelRefundPage(locale, adminUser, t(locale, 'nav_transaction_list') + ' (' + reversed.length + ')', tableContent, '', req.originalUrl, req.session.member));
});

app.get('/admin/cancel-refund/cancel', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const reversed = [...NOTI_LOGS].slice().reverse();
  const cancelled = reversed.filter((log) => {
    const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
    const ps = body.PaymentStatus ?? body.paymentStatus ?? body.status;
    return ps === 0 || ps === '0' || ps === 'Cancel' || ps === 'Canceled' || ps === 'Cancelled' || String(ps).toLowerCase() === 'cancel';
  });
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const rows = cancelled.map((log, i) => {
    const realIndex = NOTI_LOGS.length - 1 - reversed.indexOf(log);
    const dt = formatDateAndTimeTHJP(log.receivedAtIso || log.receivedAt);
    const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
    const txId = body.TransactionId ?? body.transactionId ?? '-';
    const orderNo = body.OrderNo ?? body.orderNo ?? '-';
    const amount = body.Amount ?? body.amount ?? '-';
    const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
    const routeNoDisplay = getRouteNoDisplay(merchant, log.routeKey);
    return `<tr>
      <td class="col-date">${esc(dt.date)}</td>
      <td class="col-time">TH: ${esc(dt.timeTh)}<br><span class="time-jp">JP: ${esc(dt.timeJp)}</span></td>
      <td class="col-narrow">${esc(routeNoDisplay)}</td>
      <td class="col-narrow">${esc(log.merchantId || '')}</td>
      <td>${esc(txId)} / ${esc(orderNo)} / ${esc(amount)}</td>
    </tr>`;
  }).join('');
  const thead = '<thead><tr><th>수신일</th><th>수신시각</th><th>Route No.</th><th>가맹점</th><th>TransactionId / OrderNo / Amount</th></tr></thead>';
  res.send(renderCancelRefundPage(locale, adminUser, t(locale, 'nav_cancel_refund_cancel') + ' (' + cancelled.length + ')', thead + '<tbody>' + rows + '</tbody>', '', req.originalUrl, req.session.member));
});

app.get('/admin/cancel-refund/noti', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const q = req.query || {};
  const typeFilter = (q.type || 'all').toString();
  const days = parseInt(q.days, 10) || 30;
  const entries = loadVoidRefundNotiLog(days);
  const filtered = typeFilter === 'void' ? entries.filter((e) => e.type === 'void') : typeFilter === 'refund' ? entries.filter((e) => e.type === 'refund') : entries;
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const relayLabel = (s) => (s === 'ok' ? '<span class="status-ok">' + t(locale, 'cr_status_ok') + '</span>' : s === 'fail' ? '<span class="status-fail">' + t(locale, 'cr_status_fail') + '</span>' : '-');
  const internalLabel = (s) => (s === 'ok' ? '<span class="status-ok">' + t(locale, 'cr_status_ok') + '</span>' : s === 'fail' ? '<span class="status-fail">' + t(locale, 'cr_status_fail') + '</span>' : s === 'skip' ? '<span class="status-skip">' + t(locale, 'cr_status_skip') + '</span>' : '-');
  const rows = filtered.map((e) => {
    const dt = e.sentAtIso ? new Date(e.sentAtIso).toLocaleString('ko-KR', { hour12: false }) : '-';
    const typeLabel = e.type === 'void' ? t(locale, 'cr_type_void') : e.type === 'refund' ? t(locale, 'cr_type_refund') : e.type || '-';
    return `<tr>
      <td class="col-date">${esc(dt)}</td>
      <td class="col-narrow">${esc(typeLabel)}</td>
      <td class="col-narrow">${esc(e.transactionId || '')}</td>
      <td class="col-narrow">${esc(e.orderNo || '')}</td>
      <td class="col-narrow">${esc(e.merchantId || '')}</td>
      <td class="col-narrow">${esc(e.routeNo || '')}</td>
      <td class="col-status">${relayLabel(e.relayStatus)}</td>
      <td class="col-status">${internalLabel(e.internalStatus)}</td>
    </tr>`;
  }).join('');
  const filterLinks = `<div style="margin-bottom:12px;"><span style="font-size:13px;color:#374151;">${t(locale, 'cr_filter_type')}: </span>
    <a href="/admin/cancel-refund/noti?type=all&days=${days}" style="padding:6px 12px;font-size:13px;border-radius:6px;text-decoration:none;background:${typeFilter === 'all' ? '#2563eb' : '#e5e7eb'};color:${typeFilter === 'all' ? '#fff' : '#374151'};">${t(locale, 'cr_filter_all')}</a>
    <a href="/admin/cancel-refund/noti?type=void&days=${days}" style="padding:6px 12px;font-size:13px;border-radius:6px;text-decoration:none;background:${typeFilter === 'void' ? '#2563eb' : '#e5e7eb'};color:${typeFilter === 'void' ? '#fff' : '#374151'};">${t(locale, 'cr_type_void')}</a>
    <a href="/admin/cancel-refund/noti?type=refund&days=${days}" style="padding:6px 12px;font-size:13px;border-radius:6px;text-decoration:none;background:${typeFilter === 'refund' ? '#2563eb' : '#e5e7eb'};color:${typeFilter === 'refund' ? '#fff' : '#374151'};">${t(locale, 'cr_type_refund')}</a>
    <span style="margin-left:12px;font-size:13px;">${t(locale, 'cr_period_recent')} ${days}${t(locale, 'cr_days')}</span>
    <a href="/admin/cancel-refund/noti?type=${typeFilter}&days=7" style="margin-left:6px;font-size:12px;">7${t(locale, 'cr_days')}</a>
    <a href="/admin/cancel-refund/noti?type=${typeFilter}&days=30" style="margin-left:4px;font-size:12px;">30${t(locale, 'cr_days')}</a>
    <a href="/admin/cancel-refund/noti?type=${typeFilter}&days=90" style="margin-left:4px;font-size:12px;">90${t(locale, 'cr_days')}</a>
  </div>`;
  const thead = '<thead><tr><th>' + t(locale, 'cr_th_sent_at') + '</th><th>' + t(locale, 'cr_th_type') + '</th><th>TransactionId</th><th>OrderNo</th><th>' + t(locale, 'cr_th_merchant') + '</th><th>' + t(locale, 'cr_th_route_no') + '</th><th>' + t(locale, 'cr_th_merchant_receive') + '</th><th>' + t(locale, 'cr_th_internal_receive') + '</th></tr></thead>';
  const tableContent = filterLinks + '<table>' + thead + '<tbody>' + rows + '</tbody></table>';
  res.send(renderCancelRefundPage(locale, adminUser, t(locale, 'nav_cancel_refund_noti') + ' (' + filtered.length + ')', tableContent, '', req.originalUrl, req.session.member));
});

app.get('/admin/cancel-refund/void', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const q = req.query || {};
  const escQ = (s) => (s ? String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '');
  let alertHtml = q.void === 'ok' ? '<div class="alert alert-ok">' + t(locale, 'cr_alert_void_ok') + '</div>' : q.void === 'fail' && q.reason ? '<div class="alert alert-fail">' + t(locale, 'cr_alert_void_fail') + ': ' + escQ(q.reason) + '</div>' : '';
  if (q.sync === 'ok') {
    const sent = parseInt(q.sent, 10) || 0;
    const total = parseInt(q.total, 10) || 0;
    alertHtml += '<div class="alert alert-ok">' + t(locale, 'cr_alert_sync_void_ok').replace(/\{\{total\}\}/g, total).replace(/\{\{sent\}\}/g, sent) + '</div>';
  } else if (q.sync === 'fail' && q.reason) {
    alertHtml += '<div class="alert alert-fail">' + t(locale, 'cr_alert_sync_fail') + ': ' + escQ(q.reason) + '</div>';
  }
  const confirmSyncVoid = (t(locale, 'cr_confirm_sync_void') || '').replace(/'/g, "\\'");
  const syncForm = '<form method="post" action="/admin/cancel-refund/sync-void" style="margin-bottom:14px;" onsubmit="return confirm(\'' + confirmSyncVoid + '\');"><button type="submit" class="btn-email">' + t(locale, 'cr_btn_sync_void') + '</button></form>';
  const reversed = [...NOTI_LOGS].slice().reverse();
  const voidList = reversed.filter((log) => {
    const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
    const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : null);
    const isSuccess = body.PaymentStatus === 1 || body.PaymentStatus === '1' || body.paymentStatus === 'Success' || body.status === 1;
    if (!txId || !isSuccess || !log.merchantId || !MERCHANTS.get(log.merchantId)) return false;
    const payDate = body.PaymentDate || body.paymentDate || log.receivedAtIso || log.receivedAt;
    const w = getVoidRefundWindow(payDate);
    return w === 'void_auto' || w === 'void_manual';
  });
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const cfg = loadChillPayTransactionConfig();
  const rows = voidList.map((log) => {
    const realIndex = NOTI_LOGS.indexOf(log);
    const dt = formatDateAndTimeTHJP(log.receivedAtIso || log.receivedAt);
    const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
    const payDate = body.PaymentDate || body.paymentDate || log.receivedAtIso || log.receivedAt;
    const windowType = getVoidRefundWindow(payDate);
    const txId = body.TransactionId ?? body.transactionId ?? '-';
    const orderNo = body.OrderNo ?? body.orderNo ?? '-';
    const amount = body.Amount ?? body.amount ?? '-';
    const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
    const routeNoDisplay = getRouteNoDisplay(merchant, log.routeKey);
    const confirmVoid = (t(locale, 'cr_confirm_void') || '').replace(/'/g, "\\'");
    let action = '';
    if (windowType === 'void_auto') {
      action = `<form method="post" action="/admin/cancel-refund/void-request" style="display:inline;" onsubmit="return confirm('${confirmVoid}');"><input type="hidden" name="index" value="${realIndex}" /><button type="submit" class="btn-void">${t(locale, 'cr_btn_void_request')}</button></form>`;
    } else {
      const manualFrom = cfg.voidCutoffHour + ':' + String(cfg.voidCutoffMinute).padStart(2, '0');
      const manualTo = cfg.refundStartHour + ':' + String(cfg.refundStartMinute).padStart(2, '0');
      const manualWindowTip = (t(locale, 'cr_manual_email_window') || '') + ': ' + manualFrom + '~' + manualTo;
      if (isCurrentTimeInVoidManualWindow()) {
        const { subject, body: bodyText } = buildVoidEmailContent(log);
        const emailTo = (cfg.emailTo || 'help@chillpay.co.th').trim();
        const mailto = 'mailto:' + encodeURIComponent(emailTo) + '?subject=' + encodeURIComponent(subject) + '&body=' + encodeURIComponent(bodyText);
        action = `<a href="${mailto}" class="btn-email">${t(locale, 'cr_btn_email_send')}</a>`;
      } else {
        action = `<span class="btn-email-disabled" title="${manualWindowTip}">${t(locale, 'cr_btn_email_disabled')}</span>`;
      }
    }
    return `<tr>
      <td class="col-date">${esc(dt.date)}</td>
      <td class="col-time">TH: ${esc(dt.timeTh)}<br><span class="time-jp">JP: ${esc(dt.timeJp)}</span></td>
      <td class="col-narrow">${esc(routeNoDisplay)}</td>
      <td class="col-narrow">${esc(log.merchantId || '')}</td>
      <td>${esc(txId)} / ${esc(orderNo)} / ${esc(amount)}</td>
      <td class="col-action">${action}</td>
    </tr>`;
  }).join('');
  const thead = '<thead><tr><th>' + t(locale, 'cr_th_received_date') + '</th><th>' + t(locale, 'cr_th_received_time') + '</th><th>' + t(locale, 'cr_th_route_no') + '</th><th>' + t(locale, 'cr_th_merchant') + '</th><th>' + t(locale, 'cr_th_tx_order_amount') + '</th><th>' + t(locale, 'cr_th_action') + '</th></tr></thead>';
  const tableContent = syncForm + '<table>' + thead + '<tbody>' + rows + '</tbody></table>';
  res.send(renderCancelRefundPage(locale, adminUser, t(locale, 'nav_cancel_refund_void') + ' (' + voidList.length + ')', tableContent, alertHtml, req.originalUrl, req.session.member));
});

app.get('/admin/cancel-refund/force-void', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const q = req.query || {};
  const escQ = (s) => (s ? String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '');
  let alertHtml = q.ok === '1' ? '<div class="alert alert-ok">' + t(locale, 'cr_alert_force_void_ok') + '</div>' : (q.fail === '1' && q.reason ? '<div class="alert alert-fail">' + escQ(q.reason) + '</div>' : '');
  const reversed = [...NOTI_LOGS].slice().reverse();
  const forceVoidList = reversed.filter((log) => {
    const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
    const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : null);
    const isSuccess = body.PaymentStatus === 1 || body.PaymentStatus === '1' || body.paymentStatus === 'Success' || body.status === 1;
    if (!txId || !isSuccess || !log.merchantId || !MERCHANTS.get(log.merchantId)) return false;
    const payDate = body.PaymentDate || body.paymentDate || log.receivedAtIso || log.receivedAt;
    const w = getVoidRefundWindow(payDate);
    return w === 'void_auto' || w === 'void_manual';
  });
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const confirmForceVoid = (t(locale, 'cr_confirm_force_void') || '').replace(/'/g, "\\'").replace(/"/g, '&quot;');
  const rows = forceVoidList.map((log) => {
    const realIndex = NOTI_LOGS.indexOf(log);
    const dt = formatDateAndTimeTHJP(log.receivedAtIso || log.receivedAt);
    const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
    const txId = body.TransactionId ?? body.transactionId ?? '-';
    const orderNo = body.OrderNo ?? body.orderNo ?? '-';
    const amount = body.Amount ?? body.amount ?? '-';
    const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
    const routeNoDisplay = getRouteNoDisplay(merchant, log.routeKey);
    const action = '<form method="post" action="/admin/cancel-refund/force-void-request" style="display:inline;" onsubmit="return confirm(\'' + confirmForceVoid + '\');"><input type="hidden" name="index" value="' + realIndex + '" /><button type="submit" class="btn-void">' + t(locale, 'cr_btn_force_void') + '</button></form>';
    return `<tr>
      <td class="col-date">${esc(dt.date)}</td>
      <td class="col-time">TH: ${esc(dt.timeTh)}<br><span class="time-jp">JP: ${esc(dt.timeJp)}</span></td>
      <td class="col-narrow">${esc(routeNoDisplay)}</td>
      <td class="col-narrow">${esc(log.merchantId || '')}</td>
      <td>${esc(txId)} / ${esc(orderNo)} / ${esc(amount)}</td>
      <td class="col-action">${action}</td>
    </tr>`;
  }).join('');
  const descHtml = '<p class="perm-legend" style="margin-bottom:12px;">' + (t(locale, 'cr_force_void_desc') || '').replace(/</g, '&lt;') + '</p>';
  const thead = '<thead><tr><th>' + t(locale, 'cr_th_received_date') + '</th><th>' + t(locale, 'cr_th_received_time') + '</th><th>' + t(locale, 'cr_th_route_no') + '</th><th>' + t(locale, 'cr_th_merchant') + '</th><th>' + t(locale, 'cr_th_tx_order_amount') + '</th><th>' + t(locale, 'cr_th_action') + '</th></tr></thead>';
  const tableContent = descHtml + '<table>' + thead + '<tbody>' + rows + '</tbody></table>';
  res.send(renderCancelRefundPage(locale, adminUser, t(locale, 'nav_cancel_refund_force_void') + ' (' + forceVoidList.length + ')', tableContent, alertHtml, req.originalUrl, req.session.member));
});

app.post('/admin/cancel-refund/sync-void', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const locale = getLocale(req);
  try {
    const result = await syncChillPayVoidNoti();
    if (result.success) {
      return res.redirect('/admin/cancel-refund/void?sync=ok&sent=' + (result.sent || 0) + '&total=' + (result.total || 0));
    }
    return res.redirect('/admin/cancel-refund/void?sync=fail&reason=' + encodeURIComponent(result.error || t(locale, 'cr_alert_sync_fail')));
  } catch (err) {
    const msg = err && err.message ? err.message : String(err);
    return res.redirect('/admin/cancel-refund/void?sync=fail&reason=' + encodeURIComponent(msg));
  }
});

app.get('/admin/cancel-refund/refund', requireAuth, requirePage('cancel_refund'), (req, res) => {
  const locale = getLocale(req);
  const adminUser = req.session.adminUser || '';
  const q = req.query || {};
  const escQ = (s) => (s ? String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '');
  let alertHtml = q.refund === 'ok' ? '<div class="alert alert-ok">환불 요청이 완료되었습니다.</div>' : q.refund === 'fail' && q.reason ? '<div class="alert alert-fail">환불 요청 실패: ' + escQ(q.reason) + '</div>' : '';
  if (q.sync === 'ok') {
    const sent = parseInt(q.sent, 10) || 0;
    const total = parseInt(q.total, 10) || 0;
    alertHtml += '<div class="alert alert-ok">ChillPay 환불 건 동기화 완료. 조회 ' + total + '건 중 환불 노티 전송 ' + sent + '건.</div>';
  } else if (q.sync === 'fail' && q.reason) {
    alertHtml += '<div class="alert alert-fail">동기화 실패: ' + escQ(q.reason) + '</div>';
  }
  const syncForm = '<form method="post" action="/admin/cancel-refund/sync-refund" style="margin-bottom:14px;" onsubmit="return confirm(\'ChillPay에서 최근 7일 환불 처리된 건을 조회하고, 아직 환불 노티를 보내지 않은 건만 가맹점/전산에 전송합니다. 진행할까요?\');"><button type="submit" class="btn-email">ChillPay 환불 건 동기화 (조회 후 미전송만 노티)</button></form>';
  const reversed = [...NOTI_LOGS].slice().reverse();
  const refundList = reversed.filter((log) => {
    const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
    const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : null);
    const isSuccess = body.PaymentStatus === 1 || body.PaymentStatus === '1' || body.paymentStatus === 'Success' || body.status === 1;
    if (!txId || !isSuccess || !log.merchantId || !MERCHANTS.get(log.merchantId)) return false;
    const payDate = body.PaymentDate || body.paymentDate || log.receivedAtIso || log.receivedAt;
    return getVoidRefundWindow(payDate) === 'refund';
  });
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const rows = refundList.map((log) => {
    const realIndex = NOTI_LOGS.indexOf(log);
    const dt = formatDateAndTimeTHJP(log.receivedAtIso || log.receivedAt);
    const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
    const txId = body.TransactionId ?? body.transactionId ?? '-';
    const orderNo = body.OrderNo ?? body.orderNo ?? '-';
    const amount = body.Amount ?? body.amount ?? '-';
    const merchant = log.merchantId ? MERCHANTS.get(log.merchantId) : null;
    const routeNoDisplay = getRouteNoDisplay(merchant, log.routeKey);
    const confirmRefund = (t(locale, 'cr_confirm_refund') || '').replace(/'/g, "\\'").replace(/"/g, '&quot;');
    const action = `<form method="post" action="/admin/cancel-refund/refund-request" style="display:inline;" onsubmit="return confirm('${confirmRefund}');"><input type="hidden" name="index" value="${realIndex}" /><button type="submit" class="btn-refund">${t(locale, 'cr_btn_refund_request')}</button></form>`;
    return `<tr>
      <td class="col-date">${esc(dt.date)}</td>
      <td class="col-time">TH: ${esc(dt.timeTh)}<br><span class="time-jp">JP: ${esc(dt.timeJp)}</span></td>
      <td class="col-narrow">${esc(routeNoDisplay)}</td>
      <td class="col-narrow">${esc(log.merchantId || '')}</td>
      <td>${esc(txId)} / ${esc(orderNo)} / ${esc(amount)}</td>
      <td class="col-action">${action}</td>
    </tr>`;
  }).join('');
  const thead = '<thead><tr><th>' + t(locale, 'cr_th_received_date') + '</th><th>' + t(locale, 'cr_th_received_time') + '</th><th>' + t(locale, 'cr_th_route_no') + '</th><th>' + t(locale, 'cr_th_merchant') + '</th><th>' + t(locale, 'cr_th_tx_order_amount') + '</th><th>' + t(locale, 'cr_th_action') + '</th></tr></thead>';
  const tableContent = syncForm + '<table>' + thead + '<tbody>' + rows + '</tbody></table>';
  res.send(renderCancelRefundPage(locale, adminUser, t(locale, 'nav_cancel_refund_refund') + ' (' + refundList.length + ')', tableContent, alertHtml, req.originalUrl, req.session.member));
});

app.post('/admin/cancel-refund/sync-refund', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const locale = getLocale(req);
  try {
    const result = await syncChillPayRefundNoti();
    if (result.success) {
      return res.redirect('/admin/cancel-refund/refund?sync=ok&sent=' + (result.sent || 0) + '&total=' + (result.total || 0));
    }
    return res.redirect('/admin/cancel-refund/refund?sync=fail&reason=' + encodeURIComponent(result.error || t(locale, 'cr_alert_sync_fail')));
  } catch (err) {
    const msg = err && err.message ? err.message : String(err);
    return res.redirect('/admin/cancel-refund/refund?sync=fail&reason=' + encodeURIComponent(msg));
  }
});

app.post('/admin/cancel-refund/force-void-request', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const locale = getLocale(req);
  const index = parseInt(req.body.index, 10);
  if (Number.isNaN(index) || index < 0 || index >= NOTI_LOGS.length) {
    return res.redirect('/admin/cancel-refund/force-void?fail=1&reason=' + encodeURIComponent(t(locale, 'cr_err_invalid_index')));
  }
  const log = NOTI_LOGS[index];
  const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
  const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : null);
  if (!txId) return res.redirect('/admin/cancel-refund/force-void?fail=1&reason=' + encodeURIComponent(t(locale, 'cr_err_no_transaction_id')));
  await sendVoidOrRefundNoti(log, 'void');
  markVoidNotiSent(txId);
  return res.redirect('/admin/cancel-refund/force-void?ok=1');
});

app.post('/admin/cancel-refund/void-request', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const locale = getLocale(req);
  const index = parseInt(req.body.index, 10);
  if (Number.isNaN(index) || index < 0 || index >= NOTI_LOGS.length) {
    return res.redirect('/admin/cancel-refund/void?void=fail&reason=' + encodeURIComponent(t(locale, 'cr_err_invalid_index')));
  }
  const log = NOTI_LOGS[index];
  const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
  const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : null);
  if (!txId) return res.redirect('/admin/cancel-refund/void?void=fail&reason=' + encodeURIComponent(t(locale, 'cr_err_no_transaction_id')));
  const cfg = loadChillPayTransactionConfig();
  const result = await chillPayRequestVoid(txId, cfg.useSandbox);
  if (!result.success) return res.redirect('/admin/cancel-refund/void?void=fail&reason=' + encodeURIComponent(result.error || t(locale, 'cr_alert_void_fail')));
  if (!hasVoidNotiSent(txId)) {
    await sendVoidOrRefundNoti(log, 'void');
    markVoidNotiSent(txId);
  }
  return res.redirect('/admin/cancel-refund/void?void=ok');
});

app.post('/admin/cancel-refund/refund-request', requireAuth, requirePage('cancel_refund'), async (req, res) => {
  const locale = getLocale(req);
  const index = parseInt(req.body.index, 10);
  if (Number.isNaN(index) || index < 0 || index >= NOTI_LOGS.length) {
    return res.redirect('/admin/cancel-refund/refund?refund=fail&reason=' + encodeURIComponent(t(locale, 'cr_err_invalid_index')));
  }
  const log = NOTI_LOGS[index];
  const body = log.body && typeof log.body === 'object' ? log.body : (typeof log.body === 'string' ? (() => { try { return JSON.parse(log.body); } catch { return {}; } })() : {});
  const txId = body.TransactionId != null ? body.TransactionId : (body.transactionId != null ? body.transactionId : null);
  if (!txId) return res.redirect('/admin/cancel-refund/refund?refund=fail&reason=' + encodeURIComponent(t(locale, 'cr_err_no_transaction_id')));
  const cfg = loadChillPayTransactionConfig();
  const result = await chillPayRequestRefund(txId, cfg.useSandbox);
  if (!result.success) return res.redirect('/admin/cancel-refund/refund?refund=fail&reason=' + encodeURIComponent(result.error || t(locale, 'cr_alert_refund_fail')));
  await sendVoidOrRefundNoti(log, 'refund');
  markRefundNotiSent(txId);
  return res.redirect('/admin/cancel-refund/refund?refund=ok');
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
    if (ok) {
      const formatUsed = forceJson ? 'json' : forceForm ? 'form' : 'raw';
      if (NOTI_LOGS[index]) {
        NOTI_LOGS[index].relayStatus = 'ok';
        NOTI_LOGS[index].relayFailReason = '';
        NOTI_LOGS[index].relayFormatUsed = formatUsed;
      }
      return res.redirect(base + '?resend=ok');
    }
    const bodyPart = relayRes.data != null
      ? (typeof relayRes.data === 'string' ? String(relayRes.data) : JSON.stringify(relayRes.data))
      : '';
    const reason = `HTTP ${relayRes.status}` + (bodyPart ? ': ' + bodyPart.slice(0, 300) : '');
    return res.redirect(base + '?resend=fail&reason=' + encodeURIComponent(reason));
  } catch (err) {
    const reason = err.code || err.message || String(err);
    const returnTo = (req.body.returnTo || '').trim() || 'logs';
    const base = returnTo === 'logs-result' ? '/admin/logs-result' : '/admin/logs';
    return res.redirect(base + '?resend=fail&reason=' + encodeURIComponent(reason));
  }
});

// 피지결과 (요약) 페이지
app.get('/admin/logs-result', requireAuth, requirePage('pg_result'), (req, res) => {
  const locale = getLocale(req);
  const q = req.query || {};
  const resendMsg =
    q.resend === 'ok'
      ? '<div class="alert alert-ok">재전송이 완료되었습니다.</div>'
      : q.resend === 'fail' && q.reason
      ? '<div class="alert alert-fail">재전송 실패: ' + String(q.reason).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') + '</div>'
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
  const reversed = [...NOTI_LOGS].slice().reverse();
  const rows = reversed
    .map((log, i) => {
      const realIndex = NOTI_LOGS.length - 1 - i;
      const dt = formatDateAndTimeTHJP(log.receivedAtIso || log.receivedAt);
      const envLabel = (log.env && String(log.env).toLowerCase()) === 'sandbox' ? 'sandbox' : 'live';
      const routeNo = (log.routeKey && (log.routeKey.match(/\/(\d+)$/) || [null, log.routeKey])[1]) || log.routeKey || '-';
      const relayStatus = log.relayStatus || '-';
      const relayLabel = relayStatus === 'ok' ? '성공' : relayStatus === 'fail' ? '실패' : relayStatus === 'skip' ? '미전송' : relayStatus;
      const formatUsed = log.relayFormatUsed || (log.relaySentAsJson === true ? 'json' : 'raw');
      const relayClass = relayStatus === 'ok' ? (formatUsed === 'json' ? 'status-ok' : formatUsed === 'form' ? 'status-ok-form' : 'status-ok-normal') : relayStatus === 'fail' ? 'status-fail' : '';
      const failReason = (log.relayFailReason || '').trim();
      const canResend = (relayStatus === 'fail' || relayStatus === 'ok') && (log.targetUrl || findMerchantByRouteKey(log.routeKey)) && (log.body || log.rawBody);
      const resendBtn = canResend
        ? `<div class="resend-wrap" style="display:inline-flex;flex-direction:row;gap:6px;align-items:center;flex-wrap:nowrap;"><form method="post" action="/admin/logs/resend" style="display:inline;"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="returnTo" value="logs-result" /><button type="submit" class="btn-resend" onclick="return confirm('일반(원문) 형식으로 다시 전송하시겠습니까?');">일반</button></form><form method="post" action="/admin/logs/resend" style="display:inline;"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="returnTo" value="logs-result" /><input type="hidden" name="resendAsJson" value="1" /><button type="submit" class="btn-resend-json" onclick="return confirm('JSON 형식으로 다시 전송하시겠습니까?');">JSON</button></form><form method="post" action="/admin/logs/resend" style="display:inline;"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="returnTo" value="logs-result" /><input type="hidden" name="resendAsForm" value="1" /><button type="submit" class="btn-resend-form" onclick="return confirm('FORM 형식으로 다시 전송하시겠습니까?');">FORM</button></form></div>`
        : '-';
      return `<tr>
        <td>${esc(dt.date)}</td>
        <td>TH: ${esc(dt.timeTh)}<br><span class="time-jp">JP: ${esc(dt.timeJp)}</span></td>
        <td>${esc(routeNo)}</td>
        <td>${esc(envLabel)}</td>
        <td>${esc(log.merchantId || '-')}</td>
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
    .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
      <h1>${t(locale, 'nav_pg_result') || '피지 결과'} (${NOTI_LOGS.length})</h1>
      <p style="font-size:13px;color:#555;">노티 수신·가맹점 전달 결과 요약 (성공/실패·실패 사유·재전송) ㅣ 성공 <span style="color:#2563eb;font-weight:600;">파란색</span>=일반, <span style="color:#059669;font-weight:600;">녹색</span>=JSON, <span style="color:#ca8a04;font-weight:600;">노란색</span>=FORM</p>
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
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
    .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
    .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
    .nav a,
    .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
      const resendBtn = canResend
        ? `<form method="post" action="/admin/internal/resend" style="display:inline;" onsubmit="return confirm('해당 노티를 전산으로 다시 전송하시겠습니까?');"><input type="hidden" name="index" value="${realIndex}" /><button type="submit" class="btn-resend">재전송</button></form>`
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
    .nav a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
  try {
    const result = await sendToInternal(url, log.payload);
    if (result.success) return res.redirect(base + '?resend=ok');
    const reason = result.status ? 'HTTP ' + result.status : (result.error || '');
    return res.redirect(base + '?resend=fail&reason=' + encodeURIComponent(reason));
  } catch (err) {
    return res.redirect(base + '?resend=fail&reason=' + encodeURIComponent(err.message || String(err)));
  }
});

// 전산결과 (요약) 페이지
app.get('/admin/internal-result', requireAuth, requirePage('internal_result'), (req, res) => {
  const locale = getLocale(req);
  const q = req.query || {};
  const resendMsg =
    q.resend === 'ok'
      ? '<div class="alert alert-ok">재전송이 완료되었습니다.</div>'
      : q.resend === 'fail'
      ? '<div class="alert alert-fail">재전송 실패' + (q.reason ? ': ' + String(q.reason).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '') + '</div>'
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
      const resendBtn = canResend
        ? `<form method="post" action="/admin/internal/resend" style="display:inline;"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="returnTo" value="internal-result" /><button type="submit" class="btn-resend" onclick="return confirm('해당 노티를 전산으로 다시 전송하시겠습니까?');">재전송</button></form>`
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
    .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
      <p style="font-size:13px;color:#555;">전산 노티 전달 결과 요약 (성공/실패·재전송)</p>
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
      const resendBtn = canResend
        ? `<form method="post" action="/admin/dev-internal/resend" style="display:inline;" onsubmit="return confirm('해당 노티를 개발 전산으로 다시 전송하시겠습니까?');"><input type="hidden" name="index" value="${realIndex}" /><button type="submit" class="btn-resend">재전송</button></form>`
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
    .nav a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
  try {
    const result = await sendToInternal(url, log.payload);
    if (result.success) return res.redirect(base + '?resend=ok');
    const reason = result.status ? 'HTTP ' + result.status : (result.error || '');
    return res.redirect(base + '?resend=fail&reason=' + encodeURIComponent(reason));
  } catch (err) {
    return res.redirect(base + '?resend=fail&reason=' + encodeURIComponent(err.message || String(err)));
  }
});

// 개발결과 (요약) 페이지
app.get('/admin/dev-internal-result', requireAuth, requirePage('dev_result'), (req, res) => {
  const locale = getLocale(req);
  const q = req.query || {};
  const resendMsg =
    q.resend === 'ok'
      ? '<div class="alert alert-ok">재전송이 완료되었습니다.</div>'
      : q.resend === 'fail'
      ? '<div class="alert alert-fail">재전송 실패' + (q.reason ? ': ' + String(q.reason).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '') + '</div>'
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
      const resendBtn = canResend
        ? `<form method="post" action="/admin/dev-internal/resend" style="display:inline;"><input type="hidden" name="index" value="${realIndex}" /><input type="hidden" name="returnTo" value="dev-internal-result" /><button type="submit" class="btn-resend" onclick="return confirm('해당 노티를 개발 전산으로 다시 전송하시겠습니까?');">재전송</button></form>`
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
    .nav a, .nav a:visited { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
    .nav a { display:block; padding:4px 10px; margin-bottom:0; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover, .nav a.active { background:rgba(139, 92, 246, 0.35); color:#e9d5ff; }
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
});
