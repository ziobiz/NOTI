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
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());
app.use(
  session({
    secret: process.env.SESSION_SECRET || 'change-this-secret',
    resave: false,
    saveUninitialized: false,
    cookie: { maxAge: 60 * 60 * 1000 },
  }),
);

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
const PAGE_KEYS = ['merchants', 'pg_logs', 'internal_logs', 'traffic_analysis', 'internal_targets', 'internal_noti_settings', 'test_config', 'test_run', 'test_history', 'account', 'settings', 'account_reset'];
const INITIAL_PASSWORD_SUFFIX = '1!';
const INTERNAL_TARGETS_CONFIG_PATH = path.join(CONFIG_DIR, 'internal-targets.json');
const INTERNAL_NOTI_SETTINGS_PATH = path.join(CONFIG_DIR, 'internal-noti-settings.json');
const SITE_SETTINGS_PATH = path.join(CONFIG_DIR, 'site-settings.json');
const TEST_CONFIGS_CONFIG_PATH = path.join(CONFIG_DIR, 'test-configs.json');

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
    return Array.isArray(arr) ? arr : [];
  } catch (e) {
    return null;
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
      canAssignPermission: false,
      mustChangePassword: false,
      createdAt: new Date().toISOString(),
    },
  ];
  saveMembers(MEMBERS);
}

function verifyOtp(secret, token) {
  if (!secret) return true;
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
    const m = req.session.member;
    if (m.role === ROLES.SUPER_ADMIN || m.role === ROLES.ADMIN) return next();
    if (m.role === ROLES.OPERATOR && m.permissions && m.permissions.includes(pageKey)) return next();
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

// ========== 테스트 결제 환경 설정 (test payment configs) ==========
// id -> { id, name, environment, merchantCode, routeNo, apiKey, md5Key, currency, paymentApiUrl, returnUrl }
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

// ========== 가맹점 라우팅 설정 (merchantId -> { routeCallbackKey, routeResultKey, callbackUrl, resultUrl, routeNo, internalCustomerId, internalTargetId, enableRelay, enableInternal }) ==========
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

// 트래픽 수집 (관리자 페이지 접근, 최근 10000건)
const TRAFFIC_HITS = [];
const TRAFFIC_HITS_MAX = 10000;

// PG 노티 로그 메모리 저장 (최근 100건)
const NOTI_LOGS = [];

// 전산 저장용 로그 (최근 200건, 파일에도 기록)
const INTERNAL_LOGS = [];
const INTERNAL_LOG_PATH = path.join(__dirname, 'data', 'internal-noti.log');

function appendInternalLog(entry) {
  INTERNAL_LOGS.push(entry);
  if (INTERNAL_LOGS.length > 200) INTERNAL_LOGS.shift();
  const dir = path.dirname(INTERNAL_LOG_PATH);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  // 파일에도 태국 시간 기준으로 기록
  const logWithTime = {
    storedAt: getThailandNowString(),
    ...entry,
  };
  fs.appendFile(INTERNAL_LOG_PATH, JSON.stringify(logWithTime) + '\n', () => {});
}

// 설정 변경 로그 (관리자 설정, 가맹점, 전산 대상 등)
const CONFIG_LOG_PATH = path.join(__dirname, 'data', 'config-change.log');

function appendConfigChangeLog(entry) {
  const dir = path.dirname(CONFIG_LOG_PATH);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const log = {
    at: new Date().toISOString(),
    ...entry,
  };
  fs.appendFile(CONFIG_LOG_PATH, JSON.stringify(log) + '\n', () => {});
}

// 테스트 결제 로그 (최근 100건, 파일에도 기록)
const TEST_LOGS = [];
const TEST_LOG_PATH = path.join(__dirname, 'data', 'test-payments.log');

function appendTestLog(entry) {
  TEST_LOGS.push(entry);
  if (TEST_LOGS.length > 100) TEST_LOGS.shift();
  const dir = path.dirname(TEST_LOG_PATH);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  // 파일에도 태국 시간 기준으로 기록
  const logWithTime = {
    loggedAt: getThailandNowString(),
    ...entry,
  };
  fs.appendFile(TEST_LOG_PATH, JSON.stringify(logWithTime) + '\n', () => {});
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
 * 가맹점으로 원본 그대로 릴레이
 */
async function relayToMerchant(callbackUrl, body) {
  const res = await axios.post(callbackUrl, body, {
    headers: { 'Content-Type': 'application/json' },
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

async function handleNotiRequest(routeKey, req, res) {
  const body = req.body;

  console.log('[수신] routeKey=', routeKey, 'body=', JSON.stringify(body));

  const match = findMerchantByRouteKey(routeKey);
  if (!match) {
    console.error('[에러] 등록되지 않은 routeKey:', routeKey);
    return res.status(404).json({ error: 'Route not found', routeKey });
  }

  const { merchantId, merchant, kind, targetUrl } = match;
  const enableRelay = merchant.enableRelay !== false;
  const enableInternal = merchant.enableInternal !== false;

  let relaySuccess = false;
  if (enableRelay && targetUrl) {
    try {
      console.log('[포워딩 중] 가맹점으로 릴레이:', merchantId, kind, targetUrl);
      let relayRes = await relayToMerchant(targetUrl, body);
      relaySuccess = relayRes.status >= 200 && relayRes.status < 300;
      if (relaySuccess) {
        console.log('[포워딩 성공] status=', relayRes.status);
      } else {
        console.warn('[포워딩 실패] status=', relayRes.status, ' 1회 재시도 예정');
        await new Promise((r) => setTimeout(r, 2000));
        relayRes = await relayToMerchant(targetUrl, body);
        relaySuccess = relayRes.status >= 200 && relayRes.status < 300;
        if (relaySuccess) console.log('[포워딩 재시도 성공] status=', relayRes.status);
        else console.warn('[포워딩 재시도 실패] status=', relayRes.status);
      }
    } catch (err) {
      console.error('[포워딩 실패]', err.message, ' 1회 재시도 예정');
      try {
        await new Promise((r) => setTimeout(r, 2000));
        const retryRes = await relayToMerchant(targetUrl, body);
        relaySuccess = retryRes.status >= 200 && retryRes.status < 300;
        if (relaySuccess) console.log('[포워딩 재시도 성공]');
      } catch (_) {}
    }
  } else {
    console.log('[포워딩 스킵] enableRelay=false 또는 targetUrl 없음');
  }

  // 로그 적재 (가맹점 수신 여부 포함, 최근 100건)
  NOTI_LOGS.push({
    receivedAt: getThailandNowString(),
    receivedAtIso: new Date().toISOString(),
    routeKey,
    merchantId,
    kind,
    body,
    targetUrl: enableRelay ? targetUrl || '' : '',
    relayStatus: enableRelay ? (relaySuccess ? 'ok' : 'fail') : 'skip',
  });
  if (NOTI_LOGS.length > 100) NOTI_LOGS.shift();

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

function getAdminSidebar(locale, adminUser, member) {
  const site = loadSiteSettings();
  const langLinks = SUPPORTED_LOCALES.map((l) => `<a href="/admin/set-locale?lang=${l}" style="color:#93c5fd;text-decoration:none;margin:0 2px;">${l.toUpperCase()}</a>`).join(' ');
  const role = member && member.role ? member.role : null;
  const canSeeMembers = role === ROLES.SUPER_ADMIN || role === ROLES.ADMIN;
  const perms = member && member.permissions ? member.permissions : PAGE_KEYS;
  const can = (key) => canSeeMembers || perms.includes(key);
  const nav = [];
  if (can('merchants')) {
    nav.push(`<div class="nav-section-title">${t(locale, 'nav_merchant')}</div><a href="/admin/merchants">${t(locale, 'nav_merchant_settings')}</a>`);
  }
  if (can('pg_logs') || can('internal_logs') || can('traffic_analysis')) {
    nav.push(`<div class="nav-section-title">${t(locale, 'nav_logs')}</div>`);
    if (can('pg_logs')) nav.push(`<a href="/admin/logs">${t(locale, 'nav_pg_noti_log')}</a>`);
    if (can('internal_logs')) nav.push(`<a href="/admin/internal">${t(locale, 'nav_internal_noti_log')}</a>`);
    if (can('traffic_analysis')) nav.push(`<a href="/admin/traffic">${t(locale, 'nav_traffic_analysis') || '트래픽분석'}</a>`);
  }
  if (can('internal_targets') || can('internal_noti_settings')) {
    nav.push(`<div class="nav-section-title">${t(locale, 'nav_internal')}</div>`);
    if (can('internal_targets')) nav.push(`<a href="/admin/internal-targets">${t(locale, 'nav_internal_targets')}</a>`);
    if (can('internal_noti_settings')) nav.push(`<a href="/admin/internal-noti-settings">${t(locale, 'nav_internal_noti_settings')}</a>`);
  }
  if (can('test_config') || can('test_run') || can('test_history')) {
    nav.push(`<div class="nav-section-title">${t(locale, 'nav_test')}</div>`);
    if (can('test_config')) nav.push(`<a href="/admin/test-configs">${t(locale, 'nav_test_config')}</a>`);
    if (can('test_run')) nav.push(`<a href="/admin/test-pay">${t(locale, 'nav_test_run')}</a>`);
    if (can('test_history')) nav.push(`<a href="/admin/test-logs">${t(locale, 'nav_test_history')}</a>`);
  }
  if (canSeeMembers || can('settings') || can('account') || can('account_reset')) {
    nav.push(`<div class="nav-section-title">${t(locale, 'nav_system')}</div>`);
    if (canSeeMembers) nav.push(`<a href="/admin/members">${t(locale, 'nav_account_manage')}</a>`);
    if (can('account_reset')) nav.push(`<a href="/admin/account-reset">${t(locale, 'nav_account_reset')}</a>`);
    if (can('settings')) nav.push(`<a href="/admin/settings">${t(locale, 'nav_settings')}</a>`);
    if (can('account')) nav.push(`<a href="/admin/account">${t(locale, 'nav_account')}</a>`);
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
function getAdminTopbar(locale, clientIp, now, adminUser, currentPath) {
  const back = currentPath || '/admin/merchants';
  const langLinks = SUPPORTED_LOCALES.map((l) => `<a href="/admin/set-locale?lang=${l}&back=${encodeURIComponent(back)}" style="color:#0369a1;text-decoration:none;margin:0 4px;">${l.toUpperCase()}</a>`).join(' ');
  const logoutLink = '<a href="/admin/logout" style="color:#0369a1;text-decoration:none;margin-left:8px;">로그아웃</a>';
  return `<div class="topbar">
    <span>${t(locale, 'topbar_ip')}: ${clientIp || '-'}</span>
    <span>${t(locale, 'topbar_time')}: ${now}</span>
    <span style="margin-left:auto;">${t(locale, 'lang_switch')}: ${langLinks}</span>
    <span>${t(locale, 'user_label')}: ${adminUser || '-'}</span>
    <span>${logoutLink}</span>
  </div>`;
}

// 로그인 페이지 (다국어 + Google OTP 안내)
app.get('/admin/login', (req, res) => {
  const locale = getLocale(req);
  const langLinks = SUPPORTED_LOCALES.map((l) => `<a href="/admin/set-locale?lang=${l}&back=/admin/login" style="color:#93c5fd;margin:0 4px;">${l.toUpperCase()}</a>`).join(' ');
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
      <form method="post" action="/admin/login">
        <label>${t(locale, 'login_username')}<input type="text" name="username" required /></label>
        <label>${t(locale, 'login_password')}<input type="password" name="password" required /></label>
        <label>${t(locale, 'login_otp')}<input type="text" name="otp" maxlength="6" placeholder="000000" /></label>
        <div class="hint">${t(locale, 'login_otp_hint')}</div>
        <button type="submit">${t(locale, 'login_submit')}</button>
      </form>
      <p style="margin-top:14px;font-size:13px;"><a href="/admin/forgot" style="color:#93c5fd;">비밀번호 초기화 요청</a> &middot; <a href="/admin/forgot-id" style="color:#93c5fd;">아이디 찾기</a></p>
    </div>
  </div>
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
    return res.status(401).send(t(locale, 'login_error_cred'));
  }

  const ok = bcrypt.compareSync(password, member.passwordHash);
  if (!ok) {
    return res.status(401).send(t(locale, 'login_error_cred'));
  }

  if (member.otpRequired && !verifyOtp(member.otpSecret, otp)) {
    return res.status(401).send(t(locale, 'login_error_otp'));
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
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const now = new Date().toLocaleString('ko-KR', { hour12: false });

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
    .sidebar { width:280px; background:#111827; padding:20px 16px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:4px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:12px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:16px; padding:6px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:12px 4px 4px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav a,
    .nav a:visited { display:block; padding:8px 10px; margin-bottom:4px; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover { background:#1f2937; color:#e0f2fe; }
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
    ${getAdminSidebar(locale, adminUser, req.session.member)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, now, adminUser, req.originalUrl)}
      <div class="card">
        <h1>${t(locale, 'account_title')}</h1>
        <p style="font-size:13px;color:#555;">${t(locale, 'account_desc')}</p>
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
  return res.redirect('/admin/account');
});

// ----- 환경설정 (왼쪽 상단 노출 이름 / 추가 노출 문구) -----
app.get('/admin/settings', requireAuth, requirePage('settings'), (req, res) => {
  const locale = getLocale(req);
  const clientIp = (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() || req.ip || '';
  const adminUser = req.session.adminUser || '';
  const now = new Date().toLocaleString('ko-KR', { hour12: false });
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
    .sidebar { width:260px; background:#111827; padding:20px 16px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:4px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:12px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:16px; padding:6px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:12px 4px 4px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav a, .nav a:visited { display:block; padding:8px 10px; margin-bottom:4px; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover { background:#1f2937; color:#e0f2fe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; margin-bottom:0; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:18px 22px; border-radius:10px; box-shadow:0 10px 25px rgba(15,23,42,0.06); border:1px solid #e5e7eb; }
    label { display:block; margin-top:12px; font-size:14px; }
    input[type="text"] { width:100%; max-width:400px; padding:10px 12px; margin-top:4px; box-sizing:border-box; border-radius:6px; border:1px solid #d1d5db; }
    button { margin-top:16px; padding:10px 18px; background:#2563eb; color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:14px; }
    button:hover { background:#1d4ed8; }
    .hint { font-size:12px; color:#6b7280; margin-top:6px; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, now, adminUser, req.originalUrl)}
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
    </main>
  </div>
</body>
</html>`);
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
  pg_logs: 'PG 노티로그',
  internal_logs: '정산 노티로그',
  traffic_analysis: '트래픽분석',
  internal_targets: '노티 추가설정',
  internal_noti_settings: '노티 환경설정',
  test_config: '테스트 설정',
  test_run: '테스트 실행',
  test_history: '테스트 내역',
  account: '계정 설정',
  settings: '환경 설정',
  account_reset: '계정초기화',
};

function getRoleLabel(role, locale) {
  if (role === ROLES.SUPER_ADMIN) return t(locale, 'role_super_admin');
  if (role === ROLES.ADMIN) return t(locale, 'role_admin');
  if (role === ROLES.OPERATOR) return t(locale, 'role_operator');
  return role || '';
}

// 페이지 번호(1~10) 설명 문구 (상단 안내용)
const PAGE_NUM_LEGEND = PAGE_KEYS.map((k, i) => `${i + 1}=${PAGE_KEY_LABELS[k] || k}`).join(', ');

app.get('/admin/members', requireMemberManage[0], requireMemberManage[1], (req, res) => {
  const locale = getLocale(req);
  const clientIp = (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() || req.ip || '';
  const adminUser = req.session.adminUser || '';
  const now = new Date().toLocaleString('ko-KR', { hour12: false });
  MEMBERS = loadMembers();
  OPERATOR_PERMISSIONS = loadOperatorPermissions();
  const cur = req.session.member;
  const isSuper = cur && cur.role === ROLES.SUPER_ADMIN;
  const list = isSuper ? MEMBERS : MEMBERS.filter((x) => x.role === ROLES.OPERATOR);
  const addFormRoleBlock = isSuper ? `<label>역할 <select name="role"><option value="${ROLES.OPERATOR}">${t(locale, 'role_operator')}</option><option value="${ROLES.ADMIN}">${t(locale, 'role_admin')}</option></select></label>` : '<input type="hidden" name="role" value="OPERATOR" />';
  const addFormPermBlock = PAGE_KEYS.map((k, i) => `<label class="perm-check" title="${PAGE_KEY_LABELS[k] || k}"><input type="checkbox" name="perm_${k}" /> ${i + 1}</label>`).join('');
  const permHeaderCells = PAGE_KEYS.map((_, i) => `<th class="perm-col">${i + 1}</th>`).join('');
  const confirmDel = (t(locale, 'members_confirm_delete') || '삭제하시겠습니까?').replace(/'/g, "\\'");
  const confirmPw = '비밀번호를 초기(아이디+1!)로 초기화합니다. 진행할까요?'.replace(/'/g, "\\'");
  const confirmOtp = 'OTP를 초기화합니다. 해당 계정은 재등록 후 사용 가능합니다. 진행할까요?'.replace(/'/g, "\\'");
  const rows = list
    .map((mem) => {
      const canEdit = isSuper || (mem.role === ROLES.OPERATOR);
      const canDelete = isSuper || (mem.role === ROLES.OPERATOR) && mem.id !== cur.id;
      const canReset = (isSuper || (mem.role === ROLES.OPERATOR && cur.role === ROLES.ADMIN)) && mem.id !== cur.id;
      const opPerms = mem.role === ROLES.OPERATOR ? (OPERATOR_PERMISSIONS[mem.userId] || []) : [];
      const permCells = mem.role === ROLES.OPERATOR && canEdit
        ? PAGE_KEYS.map((k) => `<td class="perm-cell"><input type="checkbox" name="perm_${k}" ${opPerms.includes(k) ? 'checked' : ''} form="perm-form-${mem.id}" /></td>`).join('')
        : PAGE_KEYS.map(() => '<td class="perm-cell">-</td>').join('');
      const confirmPerm = (t(locale, 'members_confirm_update_permissions') || '페이지 접근 권한을 수정하시겠습니까?').replace(/'/g, "\\'");
      const permForm = mem.role === ROLES.OPERATOR && canEdit
        ? `<form id="perm-form-${mem.id}" method="post" action="/admin/members/update-permissions" style="display:inline;" onsubmit="return confirm('${confirmPerm}');"><input type="hidden" name="id" value="${mem.id}" /><button type="submit" class="btn-update-perm">수정</button></form>`
        : '';
      const resetPwBtn = canReset ? `<form method="post" action="/admin/members/reset-password" style="display:inline;" onsubmit="return confirm('${confirmPw}');"><input type="hidden" name="id" value="${mem.id}" /><button type="submit" class="btn-reset-pw">비번</button></form>` : '-';
      const resetOtpBtn = canReset ? `<form method="post" action="/admin/members/reset-otp" style="display:inline;" onsubmit="return confirm('${confirmOtp}');"><input type="hidden" name="id" value="${mem.id}" /><button type="submit" class="btn-reset-otp">OTP</button></form>` : '-';
      const initCell = canReset ? `<td class="init-cell">${resetPwBtn} ${resetOtpBtn}</td>` : '<td class="init-cell">-</td>';
      const editLink = canEdit ? `<a href="/admin/members/edit/${mem.id}" class="btn-edit">${t(locale, 'edit_member')}</a>` : '-';
      const delBtn = canDelete ? `<form method="post" action="/admin/members/delete" style="display:inline;" onsubmit="return confirm('${confirmDel}');"><input type="hidden" name="id" value="${mem.id}" /><button type="submit" class="btn-del">${t(locale, 'delete_member')}</button></form>` : '-';
      return `<tr>
        <td>${(mem.name || '').replace(/</g, '&lt;')}</td>
        <td>${(mem.country || '').replace(/</g, '&lt;')}</td>
        <td>${(mem.userId || '').replace(/</g, '&lt;')}</td>
        <td>${(mem.email || '').replace(/</g, '&lt;')}</td>
        <td>${(mem.birthDate || '').replace(/</g, '&lt;')}</td>
        <td>${getRoleLabel(mem.role, locale)}</td>
        ${permCells}
        ${initCell}
        <td class="manage-cell">${permForm} ${editLink} ${delBtn}</td>
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
    .sidebar { width:260px; background:#111827; padding:20px 16px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:4px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:12px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:16px; padding:6px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:12px 4px 4px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav a, .nav a:visited { display:block; padding:8px 10px; margin-bottom:4px; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover { background:#1f2937; color:#e0f2fe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; margin-bottom:0; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:18px 22px; border-radius:10px; box-shadow:0 10px 25px rgba(15,23,42,0.06); border:1px solid #e5e7eb; }
    h1 { margin-bottom:8px; }
    h2 { margin-top:20px; margin-bottom:12px; font-size:16px; }
    table { border-collapse:collapse; width:100%; background:#fff; border-radius:8px; overflow:hidden; }
    th, td { border:1px solid #e5e7eb; padding:8px 10px; font-size:14px; text-align:center; }
    th { background:#e5f0ff; color:#1f2937; }
    tr:nth-child(even) { background:#f9fafb; }
    .perm-col { min-width:32px; font-weight:600; }
    .perm-cell { padding:4px; }
    .perm-cell input { margin:0; cursor:pointer; }
    .manage-cell { white-space:nowrap; }
    .manage-cell form { display:inline; margin-right:6px; }
    .init-cell { white-space:nowrap; }
    .init-cell form { display:inline; margin-right:4px; }
    .btn-update-perm { padding:4px 10px; font-size:12px; background:#2563eb; color:#fff; border:none; border-radius:6px; cursor:pointer; }
    .btn-update-perm:hover { background:#1d4ed8; }
    .btn-edit { display:inline-block; padding:4px 10px; font-size:12px; background:#eab308; color:#111827; border:none; border-radius:4px; cursor:pointer; text-decoration:none; margin-right:4px; }
    .btn-edit:hover { background:#ca8a04; color:#fff; }
    .btn-del { padding:4px 8px; font-size:12px; background:#dc2626; color:#fff; border:none; border-radius:4px; cursor:pointer; }
    .btn-reset-pw { padding:4px 8px; font-size:12px; background:#6b7280; color:#fff; border:none; border-radius:4px; cursor:pointer; }
    .btn-reset-otp { padding:4px 8px; font-size:12px; background:#7c3aed; color:#fff; border:none; border-radius:4px; cursor:pointer; }
    label { display:block; margin-top:10px; font-size:14px; }
    .perm-legend { font-size:12px; color:#6b7280; background:#f9fafb; border:1px solid #e5e7eb; border-radius:6px; padding:8px 12px; margin-bottom:12px; }
    .add-form-grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(200px, 1fr)); gap:12px 20px; align-items:end; }
    .add-form-grid label { margin-top:0; }
    input[type="text"], input[type="email"], input[type="date"], select { width:100%; max-width:280px; padding:8px 10px; border-radius:6px; border:1px solid #d1d5db; box-sizing:border-box; }
    .perm-check { display:inline-flex; align-items:center; gap:4px; margin-right:10px; margin-top:8px; }
    .perm-check input { width:auto; max-width:none; }
    button[type="submit"].btn-add { padding:10px 18px; background:#2563eb; color:#fff; border:none; border-radius:6px; cursor:pointer; font-size:14px; margin-top:12px; }
    button[type="submit"].btn-add:hover { background:#1d4ed8; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, now, adminUser, req.originalUrl)}
      <div class="card">
        <h1>${t(locale, 'account_manage_title')}</h1>
        <p style="font-size:13px;color:#555;">${t(locale, 'account_list_desc')}</p>
        ${(isSuper || cur.role === ROLES.ADMIN) ? `
        <h2>${t(locale, 'add_member')}</h2>
        <p class="perm-legend"><strong>페이지 접근 번호:</strong> ${PAGE_NUM_LEGEND}</p>
        <form method="post" action="/admin/members/add" class="add-form-grid" onsubmit="return confirm('${(t(locale, 'merchants_confirm_save') || '저장(적용)하시겠습니까?').replace(/'/g, "\\'")}');">
          <label>${t(locale, 'member_name')} <input type="text" name="name" required /></label>
          <label>${t(locale, 'member_country')} <input type="text" name="country" /></label>
          <label>${t(locale, 'member_user_id')} <input type="text" name="userId" required /></label>
          <label>${t(locale, 'member_email')} <input type="email" name="email" /></label>
          <label>${t(locale, 'member_birth_date')} <input type="text" name="birthDate" placeholder="YYYY-MM-DD" /></label>
          ${addFormRoleBlock}
          <label style="grid-column:1/-1;">${t(locale, 'page_permissions')} (OPERATOR만 해당) <div>${addFormPermBlock}</div></label>
          <label style="grid-column:1/-1;"><button type="submit" class="btn-add">${t(locale, 'members_save')}</button></label>
        </form>
        ` : ''}
      </div>
      <div class="card">
        <h2>${t(locale, 'registered_accounts')}</h2>
        <p class="perm-legend"><strong>페이지 접근:</strong> ${PAGE_NUM_LEGEND}</p>
        <table>
          <thead><tr><th>${t(locale, 'member_name')}</th><th>${t(locale, 'member_country')}</th><th>${t(locale, 'member_user_id')}</th><th>${t(locale, 'member_email')}</th><th>${t(locale, 'member_birth_date')}</th><th>역할</th>${permHeaderCells}<th>초기화</th><th>관리</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    </main>
  </div>
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

app.get('/admin/members/edit/:id', requireMemberManage[0], requireMemberManage[1], (req, res) => {
  const locale = getLocale(req);
  const id = req.params.id;
  MEMBERS = loadMembers();
  const mem = getMemberById(id);
  if (!mem) return res.status(404).send('Not found');
  const cur = req.session.member;
  const isSuper = cur.role === ROLES.SUPER_ADMIN;
  const canEditPermission = isSuper || (mem.role === ROLES.OPERATOR && cur.canAssignPermission);
  if (!isSuper && mem.role !== ROLES.OPERATOR) return res.status(403).send('Forbidden');
  const clientIp = (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() || req.ip || '';
  const adminUser = req.session.adminUser || '';
  const now = new Date().toLocaleString('ko-KR', { hour12: false });
  OPERATOR_PERMISSIONS = loadOperatorPermissions();
  const opPerms = OPERATOR_PERMISSIONS[mem.userId] || [];
  const permCheckboxes = canEditPermission && mem.role === ROLES.OPERATOR
    ? PAGE_KEYS.map((k, i) => `<label class="perm-check" title="${PAGE_KEY_LABELS[k] || k}"><input type="checkbox" name="perm_${k}" ${opPerms.includes(k) ? 'checked' : ''} /> ${i + 1}</label>`).join('')
    : '';
  const canAssignBlock = isSuper && mem.role === ROLES.ADMIN ? `<label><input type="checkbox" name="canAssignPermission" ${mem.canAssignPermission ? 'checked' : ''} /> ${t(locale, 'can_assign_permission')}</label>` : '';
  const otpRequiredBlock = isSuper ? `<label><input type="checkbox" name="otpRequired" ${mem.otpRequired ? 'checked' : ''} /> OTP 로그인 필수</label>` : '';
  res.send(`<!DOCTYPE html>
<html lang="${locale}">
<head>
  <meta charset="UTF-8" />
  <title>${t(locale, 'edit_member')}</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; color:#111827; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:260px; background:#111827; padding:20px 16px; border-radius:0 10px 10px 0; border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:4px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:12px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:16px; padding:6px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:12px 4px 4px; }
    .nav a { display:block; padding:8px 10px; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover { background:#1f2937; color:#e0f2fe; }
    .main { flex:1; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; border:1px solid #bae6fd; margin-bottom:16px; }
    .card { background:#fff; padding:18px 22px; border-radius:10px; box-shadow:0 10px 25px rgba(15,23,42,0.06); border:1px solid #e5e7eb; }
    .perm-legend { font-size:12px; color:#6b7280; background:#f9fafb; border:1px solid #e5e7eb; border-radius:6px; padding:8px 12px; margin-bottom:12px; }
    label { display:block; margin-top:10px; font-size:14px; }
    .perm-check { display:inline-flex; align-items:center; gap:4px; margin-right:10px; margin-top:8px; }
    .perm-check input { width:auto; max-width:none; }
    input[type="text"], input[type="email"] { width:100%; max-width:320px; padding:8px 10px; border-radius:6px; border:1px solid #d1d5db; box-sizing:border-box; }
    button { padding:10px 18px; background:#2563eb; color:#fff; border:none; border-radius:6px; cursor:pointer; margin-top:12px; font-size:14px; }
    button:hover { background:#1d4ed8; }
    .btn-reset { padding:8px 14px; background:#dc2626; color:#fff; border:none; border-radius:6px; cursor:pointer; margin-top:16px; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, now, adminUser, req.originalUrl)}
      <div class="card">
        <h1>${t(locale, 'edit_member')} - ${(mem.userId || '').replace(/</g, '&lt;')}</h1>
        <p><a href="/admin/members">← ${t(locale, 'account_manage_title')}로</a></p>
        <form method="post" action="/admin/members/edit/${mem.id}" onsubmit="return confirm('${(t(locale, 'merchants_confirm_save') || '저장(적용)하시겠습니까?').replace(/'/g, "\\'")}');">
          <label>${t(locale, 'member_name')} <input type="text" name="name" value="${(mem.name || '').replace(/"/g, '&quot;')}" /></label>
          <label>${t(locale, 'member_country')} <input type="text" name="country" value="${(mem.country || '').replace(/"/g, '&quot;')}" /></label>
          <label>${t(locale, 'member_user_id')} <input type="text" name="userId" value="${(mem.userId || '').replace(/"/g, '&quot;')}" required /></label>
          <label>${t(locale, 'member_email')} <input type="email" name="email" value="${(mem.email || '').replace(/"/g, '&quot;')}" /></label>
          <label>${t(locale, 'member_birth_date')} <input type="text" name="birthDate" value="${(mem.birthDate || '').replace(/"/g, '&quot;')}" placeholder="YYYY-MM-DD" /></label>
          ${otpRequiredBlock}
          ${canAssignBlock}
          ${permCheckboxes ? `<p class="perm-legend"><strong>페이지 접근:</strong> ${PAGE_NUM_LEGEND}</p><div><strong>${t(locale, 'page_permissions')}</strong> <div>${permCheckboxes}</div></div>` : ''}
          <button type="submit">${t(locale, 'members_save')}</button>
        </form>
        ${(isSuper || (mem.role === ROLES.OPERATOR && cur.role === ROLES.ADMIN)) ? `<form method="post" action="/admin/members/reset-password" style="margin-top:16px;" onsubmit="return confirm('비밀번호를 초기(아이디+1!)로 초기화합니다.');"><input type="hidden" name="id" value="${mem.id}" /><button type="submit" class="btn-reset">비밀번호 초기화</button></form>` : ''}
      </div>
    </main>
  </div>
</body>
</html>`);
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
  return res.redirect('/admin/members/edit/' + id);
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
  if (!isSuper && mem.role !== ROLES.OPERATOR) return res.status(403).send('Forbidden');
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
  const now = new Date().toLocaleString('ko-KR', { hour12: false });
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
    .sidebar { width:260px; background:#111827; padding:20px 16px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:4px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:12px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:16px; padding:6px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:12px 4px 4px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav a { display:block; padding:8px 10px; margin-bottom:4px; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover { background:#1f2937; color:#e0f2fe; }
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
    ${getAdminSidebar(locale, adminUser, req.session.member)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, now, adminUser, req.originalUrl)}
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
        <label>생년월일 <input type="text" name="birthDate" placeholder="YYYY-MM-DD" /></label>
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
  const now = new Date().toLocaleString('ko-KR', { hour12: false });
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

  const rows = Array.from(MERCHANTS.entries())
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
    .sidebar { width:260px; background:#111827; padding:20px 16px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:4px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:12px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:16px; padding:6px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:12px 4px 4px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav a,
    .nav a:visited { display:block; padding:8px 10px; margin-bottom:4px; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover { background:#1f2937; color:#e0f2fe; }
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
    ${getAdminSidebar(locale, adminUser, req.session.member)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, now, adminUser, req.originalUrl)}
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
          <input type="checkbox" name="enableInternal" checked />
          전산 노티 사용 (내부 전산 시스템으로 전송)
        </label>
        <button type="submit">${t(locale, 'merchants_save')}</button>
      </form>
    </div>
    <div class="card merchants-table-wrap">
      <h2>${t(locale, 'merchants_list')}</h2>
      <table>
        <thead>
          <tr>
            <th>${t(locale, 'merchants_id')}</th>
            <th class="cell-url">${t(locale, 'merchants_route_callback')}</th>
            <th class="cell-url">${t(locale, 'merchants_route_result')}</th>
            <th class="cell-url">${t(locale, 'merchants_callback_url')}</th>
            <th class="cell-url">${t(locale, 'merchants_result_url')}</th>
            <th>${t(locale, 'merchants_route_no')}</th>
            <th>${t(locale, 'merchants_internal_customer_id')}</th>
            <th>${t(locale, 'merchants_internal_target')}</th>
            <th>${t(locale, 'merchants_relay')}</th>
            <th>${t(locale, 'merchants_internal')}</th>
            <th class="actions-cell">${t(locale, 'internal_targets_manage')}</th>
          </tr>
        </thead>
        <tbody>
          ${
            rows ||
            `<tr><td colspan="11" style="text-align:center;color:#777;">${t(locale, 'merchants_empty')}</td></tr>`
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

        if (cbSelect) cbSelect.value = button.dataset.routeCallbackKey || '';
        if (rsSelect) rsSelect.value = button.dataset.routeResultKey || '';

        updatePreviews();
      }

      if (editButtons && editButtons.length > 0) {
        Array.prototype.forEach.call(editButtons, function (btn) {
          btn.addEventListener('click', function () {
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
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const now = new Date().toLocaleString('ko-KR', { hour12: false });

  const reversed = [...NOTI_LOGS].slice().reverse();
  const rows = reversed
    .map((log, i) => {
      const realIndex = NOTI_LOGS.length - 1 - i;
      const tz = formatTimeMultiTZ(log.receivedAtIso || log.receivedAt);
      const jsonCallback = log.kind === 'callback' ? JSON.stringify(log.body, null, 2) : '';
      const jsonResult = log.kind === 'result' ? JSON.stringify(log.body, null, 2) : '';
      const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      const relayStatus = log.relayStatus || '-';
      const relayLabel = relayStatus === 'ok' ? '성공' : relayStatus === 'fail' ? '실패' : relayStatus === 'skip' ? '미전송' : relayStatus;
      const relayClass = relayStatus === 'ok' ? 'status-ok' : relayStatus === 'fail' ? 'status-fail' : '';
      const canResend = (relayStatus === 'fail' || relayStatus === 'ok') && log.targetUrl && log.body;
      const resendBtn = canResend
        ? `<form method="post" action="/admin/logs/resend" style="display:inline;" onsubmit="return confirm('해당 노티를 가맹점으로 다시 전송하시겠습니까?');"><input type="hidden" name="index" value="${realIndex}" /><button type="submit" class="btn-resend">재전송</button></form>`
        : '-';
      return `<tr>
        <td class="col-time" style="white-space:nowrap;font-size:11px;">${typeof tz === 'object' && tz.th != null ? `TH: ${esc(tz.th)}<br>JP: ${esc(tz.jp)}<br>SG: ${esc(tz.sg)}<br>US: ${esc(tz.us)}` : esc(log.receivedAt || '-')}</td>
        <td class="col-narrow">${esc(log.routeKey || '')}</td>
        <td class="col-narrow">${esc(log.merchantId || '')}</td>
        <td class="col-status"><span class="${relayClass}">${esc(relayLabel)}</span></td>
        <td class="col-json"><pre style="margin:0;white-space:pre-wrap;font-size:12px;">${esc(jsonCallback) || '-'}</pre></td>
        <td class="col-json"><pre style="margin:0;white-space:pre-wrap;font-size:12px;">${esc(jsonResult) || '-'}</pre></td>
        <td class="col-action">${resendBtn}</td>
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
    th, td { border: 1px solid #e5e7eb; padding: 8px 10px; font-size: 13px; vertical-align: top; }
    th { background: #e5f0ff; text-align: center; }
    tr:nth-child(even) { background:#f9fafb; }
    .col-time, .col-narrow { width: 7%; min-width: 65px; }
    .col-status { width: 6%; min-width: 58px; }
    .col-json { width: 32%; }
    .col-action { width: 6%; min-width: 60px; }
    .status-ok { color: #059669; font-weight: 600; }
    .status-fail { color: #dc2626; font-weight: 600; }
    .btn-resend { padding: 4px 10px; font-size: 12px; background: #2563eb; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
    .btn-resend:hover { background: #1d4ed8; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:260px; background:#111827; padding:20px 16px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:4px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:12px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:16px; padding:6px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:12px 4px 4px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav a,
    .nav a:visited { display:block; padding:8px 10px; margin-bottom:4px; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover { background:#1f2937; color:#e0f2fe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:8px; border:1px solid #e5e7eb; }
    pre { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, now, adminUser, req.originalUrl)}
      <div class="card">
      <h1>${t(locale, 'pg_logs_title')} (${NOTI_LOGS.length})</h1>
      <p style="font-size:13px;color:#555;">${t(locale, 'pg_logs_desc')}</p>
      <table>
        <colgroup><col class="col-time" /><col class="col-narrow" /><col class="col-narrow" /><col class="col-status" /><col class="col-json" /><col class="col-json" /><col class="col-action" /></colgroup>
        <thead>
          <tr>
            <th>${t(locale, 'pg_logs_time')}</th>
            <th>${t(locale, 'pg_logs_route_key')}</th>
            <th>${t(locale, 'pg_logs_merchant_id')}</th>
            <th>가맹점 수신</th>
            <th>${t(locale, 'pg_logs_json_callback')}</th>
            <th>${t(locale, 'pg_logs_json_result')}</th>
            <th>재전송</th>
          </tr>
        </thead>
        <tbody>
          ${rows || `<tr><td colspan="7" style="text-align:center;color:#777;">${t(locale, 'pg_logs_empty')}</td></tr>`}
        </tbody>
      </table>
      </div>
    </main>
  </div>
</body>
</html>`);
});

// PG 노티 재전송 (가맹점으로 수동 재전송)
app.post('/admin/logs/resend', requireAuth, requirePage('pg_logs'), async (req, res) => {
  const index = parseInt(req.body.index, 10);
  if (Number.isNaN(index) || index < 0 || index >= NOTI_LOGS.length) {
    return res.redirect('/admin/logs?err=invalid');
  }
  const log = NOTI_LOGS[index];
  const targetUrl = log.targetUrl || (findMerchantByRouteKey(log.routeKey) && findMerchantByRouteKey(log.routeKey).targetUrl);
  if (!targetUrl || !log.body) {
    return res.redirect('/admin/logs?err=no_target');
  }
  try {
    const relayRes = await relayToMerchant(targetUrl, log.body);
    const ok = relayRes.status >= 200 && relayRes.status < 300;
    return res.redirect('/admin/logs?resend=' + (ok ? 'ok' : 'fail'));
  } catch (err) {
    return res.redirect('/admin/logs?resend=fail');
  }
});

// 전산 노티 대상 관리 페이지
app.get('/admin/internal-targets', requireAuth, requirePage('internal_targets'), (req, res) => {
  const locale = getLocale(req);
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const now = new Date().toLocaleString('ko-KR', { hour12: false });

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
    .sidebar { width:260px; background:#111827; padding:20px 16px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:4px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:12px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:16px; padding:6px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:12px 4px 4px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav a,
    .nav a:visited { display:block; padding:8px 10px; margin-bottom:4px; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover { background:#1f2937; color:#e0f2fe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:8px; border:1px solid #e5e7eb; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, now, adminUser, req.originalUrl)}
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
  const now = new Date().toLocaleString('ko-KR', { hour12: false });
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
    .sidebar { width:260px; background:#111827; padding:20px 16px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:4px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:12px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:16px; padding:6px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:12px 4px 4px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav a, .nav a:visited { display:block; padding:8px 10px; margin-bottom:4px; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover { background:#1f2937; color:#e0f2fe; }
    .main { flex:1; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; margin-bottom:16px; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:18px 22px; border-radius:10px; box-shadow:0 10px 25px rgba(15,23,42,0.06); margin-bottom:8px; border:1px solid #e5e7eb; }
    .hint { font-size:12px; color:#6b7280; margin-top:8px; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, now, adminUser, req.originalUrl)}
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
  const now = new Date().toLocaleString('ko-KR', { hour12: false });

  const editId = (req.query.id || '').toString();
  const editingConfig = editId && TEST_CONFIGS.get(editId) ? TEST_CONFIGS.get(editId) : null;

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
          <td class="td-long">${cfg.returnUrl || ''}</td>
          <td class="actions-cell">
            <div style="display:flex;flex-direction:column;gap:4px;align-items:center;">
              <form method="get" action="/admin/test-configs" style="margin:0;">
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
    .sidebar { width:260px; background:#111827; padding:20px 16px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:4px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:12px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:16px; padding:6px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:12px 4px 4px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav a,
    .nav a:visited { display:block; padding:8px 10px; margin-bottom:4px; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover { background:#1f2937; color:#e0f2fe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:8px; border:1px solid #e5e7eb; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, now, adminUser, req.originalUrl)}
      <div class="card">
        <h1>${t(locale, 'test_config_title')}</h1>
        <p style="font-size:13px;color:#555;">${t(locale, 'test_config_desc')}</p>
        <h2>등록/수정</h2>
        <form method="post" action="/admin/test-configs" onsubmit="return confirm('${(t(locale, 'merchants_confirm_save') || '저장(적용)하시겠습니까?').replace(/'/g, "\\'")}');">
          <label>
            설정 ID (영문/숫자)
            <input type="text" name="id" value="${editingConfig ? editingConfig.id : ''}" ${editingConfig ? 'readonly' : ''} required />
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
            <input type="text" name="routeNo" value="${editingConfig ? editingConfig.routeNo : ''}" required />
          </label>
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
            <input type="text" name="returnUrl" value="${editingConfig ? (editingConfig.returnUrl || '') : ''}" placeholder="예: https://tapi.soonpay.co.kr/pay/chillResul" />
          </label>
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
    name,
    environment,
    merchantCode,
    routeNo,
    apiKey,
    md5Key,
    currency,
    paymentApiUrl,
    returnUrl,
  } = req.body;
  if (!id || !name || !environment || !merchantCode || !routeNo || !apiKey || !md5Key || !currency) {
    return res.status(400).send('모든 필드는 필수입니다.');
  }

  const actor = req.session.adminUser || 'unknown';
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const before = TEST_CONFIGS.get(id) || null;

  TEST_CONFIGS.set(id, {
    id,
    name,
    environment,
    merchantCode,
    routeNo,
    apiKey,
    md5Key,
    currency,
    paymentApiUrl: paymentApiUrl || '',
    returnUrl: returnUrl || '',
  });
  saveTestConfigs();

  appendConfigChangeLog({
    type: 'test_config_update',
    actor,
    clientIp,
    testConfigId: id,
    before,
    after: TEST_CONFIGS.get(id),
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
  const now = new Date().toLocaleString('ko-KR', { hour12: false });

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
    .sidebar { width:260px; background:#111827; padding:20px 16px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:4px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:12px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:16px; padding:6px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:12px 4px 4px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav a,
    .nav a:visited { display:block; padding:8px 10px; margin-bottom:4px; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover { background:#1f2937; color:#e0f2fe; }
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
    ${getAdminSidebar(locale, adminUser, req.session.member)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, now, adminUser, req.originalUrl)}
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
    .sidebar { width:260px; background:#111827; padding:20px 16px; border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:4px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:12px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:16px; padding:6px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:12px 4px 4px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav a,
    .nav a:visited { display:block; padding:8px 10px; margin-bottom:4px; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover { background:#1f2937; color:#e0f2fe; }
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
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const now = new Date().toLocaleString('ko-KR', { hour12: false });

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
    .sidebar { width:280px; background:#111827; padding:20px 16px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:4px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:12px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:16px; padding:6px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:12px 4px 4px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav a,
    .nav a:visited { display:block; padding:8px 10px; margin-bottom:4px; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover { background:#1f2937; color:#e0f2fe; }
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
    <aside class="sidebar">
      <div class="sidebar-title">PG 노티 관리자</div>
      <div class="sidebar-sub">테스트 내역</div>
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
    </aside>
    <main class="main">
      <div class="topbar">
        <span>접속 IP: ${clientIp || '-'}</span>
        <span>시간: ${now}</span>
        <span>아이디: ${adminUser || '-'}</span>
      </div>
      <div class="card">
        <h1>테스트 결제 결과</h1>
        <p style="font-size:13px;color:#555;">테스트 결제 요청의 전체 흐름과 결제 API 응답을 확인할 수 있습니다.</p>

        ${
          !paymentError && paymentResponse && paymentResponse.data && paymentResponse.data.data && paymentResponse.data.data.paymentUrl
            ? `<div style="margin:10px 0 16px;padding:10px 12px;border-radius:8px;border:1px solid #d1d5db;background:#ecfeff;font-size:13px;line-height:1.5;">
                <div style="font-weight:600;color:#0369a1;margin-bottom:4px;">3DS(OTP) 인증 페이지</div>
                <div style="word-break:break-all;margin-bottom:6px;">
                  <code style="font-size:12px;background:#e5e7eb;padding:2px 4px;border-radius:4px;color:#111827;">${
                    paymentResponse.data.data.paymentUrl
                  }</code>
                </div>
                <a href="${
                  paymentResponse.data.data.paymentUrl
                }" target="_blank" rel="noopener noreferrer"
                   style="display:inline-block;margin-top:4px;padding:6px 12px;border-radius:6px;background:#0ea5e9;color:#f9fafb;text-decoration:none;font-size:13px;">
                  새 탭에서 3DS 페이지 열기
                </a>
              </div>`
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

// 테스트 결제 내역 페이지 (최근 2건, 로그 시각 4타임존)
app.get('/admin/test-logs', requireAuth, requirePage('test_history'), (req, res) => {
  const locale = getLocale(req);
  const clientIp =
    (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() ||
    req.ip ||
    '';
  const adminUser = req.session.adminUser || '';
  const now = new Date().toLocaleString('ko-KR', { hour12: false });
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
    .sidebar { width:260px; background:#111827; padding:20px 16px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:4px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:12px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:16px; padding:6px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:12px 4px 4px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav a,
    .nav a:visited { display:block; padding:8px 10px; margin-bottom:4px; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover { background:#1f2937; color:#e0f2fe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:8px; border:1px solid #e5e7eb; }
    pre { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; white-space:pre-wrap; word-break:break-all; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, now, adminUser, req.originalUrl)}
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
  const now = new Date().toLocaleString('ko-KR', { hour12: false });
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const reversedInternal = [...INTERNAL_LOGS].slice().reverse();
  const rows = reversedInternal
    .map((log, i) => {
      const realIndex = INTERNAL_LOGS.length - 1 - i;
      const tz = formatTimeMultiTZ(log.storedAt);
      const timeHtml =
        typeof tz === 'object' && tz.th != null
          ? `TH: ${esc(tz.th)}<br>JP: ${esc(tz.jp)}<br>SG: ${esc(tz.sg)}<br>US: ${esc(tz.us)}`
          : esc(log.storedAt || '-');
      const payload = log.payload || {};
      const jsonHeader = Object.keys(payload).join(', ');
      const jsonValue = JSON.stringify(payload, null, 2);
      const internalStatus = log.internalDeliveryStatus || '-';
      const internalLabel = internalStatus === 'ok' ? '성공' : internalStatus === 'fail' ? '실패' : internalStatus === 'skip' ? '미전송' : internalStatus;
      const internalClass = internalStatus === 'ok' ? 'status-ok' : internalStatus === 'fail' ? 'status-fail' : '';
      const canResend = log.internalTargetUrl && log.payload;
      const resendBtn = canResend
        ? `<form method="post" action="/admin/internal/resend" style="display:inline;" onsubmit="return confirm('해당 노티를 전산으로 다시 전송하시겠습니까?');"><input type="hidden" name="index" value="${realIndex}" /><button type="submit" class="btn-resend">재전송</button></form>`
        : '-';
      return `<tr>
        <td class="col-time" style="white-space:nowrap;font-size:11px;width:10%;">${timeHtml}</td>
        <td class="col-status" style="width:6%;"><span class="${internalClass}">${esc(internalLabel)}</span></td>
        <td class="col-header" style="width:24%;"><pre style="margin:0;white-space:pre-wrap;font-size:12px;">${esc(jsonHeader)}</pre></td>
        <td class="col-json" style="width:48%;"><pre style="margin:0;white-space:pre-wrap;font-size:12px;">${esc(jsonValue)}</pre></td>
        <td class="col-action" style="width:6%;">${resendBtn}</td>
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
    th, td { border: 1px solid #e5e7eb; padding: 8px 10px; font-size: 13px; vertical-align: top; }
    th { background: #e5f0ff; text-align: center; }
    tr:nth-child(even) { background:#f9fafb; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:260px; background:#111827; padding:20px 16px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:4px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:12px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:16px; padding:6px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:12px 4px 4px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav a { display:block; padding:8px 10px; margin-bottom:4px; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover { background:#1f2937; color:#e0f2fe; }
    .main { flex:1; display:flex; flex-direction:column; gap:16px; padding:16px 24px; box-sizing:border-box; }
    .topbar { background:#e0f2fe; border-radius:10px; padding:8px 14px; font-size:13px; color:#1e293b; display:flex; justify-content:space-between; align-items:center; border:1px solid #bae6fd; flex-wrap:wrap; }
    .topbar span { margin-right:12px; }
    .card { background:#fff; padding:16px 20px; border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,0.08); margin-bottom:8px; border:1px solid #e5e7eb; }
    pre { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; }
    .status-ok { color: #059669; font-weight: 600; }
    .status-fail { color: #dc2626; font-weight: 600; }
    .btn-resend { padding: 4px 10px; font-size: 12px; background: #2563eb; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
    .btn-resend:hover { background: #1d4ed8; }
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, now, adminUser, req.originalUrl)}
      <div class="card">
      <h1>${t(locale, 'internal_logs_title')} (${INTERNAL_LOGS.length})</h1>
      <p style="font-size:13px;color:#555;">${t(locale, 'internal_logs_desc')}</p>
      <table>
        <colgroup><col style="width:10%;" /><col style="width:6%;" /><col style="width:24%;" /><col style="width:48%;" /><col style="width:6%;" /></colgroup>
        <thead>
          <tr>
            <th>${t(locale, 'internal_logs_time')}</th>
            <th>전산 수신</th>
            <th>${t(locale, 'internal_logs_header')}</th>
            <th>${t(locale, 'internal_logs_value')}</th>
            <th>재전송</th>
          </tr>
        </thead>
        <tbody>
          ${rows || `<tr><td colspan="5" style="text-align:center;color:#777;">${t(locale, 'internal_logs_empty')}</td></tr>`}
        </tbody>
      </table>
      </div>
    </main>
  </div>
</body>
</html>`);
});

// 정산 노티 재전송 (전산으로 수동 재전송)
app.post('/admin/internal/resend', requireAuth, requirePage('internal_logs'), async (req, res) => {
  const index = parseInt(req.body.index, 10);
  if (Number.isNaN(index) || index < 0 || index >= INTERNAL_LOGS.length) {
    return res.redirect('/admin/internal?err=invalid');
  }
  const log = INTERNAL_LOGS[index];
  const url = log.internalTargetUrl;
  if (!url || !log.payload) {
    return res.redirect('/admin/internal?err=no_target');
  }
  try {
    const result = await sendToInternal(url, log.payload);
    return res.redirect('/admin/internal?resend=' + (result.success ? 'ok' : 'fail'));
  } catch (err) {
    return res.redirect('/admin/internal?resend=fail');
  }
});

// 트래픽 분석 (일자별 / 월간별 / 시간별)
function aggregateTraffic() {
  const byDay = {};
  const byMonth = {};
  const byHour = {};
  for (let i = 0; i < TRAFFIC_HITS.length; i++) {
    const hit = TRAFFIC_HITS[i];
    const d = new Date(hit.at);
    if (Number.isNaN(d.getTime())) continue;
    const dayKey = d.toISOString().slice(0, 10);
    const monthKey = d.toISOString().slice(0, 7);
    const hourKey = d.getUTCHours();
    byDay[dayKey] = (byDay[dayKey] || 0) + 1;
    byMonth[monthKey] = (byMonth[monthKey] || 0) + 1;
    byHour[hourKey] = (byHour[hourKey] || 0) + 1;
  }
  const dayEntries = Object.entries(byDay).sort((a, b) => b[0].localeCompare(a[0])).slice(0, 31);
  const monthEntries = Object.entries(byMonth).sort((a, b) => b[0].localeCompare(a[0])).slice(0, 12);
  const hourEntries = Array.from({ length: 24 }, (_, h) => [h, byHour[h] || 0]);
  const maxDay = Math.max(1, ...dayEntries.map((e) => e[1]));
  const maxMonth = Math.max(1, ...monthEntries.map((e) => e[1]));
  const maxHour = Math.max(1, ...hourEntries.map((e) => e[1]));
  return { dayEntries, monthEntries, hourEntries, maxDay, maxMonth, maxHour };
}

app.get('/admin/traffic', requireAuth, requirePage('traffic_analysis'), (req, res) => {
  const locale = getLocale(req);
  const clientIp = (req.headers['x-forwarded-for'] || '').toString().split(',')[0].trim() || req.ip || '';
  const adminUser = req.session.adminUser || '';
  const now = new Date().toLocaleString('ko-KR', { hour12: false });
  const { dayEntries, monthEntries, hourEntries, maxDay, maxMonth, maxHour } = aggregateTraffic();
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
  <title>트래픽분석</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background:#edf2f7; color:#111827; }
    .layout { display:flex; min-height:100vh; width:100%; gap:0; margin:0; }
    .sidebar { width:260px; background:#111827; padding:20px 16px; border-radius:0 10px 10px 0; box-shadow:0 10px 30px rgba(15,23,42,0.4); border-right:1px solid #1f2937; }
    .sidebar-title { font-weight:700; margin-bottom:4px; color:#f9fafb; font-size:18px; }
    .sidebar-sub { font-size:12px; color:#9ca3af; margin-bottom:12px; }
    .sidebar-user { font-size:13px; color:#e5e7eb; margin-bottom:16px; padding:6px 8px; background:#1f2937; border-radius:6px; }
    .nav-section-title { font-size:11px; font-weight:600; color:#6b7280; margin:12px 4px 4px; text-transform:uppercase; letter-spacing:0.08em; }
    .nav a { display:block; padding:8px 10px; margin-bottom:4px; color:#e5e7eb; text-decoration:none; font-size:14px; border-radius:6px; }
    .nav a:hover { background:#1f2937; color:#e0f2fe; }
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
  </style>
</head>
<body>
  <div class="layout">
    ${getAdminSidebar(locale, adminUser, req.session.member)}
    <main class="main">
      ${getAdminTopbar(locale, clientIp, now, adminUser, req.originalUrl)}
      <div class="card traffic-card">
        <h1>트래픽분석</h1>
        <p class="stat">총 요청 수 (최근 ${TRAFFIC_HITS_MAX}건 기준): <strong>${total}</strong></p>
        <h2>시간별 (UTC 0~23시)</h2>
        <div class="hour-grid">${hourGridCells}</div>
        <table style="margin-top:16px;"><thead><tr><th class="hour-label">시간</th><th class="hour-count">건수</th><th class="hour-bar">비율</th></tr></thead><tbody>${hourRows}</tbody></table>
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
