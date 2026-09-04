/**
 * NOTI 운영 매뉴얼 — ICOPAY chatbot merchant-manual 과 동일 커버·TOC·섹션 UI.
 * 결제대행사별 문서 + 환경설정 변경 버전 이력.
 */
const fs = require('fs');
const path = require('path');

const OPS_MANUAL_LIVE_VERSION = '1.0.0';

const THEME_CSS = `
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Malgun Gothic', 'Apple SD Gothic Neo', 'Noto Sans KR', 'Segoe UI', sans-serif; font-size: 11pt; line-height: 1.7; color: #1a1a1a; background: #f5f6fa; }
  .page-wrap { max-width: 860px; margin: 32px auto; background: #fff; border-radius: 10px; box-shadow: 0 2px 16px rgba(0,0,0,.10); overflow: hidden; }
  .cover { background: linear-gradient(135deg, #1a3a5c 0%, #1565c0 60%, #1e88e5 100%); color: #fff; padding: 48px 52px 40px; display: flex; align-items: flex-start; justify-content: space-between; gap: 32px; }
  .cover-body { flex: 1; }
  .cover .logo-line { font-size: 13pt; font-weight: 700; letter-spacing: 2px; opacity: .85; margin-bottom: 14px; }
  .cover h1 { font-size: 26pt; font-weight: 900; line-height: 1.25; margin-bottom: 10px; }
  .cover .subtitle { font-size: 11.5pt; opacity: .82; margin-bottom: 24px; }
  .cover .meta { font-size: 9.5pt; opacity: .65; border-top: 1px solid rgba(255,255,255,.25); padding-top: 14px; }
  .body { padding: 44px 52px 60px; }
  .toc { background: #f0f4ff; border-left: 4px solid #1565c0; border-radius: 0 8px 8px 0; padding: 20px 26px; margin-bottom: 40px; }
  .toc h2 { font-size: 12pt; font-weight: 700; color: #1565c0; margin-bottom: 12px; }
  .toc ol { padding-left: 20px; }
  .toc li { font-size: 10pt; line-height: 2.1; color: #1a3a5c; }
  .toc a { color: #1565c0; text-decoration: none; }
  .toc a:hover { text-decoration: underline; }
  h2.section-title { font-size: 15pt; font-weight: 800; color: #1a3a5c; border-bottom: 2.5px solid #1565c0; padding-bottom: 7px; margin: 42px 0 16px; page-break-after: avoid; }
  h3.sub-title { font-size: 12pt; font-weight: 700; color: #1565c0; margin: 24px 0 10px; page-break-after: avoid; }
  p { margin-bottom: 10px; }
  .info-box { background: #e3f0ff; border: 1px solid #90caf9; border-left: 4px solid #1565c0; border-radius: 6px; padding: 12px 16px; margin: 14px 0; font-size: 10pt; color: #1a3a5c; }
  .warn-box { background: #fff8e1; border: 1px solid #ffe082; border-left: 4px solid #f9a825; border-radius: 6px; padding: 12px 16px; margin: 14px 0; font-size: 10pt; color: #5d4037; }
  .check-box { background: #e8f5e9; border: 1px solid #a5d6a7; border-left: 4px solid #2e7d32; border-radius: 6px; padding: 12px 16px; margin: 14px 0; font-size: 10pt; color: #1b5e20; }
  table { width: 100%; border-collapse: collapse; margin: 14px 0 20px; font-size: 10pt; page-break-inside: avoid; }
  th { background: #1565c0; color: #fff; font-weight: 700; padding: 9px 12px; text-align: left; border: 1px solid #1245a0; }
  td { padding: 8px 12px; border: 1px solid #cfd8dc; vertical-align: top; }
  tr:nth-child(even) td { background: #f4f7ff; }
  .menu-path { display: inline-block; background: #ececec; border: 1px solid #d0d0d0; border-radius: 5px; padding: 3px 11px; font-size: 9.5pt; color: #37474f; font-weight: 600; margin-bottom: 12px; }
  .footer { background: #1a3a5c; color: rgba(255,255,255,.65); text-align: center; font-size: 9pt; padding: 18px 24px; }
  ul { padding-left: 20px; margin-bottom: 12px; }
  li { margin-bottom: 4px; }
  code { background: #e8edf5; color: #c0392b; font-family: Consolas, monospace; font-size: 9pt; padding: 1px 5px; border-radius: 3px; }
  .print-btn { position: fixed; bottom: 28px; right: 28px; z-index: 9999; background: #1565c0; color: #fff; border: none; border-radius: 10px; padding: 11px 20px; font-size: 10.5pt; font-weight: 700; cursor: pointer; box-shadow: 0 4px 16px rgba(21,101,192,.45); }
  @media print {
    body { background: #fff; }
    .page-wrap { max-width: 100%; margin: 0; border-radius: 0; box-shadow: none; }
    .print-btn, .footer { display: none !important; }
    @page { size: A4; margin: 14mm 12mm; }
  }
`;

function ui(locale) {
  const L = String(locale || 'ko').toLowerCase();
  if (L === 'en') {
    return {
      print: 'Print / Save PDF',
      toc: 'Contents',
      logoLine: 'OTL · ICOPAY NOTI',
      footer: 'OTL ICOPAY NOTI &nbsp;|&nbsp; Operations Manual',
      versionLabel: 'Document version',
      changelogTitle: 'Settings change history (versioned)',
      changelogEmpty: 'No settings changes recorded yet.',
      colWhen: 'When',
      colVer: 'Ver.',
      colActor: 'By',
      colDetail: 'Change',
    };
  }
  if (L === 'ja') {
    return {
      print: '印刷 / PDF保存',
      toc: '目次',
      logoLine: 'OTL · ICOPAY NOTI',
      footer: 'OTL ICOPAY NOTI &nbsp;|&nbsp; 運用マニュアル',
      versionLabel: 'ドキュメント版',
      changelogTitle: '環境設定変更履歴（バージョン）',
      changelogEmpty: 'まだ設定変更が記録されていません。',
      colWhen: '日時',
      colVer: '版',
      colActor: '実行者',
      colDetail: '内容',
    };
  }
  if (L === 'zh') {
    return {
      print: '打印 / 保存 PDF',
      toc: '目录',
      logoLine: 'OTL · ICOPAY NOTI',
      footer: 'OTL ICOPAY NOTI &nbsp;|&nbsp; 运营手册',
      versionLabel: '文档版本',
      changelogTitle: '环境设置变更记录（版本）',
      changelogEmpty: '暂无设置变更记录。',
      colWhen: '时间',
      colVer: '版本',
      colActor: '操作者',
      colDetail: '内容',
    };
  }
  if (L === 'th') {
    return {
      print: 'พิมพ์ / บันทึก PDF',
      toc: 'สารบัญ',
      logoLine: 'OTL · ICOPAY NOTI',
      footer: 'OTL ICOPAY NOTI &nbsp;|&nbsp; คู่มือปฏิบัติการ',
      versionLabel: 'เวอร์ชันเอกสาร',
      changelogTitle: 'ประวัติการเปลี่ยนการตั้งค่า (เวอร์ชัน)',
      changelogEmpty: 'ยังไม่มีการบันทึกการเปลี่ยนแปลง',
      colWhen: 'เวลา',
      colVer: 'เวอร์ชัน',
      colActor: 'โดย',
      colDetail: 'รายละเอียด',
    };
  }
  return {
    print: '인쇄 / PDF 저장',
    toc: '목 차',
    logoLine: 'OTL · ICOPAY NOTI',
    footer: 'OTL ICOPAY NOTI &nbsp;|&nbsp; 운영 매뉴얼',
    versionLabel: '문서 버전',
    changelogTitle: '환경설정 변경 이력 (버전)',
    changelogEmpty: '아직 기록된 환경설정 변경이 없습니다.',
    colWhen: '시각',
    colVer: '버전',
    colActor: '실행자',
    colDetail: '내용',
  };
}

/** @returns {{ id: string, pg: string, title: Record<string,string>, subtitle: Record<string,string> }[]} */
function getManualCatalog() {
  return [
    {
      id: 'common',
      pg: 'common',
      title: {
        ko: '공통 운영 매뉴얼',
        en: 'Common Operations Manual',
        ja: '共通運用マニュアル',
        zh: '共通运营手册',
        th: 'คู่มือปฏิบัติการร่วม',
      },
      subtitle: {
        ko: '모든 결제대행사 공통 — 노티 수신·전산·개발·로그',
        en: 'Shared across PGs — notify, internal, logs',
        ja: '全PG共通 — 通知・基幹・ログ',
        zh: '各支付机构共通 — 通知、内部、日志',
        th: 'ใช้ร่วมทุก PG — แจ้งเตือน ระบบภายใน ล็อก',
      },
    },
    {
      id: 'chillpay',
      pg: 'chillpay',
      title: {
        ko: 'ChillPay 운영 매뉴얼',
        en: 'ChillPay Operations Manual',
        ja: 'ChillPay 運用マニュアル',
        zh: 'ChillPay 运营手册',
        th: 'คู่มือ ChillPay',
      },
      subtitle: {
        ko: 'Route·피지거래·취소/무효/환불 API',
        en: 'Routes, PG sync, cancel/void/refund API',
        ja: 'Route・PG同期・取消/無効/返金',
        zh: 'Route、PG同步、取消/无效/退款',
        th: 'Route ซิงก์ PG ยกเลิก/โมฆะ/คืนเงิน',
      },
    },
    {
      id: 'jpay',
      pg: 'jpay',
      title: {
        ko: 'JPAY 운영 매뉴얼',
        en: 'JPAY Operations Manual',
        ja: 'JPAY 運用マニュアル',
        zh: 'JPAY 运营手册',
        th: 'คู่มือ JPAY',
      },
      subtitle: {
        ko: 'MID·jN 슬롯·후속관리',
        en: 'MID, jN slots, follow-up',
        ja: 'MID・jNスロット・後続',
        zh: 'MID、jN 槽位、后续管理',
        th: 'MID ช่อง jN การติดตาม',
      },
    },
    {
      id: 'elementpay',
      pg: 'elementpay',
      title: {
        ko: 'ElementPay 운영 매뉴얼',
        en: 'ElementPay Operations Manual',
        ja: 'ElementPay 運用マニュアル',
        zh: 'ElementPay 运营手册',
        th: 'คู่มือ ElementPay',
      },
      subtitle: {
        ko: '고정 Webhook·Result·Comp-Id(MID)·Response',
        en: 'Fixed webhook/result, Comp-Id (MID), Response',
        ja: '固定Webhook/Result・Comp-Id(MID)・Response',
        zh: '固定 Webhook/Result、Comp-Id(MID)、Response',
        th: 'Webhook/Result คงที่ Comp-Id(MID) Response',
      },
    },
  ];
}

function pickLocaleMap(map, locale) {
  const L = String(locale || 'ko').toLowerCase();
  if (map[L]) return map[L];
  if (L.startsWith('zh') && map.zh) return map.zh;
  return map.en || map.ko || '';
}

function esc(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function sectionsFor(id, locale) {
  const L = String(locale || 'ko').toLowerCase();
  const useEn = L !== 'ko' && L !== 'ja' && L !== 'zh' && L !== 'th';
  // Prefer ko body; en for English; others fall back to en then ko
  const pack = CONTENT[id];
  if (!pack) return [];
  if (L === 'ko') return pack.ko;
  if (L === 'ja' && pack.ja) return pack.ja;
  if (L === 'zh' && pack.zh) return pack.zh;
  if (L === 'th' && pack.th) return pack.th;
  return pack.en || pack.ko;
}

const CONTENT = {
  common: {
    ko: [
      {
        id: 's1',
        title: '1. 이 매뉴얼의 범위',
        html: `<p>이 문서는 <strong>OTL ICOPAY NOTI</strong> 미들웨어의 <strong>공통</strong> 운영 절차를 다룹니다. ChillPay·JPAY·ElementPay 모두 동일한 노티 수신·전산/개발 전달·로그 조회 흐름을 사용합니다.</p>
<div class="info-box">개별 PG 전용(피지 API 동기화, JPAY 후속, EP Comp-Id 등)은 각 PG 매뉴얼을 보세요.</div>`,
      },
      {
        id: 's2',
        title: '2. 노티 수신 구조',
        html: `<p class="menu-path">메뉴: 로그분석 · 종합거래</p>
<ul>
<li><strong>피지 노티 / 피지 결과</strong> — PG가 NOTI로 보낸 웹훅·브라우저 복귀 원문</li>
<li><strong>전산 노티 / 전산 결과</strong> — B형 JSON으로 전산 URL 전송 시도 기록</li>
<li><strong>개발 노티 / 개발 결과</strong> — 개발 URL 전송 시도 기록</li>
<li><strong>노티거래내역 · 일일노티내역</strong> — 수신 노티 기준 거래·일별 집계</li>
</ul>
<div class="warn-box">피지거래내역(ChillPay Search API)과 노티거래내역은 데이터 출처가 다릅니다. 건수 불일치는 웹훅 미수신일 수 있습니다.</div>`,
      },
      {
        id: 's3',
        title: '3. 가맹점 · 전산 대상',
        html: `<p class="menu-path">메뉴: 신규등록 · 노티설정</p>
<ol>
<li>전산 대상(Callback/Result URL)을 등록합니다.</li>
<li>가맹점을 등록하고 전산 대상·릴레이·개발 노티 사용 여부를 설정합니다.</li>
<li>PG 측에 NOTI 수신 URL을 등록합니다 (PG별 매뉴얼 참고).</li>
</ol>`,
      },
      {
        id: 's4',
        title: '4. 환경설정과 버전',
        html: `<p class="menu-path">메뉴: 시스템 → 환경 설정 · 이용매뉴얼</p>
<p>사이트·ChillPay·JPAY·노티 관련 설정을 저장하면 <strong>운영 매뉴얼 버전 이력</strong>에 자동 기록됩니다. 매뉴얼 하단·허브의 버전 배지로 확인할 수 있습니다.</p>
<div class="check-box">설정 변경 후 반드시 매뉴얼 변경 이력을 확인해 운영 인수인계에 반영하세요.</div>`,
      },
    ],
    en: [
      {
        id: 's1',
        title: '1. Scope',
        html: `<p>This document covers <strong>shared</strong> operations of the OTL ICOPAY NOTI middleware used by ChillPay, JPAY, and ElementPay.</p>`,
      },
      {
        id: 's2',
        title: '2. Notify pipeline',
        html: `<ul>
<li><strong>PG noti / result</strong> — inbound webhooks and browser returns</li>
<li><strong>Internal / Dev</strong> — outbound delivery attempts</li>
<li><strong>Transaction list / daily noti</strong> — lists based on received notifies</li>
</ul>`,
      },
      {
        id: 's3',
        title: '3. Merchants & targets',
        html: `<p>Register internal targets, then merchants, then configure PG notify URLs (see each PG manual).</p>`,
      },
      {
        id: 's4',
        title: '4. Settings versioning',
        html: `<p>Saving site/ChillPay/JPAY/noti settings appends a <strong>versioned changelog</strong> shown in this manual hub.</p>`,
      },
    ],
  },
  chillpay: {
    ko: [
      {
        id: 's1',
        title: '1. ChillPay 개요',
        html: `<p>ChillPay는 <strong>Route No(1~50)</strong>로 callback/result URL이 구분됩니다. 가맹점 등록 시 PG callback 번호에 맞춰 RouteNo가 연동됩니다.</p>
<div class="info-box">수신 예: <code>/noti/callback/N</code>, <code>/noti/result/N</code></div>`,
      },
      {
        id: 's2',
        title: '2. 피지거래 · 일일피지',
        html: `<p class="menu-path">종합거래 → 피지거래내역 / 일일피지내역</p>
<p>ChillPay Payment Search API 동기화 결과입니다. 주기 동기화·수동 동기화를 사용합니다. 노티 미수신 건도 여기서 보일 수 있습니다.</p>`,
      },
      {
        id: 's3',
        title: '3. 취소 · 무효 · 환불',
        html: `<p class="menu-path">종합거래 → 취소/무효/환불</p>
<ul>
<li>취소 — PG 취소 노티 수신 목록</li>
<li>무효 — 당일 컷오프 전 API 또는 이메일 요청</li>
<li>환불 — 컷오프 이후·환불 가능 일수 내</li>
</ul>
<div class="warn-box">컷오프·타임존은 환경설정(ChillPay)의 표준/운영 타임존을 따릅니다.</div>`,
      },
    ],
    en: [
      {
        id: 's1',
        title: '1. Overview',
        html: `<p>ChillPay uses <strong>Route No (1–50)</strong> for callback/result URLs.</p>`,
      },
      {
        id: 's2',
        title: '2. PG transaction sync',
        html: `<p>Payment Search API sync — separate from notify logs.</p>`,
      },
      {
        id: 's3',
        title: '3. Cancel / void / refund',
        html: `<p>Use the cancel-refund menus; windows follow ChillPay timezone settings.</p>`,
      },
    ],
  },
  jpay: {
    ko: [
      {
        id: 's1',
        title: '1. JPAY 개요',
        html: `<p>JPAY API 식별자는 <strong>MID</strong>입니다. ChillPay Route와 다릅니다. 미들웨어는 <code>j1~j20</code> 수신 슬롯 URL을 사용합니다.</p>
<div class="info-box">수신 예: <code>/noti/callback/jN</code>, <code>/noti/result/jN</code> (본문 <code>memberid</code>로 프로필 매칭)</div>`,
      },
      {
        id: 's2',
        title: '2. MID · 프로필',
        html: `<p class="menu-path">시스템 → 환경 설정 → JPAY 환경 설정</p>
<p>운영/샌드박스 MID·API Key를 슬롯에 매핑합니다. 가맹점 등록에서 jN 슬롯을 선택합니다.</p>`,
      },
      {
        id: 's3',
        title: '3. 목록 표시',
        html: `<p>노티거래내역·결과 화면에서 JPAY/EP는 열 이름이 <strong>Response</strong>·<strong>MID</strong>로 표시됩니다.</p>
<p class="menu-path">종합거래 → JPAY후속관리</p>`,
      },
    ],
    en: [
      {
        id: 's1',
        title: '1. Overview',
        html: `<p>JPAY identifies merchants by <strong>MID</strong>. Receive slots are <code>j1–j20</code>.</p>`,
      },
      {
        id: 's2',
        title: '2. Profiles',
        html: `<p>Configure MID/API key per slot under Settings → JPAY.</p>`,
      },
      {
        id: 's3',
        title: '3. UI labels',
        html: `<p>Transaction/result lists use <strong>Response</strong> and <strong>MID</strong> columns for JPAY.</p>`,
      },
    ],
  },
  elementpay: {
    ko: [
      {
        id: 's1',
        title: '1. ElementPay 개요',
        html: `<p>ElementPay는 <strong>본사 고정</strong> 입구를 사용합니다. ChillPay Route / JPAY jN 슬롯이 없습니다.</p>
<div class="info-box">Webhook: <code>/noti/elementpay</code> · Result: <code>/noti/result/elementpay</code></div>
<p>가맹 식별은 ICOPAY <strong>Comp-Id</strong>(목록에서는 <strong>MID</strong>)입니다. Cabinet Webhooks에 NOTI URL을 등록해야 로그가 쌓입니다.</p>`,
      },
      {
        id: 's2',
        title: '2. 상태 · Response',
        html: `<ul>
<li>성공 <code>status=205</code> · 실패 <code>204</code> 등</li>
<li>목록 Route 열은 <strong>Response</strong>(예: webhook / result)로 표시</li>
<li>일일노티는 unix <code>timestamp</code>·수신일 폴백으로 집계</li>
</ul>`,
      },
      {
        id: 's3',
        title: '3. 운영 체크리스트',
        html: `<ol>
<li>가맹점 등록(ElementPay) + 전산 대상 연결</li>
<li>EP Cabinet → Webhook URL = NOTI <code>/noti/elementpay</code></li>
<li>ICOPAY ingress URL 환경 설정 확인</li>
<li>테스트 결제 후 노티거래내역(ElementPay)·MID·Response 확인</li>
</ol>
<div class="warn-box">Comp-Id가 ICOPAY에서 오지 않으면 MID가 비어 보일 수 있습니다. 주문 조회·형제 로그로 보강합니다.</div>`,
      },
    ],
    en: [
      {
        id: 's1',
        title: '1. Overview',
        html: `<p>Fixed ingress <code>/noti/elementpay</code> and <code>/noti/result/elementpay</code>. Identify merchants by Comp-Id (shown as <strong>MID</strong>).</p>`,
      },
      {
        id: 's2',
        title: '2. Status & Response',
        html: `<p>Success often <code>205</code>; list column is <strong>Response</strong> (webhook/result).</p>`,
      },
      {
        id: 's3',
        title: '3. Checklist',
        html: `<p>Register merchant, set Cabinet webhook to NOTI, verify ICOPAY ingress, test payment.</p>`,
      },
    ],
  },
};

function defaultVersionState() {
  return {
    liveVersion: OPS_MANUAL_LIVE_VERSION,
    changes: [],
  };
}

function loadVersionState(versionFilePath) {
  try {
    const raw = fs.readFileSync(versionFilePath, 'utf8');
    const o = JSON.parse(raw);
    if (!o || typeof o !== 'object') return defaultVersionState();
    return {
      liveVersion: String(o.liveVersion || OPS_MANUAL_LIVE_VERSION),
      changes: Array.isArray(o.changes) ? o.changes : [],
    };
  } catch (_) {
    return defaultVersionState();
  }
}

function saveVersionState(versionFilePath, state) {
  const dir = path.dirname(versionFilePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(versionFilePath, JSON.stringify(state, null, 2), 'utf8');
}

/**
 * 환경설정 저장 시 호출 — patch 버전 증가 + 이력 추가
 * @returns {{ liveVersion: string, entry: object }}
 */
function recordSettingsChange(versionFilePath, { action, detail, actor, localeNotes }) {
  const state = loadVersionState(versionFilePath);
  const parts = String(state.liveVersion || '1.0.0')
    .split('.')
    .map((x) => parseInt(x, 10) || 0);
  while (parts.length < 3) parts.push(0);
  parts[2] += 1;
  const next = parts.join('.');
  const entry = {
    version: next,
    at: new Date().toISOString(),
    action: String(action || 'settings').slice(0, 80),
    detail: String(detail || '').slice(0, 500),
    actor: String(actor || 'unknown').slice(0, 80),
    notes: localeNotes && typeof localeNotes === 'object' ? localeNotes : undefined,
  };
  state.liveVersion = next;
  state.changes = [entry].concat(state.changes || []).slice(0, 500);
  saveVersionState(versionFilePath, state);
  return { liveVersion: next, entry };
}

function buildChangelogSectionHtml(locale, versionFilePath) {
  const u = ui(locale);
  const state = loadVersionState(versionFilePath);
  const rows = (state.changes || []).slice(0, 40);
  if (!rows.length) {
    return `<h2 class="section-title" id="schange">${esc(u.changelogTitle)}</h2><p>${esc(u.changelogEmpty)}</p>`;
  }
  const tr = rows
    .map(
      (r) =>
        `<tr><td>${esc(r.at || '')}</td><td class="center">${esc(r.version || '')}</td><td>${esc(r.actor || '')}</td><td>${esc(r.detail || r.action || '')}</td></tr>`,
    )
    .join('');
  return `<h2 class="section-title" id="schange">${esc(u.changelogTitle)}</h2>
<table><thead><tr><th>${esc(u.colWhen)}</th><th class="center">${esc(u.colVer)}</th><th>${esc(u.colActor)}</th><th>${esc(u.colDetail)}</th></tr></thead>
<tbody>${tr}</tbody></table>`;
}

function buildManualHtml(id, locale, { siteTitle, versionFilePath, liveVersion } = {}) {
  const catalog = getManualCatalog().find((c) => c.id === id);
  if (!catalog) return null;
  const u = ui(locale);
  const title = pickLocaleMap(catalog.title, locale);
  const subtitle = pickLocaleMap(catalog.subtitle, locale);
  const sections = sectionsFor(id, locale);
  const ver = liveVersion || (versionFilePath ? loadVersionState(versionFilePath).liveVersion : OPS_MANUAL_LIVE_VERSION);
  const toc = sections
    .map((s, i) => `<li><a href="#${esc(s.id)}">${esc(s.title)}</a></li>`)
    .concat([`<li><a href="#schange">${esc(u.changelogTitle)}</a></li>`])
    .join('');
  const body = sections.map((s) => `<h2 class="section-title" id="${esc(s.id)}">${esc(s.title)}</h2>${s.html}`).join('\n');
  const changeHtml = versionFilePath ? buildChangelogSectionHtml(locale, versionFilePath) : '';
  const brand = siteTitle || 'OTL ICOPAY NOTI';
  return `<!DOCTYPE html>
<html lang="${esc(String(locale || 'ko').slice(0, 2))}">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>${esc(title)} V${esc(ver)}</title>
<style>${THEME_CSS}</style>
</head>
<body>
<button class="print-btn" onclick="window.print()">${esc(u.print)}</button>
<div class="page-wrap">
  <div class="cover">
    <div class="cover-body">
      <div class="logo-line">${esc(u.logoLine)}</div>
      <h1>${esc(title)}</h1>
      <div class="subtitle">${esc(subtitle)}</div>
      <div class="meta">${esc(brand)} · ${esc(u.versionLabel)} V${esc(ver)} · ${esc(new Date().toISOString().slice(0, 10))}</div>
    </div>
  </div>
  <div class="body">
    <div class="toc"><h2>${esc(u.toc)}</h2><ol>${toc}</ol></div>
    ${body}
    ${changeHtml}
  </div>
  <div class="footer">${u.footer}</div>
</div>
</body>
</html>`;
}

module.exports = {
  OPS_MANUAL_LIVE_VERSION,
  getManualCatalog,
  pickLocaleMap,
  loadVersionState,
  recordSettingsChange,
  buildManualHtml,
  ui,
};
