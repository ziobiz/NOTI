/**
 * 릭톤 (Likton) — 기본 색톤 팔레트 (JavaScript)
 * 페이지 접근 권한(계정관리) 화면 색상을 프로젝트 기본으로 정의합니다.
 * server.js 등에서 require('./likton-palette') 로 불러와 사용합니다.
 */
const LIKTON = {
  name: '릭톤',
  background: {
    page: '#FFFFFF',
    content: '#F9F9F9',
  },
  text: {
    primary: '#333333',
  },
  border: {
    default: '#DDDDDD',
    checkbox: '#CCCCCC',
  },
  section: {
    main: '#FFFAE4',
    logs: '#E2EFF9',
    transaction: '#E2F9E6',
    system: '#F0F2F5',
  },
  sectionBorder: {
    main: '#fcd34d',
    logs: '#7dd3fc',
    transaction: '#6ee7b7',
    system: '#cbd5e1',
  },
  button: {
    bg: '#FFFFFF',
    border: '#DDDDDD',
  },
  selected: {
    pink: { bg: '#FFF0F4', border: '#FFCCDA' },
    blue: { bg: '#EBF5FF', border: '#C2E0FF' },
    green: { bg: '#EEFFF0', border: '#D1FFDB' },
  },
};

module.exports = LIKTON;
