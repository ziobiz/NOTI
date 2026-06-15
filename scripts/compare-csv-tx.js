const fs = require('fs');

function parseCsv(text) {
  const rows = [];
  let i = 0;
  let field = '';
  let row = [];
  let inQ = false;
  const s = text.replace(/^\uFEFF/, '');
  while (i < s.length) {
    const c = s[i];
    if (inQ) {
      if (c === '"' && s[i + 1] === '"') {
        field += '"';
        i += 2;
        continue;
      }
      if (c === '"') {
        inQ = false;
        i++;
        continue;
      }
      field += c;
      i++;
      continue;
    }
    if (c === '"') {
      inQ = true;
      i++;
      continue;
    }
    if (c === ',') {
      row.push(field);
      field = '';
      i++;
      continue;
    }
    if (c === '\n' || (c === '\r' && s[i + 1] === '\n')) {
      row.push(field);
      if (row.some((x) => x !== '')) rows.push(row);
      row = [];
      field = '';
      i += c === '\r' ? 2 : 1;
      continue;
    }
    field += c;
    i++;
  }
  if (field || row.length) {
    row.push(field);
    if (row.some((x) => x !== '')) rows.push(row);
  }
  return rows;
}

const notiPath = process.argv[2];
const pgPath = process.argv[3];
const notiRows = parseCsv(fs.readFileSync(notiPath, 'utf8'));
const pgRows = parseCsv(fs.readFileSync(pgPath, 'utf8'));
const notiH = notiRows[0];
const pgH = pgRows[0];
const ni = (name) => notiH.indexOf(name);
const pi = (name) => pgH.indexOf(name);

const notiByTx = new Map();
for (let r = 1; r < notiRows.length; r++) {
  const tx = String(notiRows[r][ni('TransactionId')] || '').trim();
  if (!tx) continue;
  notiByTx.set(tx, {
    status: notiRows[r][ni('상태')] || '',
    statusRaw: notiRows[r][ni('status')] || '',
    date: notiRows[r][ni('수신일')] || '',
    order: notiRows[r][ni('OrderNo')] || '',
    amt: notiRows[r][ni('Amount')] || '',
  });
}

const pgByTx = new Map();
for (let r = 1; r < pgRows.length; r++) {
  const tx = String(pgRows[r][pi('TransactionId')] || '').trim();
  if (!tx) continue;
  pgByTx.set(tx, {
    status: pgRows[r][pi('Status')] || '',
    date: pgRows[r][pi('거래일시')] || '',
    order: pgRows[r][pi('OrderNo')] || '',
    amt: pgRows[r][pi('Amount')] || '',
  });
}

const onlyPg = [...pgByTx.keys()].filter((k) => !notiByTx.has(k));
const onlyNoti = [...notiByTx.keys()].filter((k) => !pgByTx.has(k));
const both = [...pgByTx.keys()].filter((k) => notiByTx.has(k));

const notiBreakdown = {};
for (const v of notiByTx.values()) notiBreakdown[v.status] = (notiBreakdown[v.status] || 0) + 1;
const pgBreakdown = {};
for (const v of pgByTx.values()) pgBreakdown[v.status] = (pgBreakdown[v.status] || 0) + 1;

const pgSuccessNotiNotPaid = [];
const notiPaidPgNotSuccess = [];
for (const tx of both) {
  const n = notiByTx.get(tx);
  const p = pgByTx.get(tx);
  const nPaid = n.status === '결제';
  const pSuccess = /^success$/i.test(p.status);
  if (pSuccess && !nPaid) pgSuccessNotiNotPaid.push({ tx, noti: n.status, pg: p.status, order: p.order });
  if (nPaid && !pSuccess) notiPaidPgNotSuccess.push({ tx, noti: n.status, pg: p.status, order: p.order });
}

function sampleOnlyPg(tx) {
  const p = pgByTx.get(tx);
  return { tx, status: p.status, order: p.order, date: p.date };
}

const onlyPgSuccessList = onlyPg.filter((tx) => /^success$/i.test(pgByTx.get(tx).status));
const onlyNoti결제 = onlyNoti.filter((tx) => notiByTx.get(tx).status === '결제');
const bothPgSuccess = both.filter((tx) => /^success$/i.test(pgByTx.get(tx).status));
const bothNoti결제 = both.filter((tx) => notiByTx.get(tx).status === '결제');

const notiDates = {};
for (const v of notiByTx.values()) notiDates[v.date] = (notiDates[v.date] || 0) + 1;
const pgDatePrefix = {};
for (const v of pgByTx.values()) {
  const d = (v.date || '').split(' ')[0] || '?';
  pgDatePrefix[d] = (pgDatePrefix[d] || 0) + 1;
}

console.log(
  JSON.stringify(
    {
      notiTotal: notiByTx.size,
      pgTotal: pgByTx.size,
      inBoth: both.length,
      onlyInPg: onlyPg.length,
      onlyInNoti: onlyNoti.length,
      notiSuccess결제: notiBreakdown['결제'] || 0,
      pgSuccess: pgBreakdown['Success'] || 0,
      gapSuccess610minus591: (pgBreakdown['Success'] || 0) - (notiBreakdown['결제'] || 0),
      notiBreakdown,
      pgBreakdown,
      both_pgSuccess_notiNot결제: pgSuccessNotiNotPaid.length,
      both_noti결제_pgNotSuccess: notiPaidPgNotSuccess.length,
      both_count_pgSuccess: bothPgSuccess.length,
      both_count_noti결제: bothNoti결제.length,
      onlyPgSuccess: onlyPgSuccessList.length,
      onlyNoti결제: onlyNoti결제.length,
      onlyPg_byStatus: onlyPg.reduce((acc, tx) => {
        const s = pgByTx.get(tx).status;
        acc[s] = (acc[s] || 0) + 1;
        return acc;
      }, {}),
      notiDateDistribution: notiDates,
      pgDateDistributionTop: pgDatePrefix,
      sampleOnlyPgSuccess: onlyPgSuccessList.slice(0, 15).map(sampleOnlyPg),
      sampleOnlyNoti결제: onlyNoti결제.slice(0, 15).map((tx) => {
        const n = notiByTx.get(tx);
        return { tx, status: n.status, order: n.order, date: n.date };
      }),
    },
    null,
    2,
  ),
);
