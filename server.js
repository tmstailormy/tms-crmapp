const express = require('express');
const { google } = require('googleapis');
const path = require('path');

const app = express();

// CORS — allow GitHub Pages origin
app.use((req, res, next) => {
  const allowed = ['https://tmstailormy.github.io', 'http://localhost:3000'];
  const origin = req.headers.origin;
  if (allowed.includes(origin)) res.setHeader('Access-Control-Allow-Origin', origin);
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname)));

const SHEET_ID = '1v5omEWDAVhBhLjake1NeFB7T3Ud7W43si7Mcx2vfgL4';

// Canonical 22-column layout (A–V):
// [0]ID [1]Name [2]Short Name [3]Type [4]District [5]Division
// [6]Website [7]Address [8]Phone [9]Phone2 [10]Fax [11]Email
// [12]Admin Email [13]Procurement Email [14]Status [15]PIC
// [16]PIC Phone [17]PIC Email [18]PIC Title [19]Notes [20]Last Action Date [21]Log
const AGENCY_HEADERS = [
  'ID', 'Name', 'Short Name', 'Type', 'District', 'Division',
  'Website', 'Address', 'Phone', 'Phone2', 'Fax', 'Email',
  'Admin Email', 'Procurement Email', 'Status', 'PIC',
  'PIC Phone', 'PIC Email', 'PIC Title', 'Notes', 'Last Action Date', 'Log'
];

// Build a canonical row array from agency static data + CRM data.
function buildAgencyRow(id, agency, crmData) {
  const log = Array.isArray(crmData.log) ? crmData.log.slice(0, 50) : [];
  return [
    id,
    agency.name             || '',
    agency.short            || '',
    agency.type             || '',
    agency.district || agency.city || '',
    agency.division         || '',
    agency.website          || '',
    agency.address          || '',
    agency.phone            || '',
    agency.phone2           || '',
    agency.fax              || '',
    agency.email            || '',
    agency.adminEmail       || '',
    agency.procurementEmail || '',
    crmData.status          || 'Not Contacted',
    crmData.pic             || '',
    crmData.picPhone        || '',
    crmData.picEmail        || '',
    crmData.picTitle        || '',
    crmData.notes           || '',
    crmData.date            || '',
    JSON.stringify(log)
  ];
}

// ── AUTH ────────────────────────────────────────────────────
async function getSheets() {
  const scopes = ['https://www.googleapis.com/auth/spreadsheets'];
  const authConfig = process.env.GOOGLE_CREDENTIALS
    ? { credentials: JSON.parse(process.env.GOOGLE_CREDENTIALS), scopes }
    : { keyFile: path.join(__dirname, 'credentials.json'), scopes };
  const auth = new google.auth.GoogleAuth(authConfig);
  const client = await auth.getClient();
  return google.sheets({ version: 'v4', auth: client });
}

// ── INIT: create tabs + headers ─────────────────────────────
app.all('/api/init', async (req, res) => {
  try {
    const sheets = await getSheets();

    const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
    const existingTitles = meta.data.sheets.map(s => s.properties.title);

    const tabs = [
      { name: 'Agencies',       headers: AGENCY_HEADERS },
      { name: 'Activity',       headers: ['Timestamp', 'Message'] },
      { name: 'Templates',      headers: ['ID', 'Name', 'Subject', 'Body', 'Created'] },
      { name: 'CustomAgencies', headers: ['ID', 'Data'] }
    ];

    // Create missing tabs
    const addRequests = tabs
      .filter(t => !existingTitles.includes(t.name))
      .map(t => ({ addSheet: { properties: { title: t.name } } }));

    if (addRequests.length > 0) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        resource: { requests: addRequests }
      });
    }

    // Write headers if row 1 is empty
    for (const tab of tabs) {
      const check = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: `${tab.name}!A1:V1`
      });
      if (!check.data.values || check.data.values.length === 0) {
        await sheets.spreadsheets.values.update({
          spreadsheetId: SHEET_ID,
          range: `${tab.name}!A1`,
          valueInputOption: 'RAW',
          resource: { values: [tab.headers] }
        });
      }
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('[init]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── CRM: read all rows from Agencies sheet ──────────────────
app.get('/api/crm', async (req, res) => {
  try {
    const sheets = await getSheets();
    const result = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Agencies!A2:V'
    });

    const rows = result.data.values || [];
    const crm = {};

    rows.forEach(row => {
      const id = parseInt(row[0]);
      if (!id) return;
      let log = [];
      try { if (row[21]) log = JSON.parse(row[21]); } catch (e) {}
      crm[id] = {
        status:   row[14] || 'Not Contacted',
        pic:      row[15] || '',
        picPhone: row[16] || '',
        picEmail: row[17] || '',
        picTitle: row[18] || '',
        notes:    row[19] || '',
        date:     row[20] || '',
        log
      };
    });

    res.json(crm);
  } catch (err) {
    console.error('[crm GET]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── CRM: upsert a single agency row ────────────────────────
app.post('/api/crm/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    const { agency, crmData } = req.body;
    const sheets = await getSheets();

    // Find existing row index
    const colA = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Agencies!A:A'
    });
    const ids = (colA.data.values || []);
    let rowIndex = -1;
    for (let i = 1; i < ids.length; i++) {
      if (parseInt(ids[i][0]) === id) { rowIndex = i + 1; break; }
    }

    const rowData = buildAgencyRow(id, agency, crmData);

    if (rowIndex > 0) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `Agencies!A${rowIndex}`,
        valueInputOption: 'RAW',
        resource: { values: [rowData] }
      });
    } else {
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: 'Agencies!A:V',
        valueInputOption: 'RAW',
        resource: { values: [rowData] }
      });
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('[crm POST]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── CRM: delete a single row by agency ID ──────────────────
app.delete('/api/crm/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    const sheets = await getSheets();

    const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
    const agenciesSheet = meta.data.sheets.find(s => s.properties.title === 'Agencies');
    if (!agenciesSheet) return res.json({ ok: true });
    const sheetId = agenciesSheet.properties.sheetId;

    const colA = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Agencies!A:A'
    });
    const ids = (colA.data.values || []);
    let rowIndex = -1;
    for (let i = 1; i < ids.length; i++) {
      if (parseInt(ids[i][0]) === id) { rowIndex = i; break; } // 0-indexed sheet row
    }

    if (rowIndex > 0) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        resource: {
          requests: [{
            deleteDimension: {
              range: {
                sheetId,
                dimension: 'ROWS',
                startIndex: rowIndex,
                endIndex: rowIndex + 1
              }
            }
          }]
        }
      });
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('[crm DELETE]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── ACTIVITY: read ──────────────────────────────────────────
app.get('/api/activity', async (req, res) => {
  try {
    const sheets = await getSheets();
    const result = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Activity!A2:B'
    });
    const rows = (result.data.values || []).reverse(); // newest first
    const activity = rows.map(r => ({ time: r[0] || '', msg: r[1] || '' }));
    res.json(activity);
  } catch (err) {
    console.error('[activity GET]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── ACTIVITY: append one entry ──────────────────────────────
app.post('/api/activity', async (req, res) => {
  try {
    const { msg, time } = req.body;
    const sheets = await getSheets();

    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: 'Activity!A:B',
      valueInputOption: 'RAW',
      resource: { values: [[time, msg]] }
    });

    // Trim to 100 entries (header + 100 data rows = 101 total)
    const all = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Activity!A:B'
    });
    const allRows = all.data.values || [];
    if (allRows.length > 101) {
      const keep = [allRows[0], ...allRows.slice(-100)];
      await sheets.spreadsheets.values.clear({ spreadsheetId: SHEET_ID, range: 'Activity!A:B' });
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: 'Activity!A1',
        valueInputOption: 'RAW',
        resource: { values: keep }
      });
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('[activity POST]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── ACTIVITY: batch restore (for import) ────────────────────
app.post('/api/activity/batch', async (req, res) => {
  try {
    const { entries } = req.body;
    const sheets = await getSheets();

    await sheets.spreadsheets.values.clear({ spreadsheetId: SHEET_ID, range: 'Activity!A2:B' });

    if (entries && entries.length > 0) {
      const rows = [...entries].reverse().map(e => [e.time || '', e.msg || '']);
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: 'Activity!A2',
        valueInputOption: 'RAW',
        resource: { values: rows }
      });
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('[activity batch]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── TEMPLATES: read ─────────────────────────────────────────
app.get('/api/templates', async (req, res) => {
  try {
    const sheets = await getSheets();
    const result = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Templates!A2:E'
    });
    const rows = result.data.values || [];
    const templates = rows.map(r => ({
      id:      parseInt(r[0]) || Date.now(),
      name:    r[1] || '',
      subject: r[2] || '',
      body:    r[3] || '',
      created: r[4] || ''
    }));
    res.json(templates);
  } catch (err) {
    console.error('[templates GET]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── TEMPLATES: full replace ─────────────────────────────────
app.post('/api/templates', async (req, res) => {
  try {
    const { templates } = req.body;
    const sheets = await getSheets();

    await sheets.spreadsheets.values.clear({ spreadsheetId: SHEET_ID, range: 'Templates!A2:E' });

    if (templates && templates.length > 0) {
      const rows = templates.map(t => [t.id, t.name, t.subject, t.body, t.created || '']);
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: 'Templates!A2',
        valueInputOption: 'RAW',
        resource: { values: rows }
      });
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('[templates POST]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── CUSTOM AGENCIES: read ───────────────────────────────────
app.get('/api/custom-agencies', async (req, res) => {
  try {
    const sheets = await getSheets();
    const result = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'CustomAgencies!A2:B'
    });
    const rows = result.data.values || [];
    const agencies = [];
    rows.forEach(r => {
      try { if (r[1]) agencies.push(JSON.parse(r[1])); } catch (e) {}
    });
    res.json(agencies);
  } catch (err) {
    console.error('[custom-agencies GET]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── CUSTOM AGENCIES: full replace ───────────────────────────
app.post('/api/custom-agencies', async (req, res) => {
  try {
    const { agencies } = req.body;
    const sheets = await getSheets();

    await sheets.spreadsheets.values.clear({ spreadsheetId: SHEET_ID, range: 'CustomAgencies!A2:B' });

    if (agencies && agencies.length > 0) {
      const rows = agencies.map(a => [a.id, JSON.stringify(a)]);
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: 'CustomAgencies!A2',
        valueInputOption: 'RAW',
        resource: { values: rows }
      });
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('[custom-agencies POST]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── MIGRATE: one-shot localStorage → Sheets ────────────────
app.post('/api/migrate', async (req, res) => {
  try {
    const { crm: crmData, activity: actData, templates: tplData, customAgencies: caData } = req.body;
    const sheets = await getSheets();
    const results = { crm: 0, activity: 0, templates: 0, customAgencies: 0, errors: [] };

    // Ensure tabs + headers exist
    const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
    const existingTitles = meta.data.sheets.map(s => s.properties.title);
    const tabs = [
      { name: 'Agencies',       headers: AGENCY_HEADERS },
      { name: 'Activity',       headers: ['Timestamp', 'Message'] },
      { name: 'Templates',      headers: ['ID', 'Name', 'Subject', 'Body', 'Created'] },
      { name: 'CustomAgencies', headers: ['ID', 'Data'] }
    ];
    const addRequests = tabs.filter(t => !existingTitles.includes(t.name))
      .map(t => ({ addSheet: { properties: { title: t.name } } }));
    if (addRequests.length > 0) {
      await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, resource: { requests: addRequests } });
    }
    for (const tab of tabs) {
      const check = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${tab.name}!A1:V1` });
      if (!check.data.values || check.data.values.length === 0) {
        await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: `${tab.name}!A1`, valueInputOption: 'RAW', resource: { values: [tab.headers] } });
      }
    }

    // CRM rows — client must send { crm, agencies } so we can write full rows
    const agencyMap = {};
    if (Array.isArray(req.body.agencies)) {
      req.body.agencies.forEach(a => { agencyMap[a.id] = a; });
    }
    if (crmData && typeof crmData === 'object') {
      const colA = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: 'Agencies!A:A' });
      const existingIds = new Set((colA.data.values || []).slice(1).map(r => parseInt(r[0])).filter(Boolean));
      for (const [id, d] of Object.entries(crmData)) {
        const numId = parseInt(id);
        if (existingIds.has(numId)) continue; // already in Sheets, skip
        const agency = agencyMap[numId] || {};
        const rowData = buildAgencyRow(numId, agency, d);
        try {
          await sheets.spreadsheets.values.append({ spreadsheetId: SHEET_ID, range: 'Agencies!A:V', valueInputOption: 'RAW', resource: { values: [rowData] } });
          results.crm++;
        } catch(e) { results.errors.push('crm:' + id); }
      }
    }

    // Activity
    if (Array.isArray(actData) && actData.length > 0) {
      try {
        await sheets.spreadsheets.values.clear({ spreadsheetId: SHEET_ID, range: 'Activity!A2:B' });
        const rows = [...actData].reverse().map(e => [e.time||'', e.msg||'']);
        await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: 'Activity!A2', valueInputOption: 'RAW', resource: { values: rows } });
        results.activity = actData.length;
      } catch(e) { results.errors.push('activity'); }
    }

    // Templates
    if (Array.isArray(tplData) && tplData.length > 0) {
      const existing = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: 'Templates!A2:E' });
      if (!existing.data.values || existing.data.values.length === 0) {
        try {
          const rows = tplData.map(t => [t.id, t.name, t.subject, t.body, t.created||'']);
          await sheets.spreadsheets.values.update({ spreadsheetId: SHEET_ID, range: 'Templates!A2', valueInputOption: 'RAW', resource: { values: rows } });
          results.templates = tplData.length;
        } catch(e) { results.errors.push('templates'); }
      }
    }

    // Custom agencies
    if (Array.isArray(caData) && caData.length > 0) {
      try {
        const existing = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: 'CustomAgencies!A2:B' });
        const existingIds = new Set((existing.data.values || []).map(r => parseInt(r[0])).filter(Boolean));
        const newOnes = caData.filter(a => !existingIds.has(a.id));
        if (newOnes.length > 0) {
          await sheets.spreadsheets.values.append({ spreadsheetId: SHEET_ID, range: 'CustomAgencies!A:B', valueInputOption: 'RAW', resource: { values: newOnes.map(a => [a.id, JSON.stringify(a)]) } });
        }
        results.customAgencies = newOnes.length;
      } catch(e) { results.errors.push('customAgencies'); }
    }

    console.log('[migrate]', results);
    res.json({ ok: true, results });
  } catch (err) {
    console.error('[migrate]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── BACKUP: download all sheet data as JSON ─────────────────
app.get('/api/backup', async (req, res) => {
  try {
    const sheets = await getSheets();

    const [agenciesRes, activityRes, templatesRes, customRes] = await Promise.all([
      sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: 'Agencies!A1:V' }),
      sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: 'Activity!A1:B' }),
      sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: 'Templates!A1:E' }),
      sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: 'CustomAgencies!A1:B' })
    ]);

    const backup = {
      exportedAt: new Date().toISOString(),
      agencies:       agenciesRes.data.values  || [],
      activity:       activityRes.data.values  || [],
      templates:      templatesRes.data.values || [],
      customAgencies: customRes.data.values    || []
    };

    res.setHeader('Content-Disposition', `attachment; filename="crm-backup-${Date.now()}.json"`);
    res.setHeader('Content-Type', 'application/json');
    res.json(backup);
  } catch (err) {
    console.error('[backup]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Helper: column index → spreadsheet letter (0=A, 26=AA, 27=AB …)
function colLetter(i) {
  let s = '';
  i++;
  while (i > 0) {
    i--;
    s = String.fromCharCode(65 + (i % 26)) + s;
    i = Math.floor(i / 26);
  }
  return s;
}

// ── SHEET DIAGNOSTICS: inspect current column layout ────────
// Returns the header row and first 5 data rows (columns A–V only).
app.get('/api/debug-sheet', async (req, res) => {
  try {
    const sheets = await getSheets();

    const raw = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Agencies!A1:V'
    });
    const allRows = raw.data.values || [];
    if (allRows.length === 0) return res.json({ headers: [], sample: [] });

    const headers = allRows[0];
    const dataRows = allRows.slice(1, 6);

    const sample = dataRows.map(row => {
      const cells = {};
      headers.forEach((h, i) => {
        cells[`${colLetter(i)}(${i}):${h}`] = row[i] || '';
      });
      return cells;
    });

    res.json({
      detectedLayout: headers[9] === 'Phone2' ? 'NEW 22-col' : 'OLD 20-col',
      headerCount: headers.length,
      headers,
      sample
    });
  } catch (err) {
    console.error('[debug-sheet]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── OVERFLOW INSPECTOR: read ALL columns including W-AM ──────
// Returns overflow column headers (row 1) and a sample of the
// raw cell values in columns W onwards for the first 10 data rows.
app.get('/api/inspect-overflow', async (req, res) => {
  try {
    const sheets = await getSheets();

    // Read the full sheet width — AM = col 39
    const raw = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Agencies!A1:AM'
    });
    const allRows = raw.data.values || [];
    if (allRows.length === 0) return res.json({ message: 'Sheet is empty' });

    const fullHeader = allRows[0] || [];
    const maxCol = Math.max(...allRows.map(r => r.length));

    // Columns beyond V (index 21) — index 22 = W
    const overflowStart = 22;
    const overflowHeaders = fullHeader.slice(overflowStart);
    const overflowSample = allRows.slice(1, 11).map((row, rowNum) => {
      const id = row[0] || `row${rowNum + 2}`;
      const name = row[1] || '';
      const cells = {};
      for (let i = overflowStart; i < Math.max(row.length, maxCol); i++) {
        if (row[i] != null && row[i] !== '') {
          cells[`${colLetter(i)}(${i})`] = row[i];
        }
      }
      return { id, name, overflowCells: cells };
    });

    // Count how many rows have any overflow data
    const rowsWithOverflow = allRows.slice(1).filter(row => row.length > overflowStart).length;

    res.json({
      totalColumns: maxCol,
      overflowStartCol: colLetter(overflowStart),
      overflowHeaders,
      rowsWithOverflow,
      sample: overflowSample
    });
  } catch (err) {
    console.error('[inspect-overflow]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── FIX OVERFLOW: recover AF-AM data into A-V, then delete W-AM ─
// The old migration wrote CRM data to cols AF-AM (indices 31-38)
// while cols W-AE (22-30) are empty.  This endpoint:
//   1. Reads all rows A1:AM
//   2. For each agency, merges AF-AM CRM data into A-V where A-V is empty
//   3. Rewrites corrected 22-col rows to A-V
//   4. Deletes columns W-AM so the sheet is clean
// Accepts { agencies } in the body so agency static data is available.
app.post('/api/fix-overflow', async (req, res) => {
  try {
    const { agencies } = req.body;
    if (!Array.isArray(agencies) || agencies.length === 0) {
      return res.status(400).json({ error: 'agencies array required in body' });
    }

    const sheets = await getSheets();

    // ── 1. Read the full sheet including overflow ──────────────────
    const raw = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Agencies!A1:AM'
    });
    const allRows = raw.data.values || [];
    if (allRows.length < 2) return res.json({ ok: true, message: 'No data rows found' });

    // Known overflow column positions (AF-AM = indices 31-38)
    const OVF = {
      status:   31, // AF
      pic:      32, // AG
      picPhone: 33, // AH
      picEmail: 34, // AI
      notes:    35, // AJ
      date:     36, // AK
      picTitle: 37, // AL
      log:      38  // AM
    };

    // New 22-col CRM field positions (A-V = indices 0-21)
    const NEW = {
      status:   14,
      pic:      15,
      picPhone: 16,
      picEmail: 17,
      picTitle: 18,
      notes:    19,
      date:     20,
      log:      21
    };

    const agencyMap = {};
    agencies.forEach(a => { agencyMap[a.id] = a; });

    const col = (row, i) => (row[i] != null ? String(row[i]) : '');
    const isEmpty = (v) => !v || v.trim() === '' || v === 'Not Contacted' || v === '[]';

    const report = { merged: 0, unchanged: 0, skipped: 0, errors: [] };
    const correctedRows = [];

    const dataRows = allRows.slice(1); // skip header row
    for (const row of dataRows) {
      const id = parseInt(row[0]);
      if (!id) { report.skipped++; continue; }

      // Current A-V CRM values
      const curStatus   = col(row, NEW.status);
      const curPic      = col(row, NEW.pic);
      const curPicPhone = col(row, NEW.picPhone);
      const curPicEmail = col(row, NEW.picEmail);
      const curPicTitle = col(row, NEW.picTitle);
      const curNotes    = col(row, NEW.notes);
      const curDate     = col(row, NEW.date);
      const curLogRaw   = col(row, NEW.log);

      // Overflow AF-AM CRM values
      const ovfStatus   = col(row, OVF.status);
      const ovfPic      = col(row, OVF.pic);
      const ovfPicPhone = col(row, OVF.picPhone);
      const ovfPicEmail = col(row, OVF.picEmail);
      const ovfPicTitle = col(row, OVF.picTitle);
      const ovfNotes    = col(row, OVF.notes);
      const ovfDate     = col(row, OVF.date);
      const ovfLogRaw   = col(row, OVF.log);

      // Merge: prefer existing A-V value; use overflow only if A-V is empty/default
      const mergedStatus   = isEmpty(curStatus)   ? (ovfStatus   || 'Not Contacted') : curStatus;
      const mergedPic      = isEmpty(curPic)      ? ovfPic      : curPic;
      const mergedPicPhone = isEmpty(curPicPhone) ? ovfPicPhone : curPicPhone;
      const mergedPicEmail = isEmpty(curPicEmail) ? ovfPicEmail : curPicEmail;
      const mergedPicTitle = isEmpty(curPicTitle) ? ovfPicTitle : curPicTitle;
      const mergedNotes    = isEmpty(curNotes)    ? ovfNotes    : curNotes;
      const mergedDate     = isEmpty(curDate)     ? ovfDate     : curDate;

      // Merge logs: combine unique entries from both sources
      let curLog = [];
      let ovfLog = [];
      try { if (curLogRaw && curLogRaw !== '[]') curLog = JSON.parse(curLogRaw); } catch(e) {}
      try { if (ovfLogRaw && ovfLogRaw !== '[]') ovfLog = JSON.parse(ovfLogRaw); } catch(e) {}
      // Deduplicate by msg+time; prefer existing log order on top, overflow below
      const logSet = new Set(curLog.map(e => e.msg + '|' + e.time));
      const mergedLog = [...curLog, ...ovfLog.filter(e => !logSet.has(e.msg + '|' + e.time))].slice(0, 50);

      const anyMerged = (
        (isEmpty(curStatus)   && ovfStatus)   ||
        (isEmpty(curPic)      && ovfPic)      ||
        (isEmpty(curPicPhone) && ovfPicPhone) ||
        (isEmpty(curPicEmail) && ovfPicEmail) ||
        (isEmpty(curPicTitle) && ovfPicTitle) ||
        (isEmpty(curNotes)    && ovfNotes)    ||
        (isEmpty(curDate)     && ovfDate)     ||
        (curLog.length === 0  && ovfLog.length > 0)
      );

      if (anyMerged) report.merged++;
      else report.unchanged++;

      const agency = agencyMap[id] || {};
      correctedRows.push({
        id,
        row: buildAgencyRow(id, agency, {
          status:   mergedStatus,
          pic:      mergedPic,
          picPhone: mergedPicPhone,
          picEmail: mergedPicEmail,
          picTitle: mergedPicTitle,
          notes:    mergedNotes,
          date:     mergedDate,
          log:      mergedLog
        })
      });
    }

    // Sort by ID before writing
    correctedRows.sort((a, b) => a.id - b.id);

    // ── 2. Collect Activity entries from ALL agency logs (A-V + overflow) ──
    // The frontend Activity tab is limited to 20 in-memory entries.
    // The overflow logs contain batch email records that never made it
    // into the Activity sheet.  We reconstruct them here.
    const activitySet = new Map(); // key = time+msg, value = {time, msg}

    for (const row of dataRows) {
      const id = parseInt(row[0]);
      if (!id) continue;

      // Current A-V log
      const curLogRaw = col(row, NEW.log);
      // Overflow log
      const ovfLogRaw = col(row, OVF.log);

      for (const raw of [curLogRaw, ovfLogRaw]) {
        if (!raw || raw === '[]') continue;
        try {
          const entries = JSON.parse(raw);
          for (const e of entries) {
            if (e.msg && e.time) {
              const key = e.time + '|' + e.msg;
              if (!activitySet.has(key)) activitySet.set(key, { time: e.time, msg: e.msg });
            }
          }
        } catch(e) {}
      }
    }

    // Read current Activity tab and add those too
    const actRes = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Activity!A2:B'
    });
    for (const row of (actRes.data.values || [])) {
      const time = row[0] || '';
      const msg  = row[1] || '';
      if (time && msg) {
        const key = time + '|' + msg;
        if (!activitySet.has(key)) activitySet.set(key, { time, msg });
      }
    }

    // Sort by time descending (newest first), keep up to 100
    const allActivity = [...activitySet.values()].sort((a, b) => {
      // Parse "DD/MM/YYYY, H:MM:SS am/pm" timestamps
      const parse = s => {
        try {
          const [datePart, timePart] = s.split(', ');
          const [d, m, y] = datePart.split('/').map(Number);
          return new Date(`${y}-${String(m).padStart(2,'0')}-${String(d).padStart(2,'0')} ${timePart}`);
        } catch(e) { return new Date(0); }
      };
      return parse(b.time) - parse(a.time); // newest first
    }).slice(0, 100);

    const prevActivityCount = (actRes.data.values || []).length;
    report.activityRecovered = allActivity.length - prevActivityCount;

    // ── 3. Clear and rewrite A1:V with corrected data ──────────────
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SHEET_ID,
      range: 'Agencies!A1:V'
    });
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: 'Agencies!A1',
      valueInputOption: 'RAW',
      resource: { values: [AGENCY_HEADERS, ...correctedRows.map(r => r.row)] }
    });

    // ── 4. Rewrite Activity tab with merged entries ─────────────────
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SHEET_ID,
      range: 'Activity!A2:B'
    });
    if (allActivity.length > 0) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: 'Activity!A2',
        valueInputOption: 'RAW',
        resource: { values: allActivity.map(e => [e.time, e.msg]) }
      });
    }

    // ── 5. Delete columns W-AM (indices 22-38, 0-based; endIndex exclusive = 39) ──
    const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
    const agenciesSheet = meta.data.sheets.find(s => s.properties.title === 'Agencies');
    if (agenciesSheet) {
      const sheetId = agenciesSheet.properties.sheetId;
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        resource: {
          requests: [{
            deleteDimension: {
              range: {
                sheetId,
                dimension: 'COLUMNS',
                startIndex: 22,  // W (0-based)
                endIndex:   39   // exclusive → deletes W through AM
              }
            }
          }]
        }
      });
    }

    console.log(`[fix-overflow] merged: ${report.merged}, unchanged: ${report.unchanged}, activity: ${allActivity.length} entries, columns W-AM deleted`);
    res.json({ ok: true, report });
  } catch (err) {
    console.error('[fix-overflow]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── RESET-SHEET: remap + rewrite Agencies tab cleanly ────────
// POST body: { agencies: [ agency objects from BUILTIN_AGENCIES ] }
// Reads the HEADER ROW first to detect old (20-col) vs new (22-col)
// layout — never relies on row.length, which is unreliable because
// the Sheets API strips trailing empty cells from every row.
app.post('/api/reset-sheet', async (req, res) => {
  try {
    const { agencies } = req.body;
    if (!Array.isArray(agencies) || agencies.length === 0) {
      return res.status(400).json({ error: 'agencies array required in body' });
    }

    const sheets = await getSheets();

    // ── Step 1: read header to determine the sheet's current layout ──
    const headerRes = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Agencies!A1:V1'
    });
    const headers = (headerRes.data.values || [[]])[0] || [];

    // Detect layout by column 9's header name:
    //   Old 20-col: col9 = 'Email'   (no Phone2/Fax)
    //   New 22-col: col9 = 'Phone2'
    // IMPORTANT: Do NOT use row.length — Google Sheets strips trailing
    // empty cells, so a new-layout row with empty Date/Log columns will
    // look like it only has 20 cells and fool a length-based check.
    const isNewLayout = headers[9] === 'Phone2';

    // Column positions for CRM fields
    let statusCol, picCol, picPhoneCol, picEmailCol, picTitleCol, notesCol, dateCol, logCol;
    if (isNewLayout) {
      // New 22-col (A–V):
      // [0]ID [1]Name [2]Short [3]Type [4]District [5]Division
      // [6]Website [7]Address [8]Phone [9]Phone2 [10]Fax [11]Email
      // [12]Admin Email [13]Procurement Email
      // [14]Status [15]PIC [16]PIC Phone [17]PIC Email [18]PIC Title
      // [19]Notes [20]Last Action Date [21]Log
      statusCol   = 14;
      picCol      = 15;
      picPhoneCol = 16;
      picEmailCol = 17;
      picTitleCol = 18;
      notesCol    = 19;
      dateCol     = 20;
      logCol      = 21;
    } else {
      // Old 20-col (A–T):
      // [0]Agency ID [1]Name [2]Short Name [3]Type [4]District [5]Division
      // [6]Website [7]Address [8]Phone [9]Email
      // [10]Admin Email [11]Procurement Email
      // [12]Status [13]PIC [14]PIC Phone [15]PIC Email
      // [16]Notes [17]Last Action Date [18]PIC Title [19]Activity Log
      statusCol   = 12;
      picCol      = 13;
      picPhoneCol = 14;
      picEmailCol = 15;
      notesCol    = 16;
      dateCol     = 17;
      picTitleCol = 18;
      logCol      = 19;
    }

    console.log(`[reset-sheet] detected layout: ${isNewLayout ? 'NEW 22-col' : 'OLD 20-col'} (header[9]="${headers[9]}")`);
    console.log(`[reset-sheet] CRM cols → status:${statusCol} pic:${picCol} picPhone:${picPhoneCol} picEmail:${picEmailCol} picTitle:${picTitleCol} notes:${notesCol} date:${dateCol} log:${logCol}`);

    // ── Step 2: read all data rows ───────────────────────────────────
    const dataRes = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Agencies!A2:V'
    });
    const existingRows = dataRes.data.values || [];

    // ── Step 3: build id → CRM map using header-derived positions ────
    const crmByID = {};
    for (const row of existingRows) {
      const id = parseInt(row[0]);
      if (!id) continue;

      // Safe accessor — returns '' for any column that is missing/empty
      const col = (i) => (row[i] != null ? String(row[i]) : '');

      let log = [];
      try { if (col(logCol)) log = JSON.parse(col(logCol)); } catch (e) {}

      crmByID[id] = {
        status:   col(statusCol)   || 'Not Contacted',
        pic:      col(picCol),
        picPhone: col(picPhoneCol),
        picEmail: col(picEmailCol),
        picTitle: col(picTitleCol),
        notes:    col(notesCol),
        date:     col(dateCol),
        log
      };
    }

    console.log(`[reset-sheet] read ${existingRows.length} existing rows, found CRM data for ${Object.keys(crmByID).length} agencies`);

    // ── Step 4: build clean 22-col rows for every agency in request ──
    const newRows = agencies.map(agency => {
      const crm = crmByID[agency.id] || {};
      return buildAgencyRow(agency.id, agency, crm);
    });
    newRows.sort((a, b) => a[0] - b[0]);

    // ── Step 5: clear and rewrite ────────────────────────────────────
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SHEET_ID,
      range: 'Agencies!A1:V'  // clear header + data
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: 'Agencies!A1',
      valueInputOption: 'RAW',
      resource: { values: [AGENCY_HEADERS, ...newRows] }
    });

    console.log(`[reset-sheet] rewrote header + ${newRows.length} rows`);
    res.json({
      ok: true,
      detectedLayout: isNewLayout ? 'NEW 22-col' : 'OLD 20-col',
      crmFound: Object.keys(crmByID).length,
      rowsWritten: newRows.length
    });
  } catch (err) {
    console.error('[reset-sheet]', err.message);
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`TMS CRM running at http://localhost:${PORT}`);
});
