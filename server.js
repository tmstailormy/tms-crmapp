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

// ── RESET-SHEET: remap + rewrite Agencies tab cleanly ────────
// POST body: { agencies: [ agency objects from BUILTIN_AGENCIES ] }
// Reads existing CRM data (status/notes/PIC/log) from the sheet,
// merges with fresh agency static data, and rewrites all rows in
// the correct 22-column order.  No CRM data is lost.
app.post('/api/reset-sheet', async (req, res) => {
  try {
    const { agencies } = req.body;
    if (!Array.isArray(agencies) || agencies.length === 0) {
      return res.status(400).json({ error: 'agencies array required in body' });
    }

    const sheets = await getSheets();

    // Read everything currently in the sheet (could be old 20-col or new 22-col layout)
    const existing = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Agencies!A2:V'
    });
    const existingRows = existing.data.values || [];

    // Build a map of id → existing CRM fields, being lenient about column count.
    // We detect old (20-col) vs new (22-col) layout by presence of Phone2/Fax columns.
    // Strategy: try to read from new positions first; if row is only 20 cols, fall back
    // to old positions.
    const crmByID = {};
    for (const row of existingRows) {
      const id = parseInt(row[0]);
      if (!id) continue;

      const isOldLayout = row.length <= 20;
      let status, pic, picPhone, picEmail, picTitle, notes, date, rawLog;

      if (isOldLayout) {
        // Old 20-col: [12]=Status [13]=PIC [14]=PICPhone [15]=PICEmail
        //             [16]=Notes [17]=Date [18]=PICTitle  [19]=Log
        status   = row[12] || 'Not Contacted';
        pic      = row[13] || '';
        picPhone = row[14] || '';
        picEmail = row[15] || '';
        notes    = row[16] || '';
        date     = row[17] || '';
        picTitle = row[18] || '';
        rawLog   = row[19] || '';
      } else {
        // New 22-col: [14]=Status [15]=PIC [16]=PICPhone [17]=PICEmail
        //             [18]=PICTitle [19]=Notes [20]=Date [21]=Log
        status   = row[14] || 'Not Contacted';
        pic      = row[15] || '';
        picPhone = row[16] || '';
        picEmail = row[17] || '';
        picTitle = row[18] || '';
        notes    = row[19] || '';
        date     = row[20] || '';
        rawLog   = row[21] || '';
      }

      let log = [];
      try { if (rawLog) log = JSON.parse(rawLog); } catch (e) {}

      crmByID[id] = { status, pic, picPhone, picEmail, picTitle, notes, date, log };
    }

    // Build clean rows for every agency provided
    const newRows = agencies.map(agency => {
      const crm = crmByID[agency.id] || {};
      return buildAgencyRow(agency.id, agency, crm);
    });

    // Sort by agency ID
    newRows.sort((a, b) => a[0] - b[0]);

    // Clear data rows and rewrite
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SHEET_ID,
      range: 'Agencies!A2:V'
    });

    // Ensure header row has correct columns
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: 'Agencies!A1',
      valueInputOption: 'RAW',
      resource: { values: [AGENCY_HEADERS] }
    });

    if (newRows.length > 0) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: 'Agencies!A2',
        valueInputOption: 'RAW',
        resource: { values: newRows }
      });
    }

    console.log(`[reset-sheet] rewrote ${newRows.length} rows`);
    res.json({ ok: true, rowsWritten: newRows.length });
  } catch (err) {
    console.error('[reset-sheet]', err.message);
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`TMS CRM running at http://localhost:${PORT}`);
});
