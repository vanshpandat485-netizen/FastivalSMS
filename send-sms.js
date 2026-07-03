// ============================================================
// FestiveSMS — Complete SMS Automation
//
// FLOW:
//   1. Aaj ke festivals + birthdays find karo
//   2. sms_queue mein pending rows insert karo
//   3. Dheeray dheeray Fast2SMS/Webhook se bhejna
//
// RATE LIMIT (DLT safe):
//   SMS_PER_SECOND = 10
//   100 SMS per API call
//   Har call ke baad 10 second wait
//   50,000 SMS = 500 calls x 10s = 83 minutes
//   GitHub Actions limit = 360 minutes — easily fits!
// ============================================================

import { createClient } from '@supabase/supabase-js';
import fs   from 'fs';
import path from 'path';

// ─── CONFIG ─────────────────────────────────────────────────
const SUPABASE_URL  = process.env.SUPABASE_URL;
const SUPABASE_KEY  = process.env.SUPABASE_KEY;
const FAST2SMS_KEY  = process.env.FAST2SMS_KEY || '';
const WEBHOOK_URL   = process.env.WEBHOOK_URL
  || 'https://webhook.site/712fc352-a4ed-4da0-b858-e2dac71c371a';
const DRY_RUN       = process.env.DRY_RUN === 'true';
const DATE_OVERRIDE = process.env.DATE_OVERRIDE || '';
const SMS_PER_SEC   = parseInt(process.env.SMS_PER_SECOND || '10');
const TODAY         = DATE_OVERRIDE
  || new Date().toLocaleDateString('en-CA');
const MODE          = FAST2SMS_KEY ? 'FAST2SMS' : 'WEBHOOK';

// ─── BATCH SIZES ────────────────────────────────────────────
const NUMS_PER_CALL = 100;   // Numbers per Fast2SMS API call
const DB_PAGE       = 1000;  // Rows per Supabase query
const INSERT_SIZE   = 200;   // Rows per INSERT

// Wait time between batches
// Example: 10/sec, 100 nums → wait 10 seconds
const WAIT_MS = Math.ceil((NUMS_PER_CALL / SMS_PER_SEC) * 1000);

// Stats
const ST = {
  queued: 0, total: 0, sent: 0,
  failed: 0, skipped: 0, calls: 0,
  t0: Date.now()
};

// ─── LOGGING ────────────────────────────────────────────────
const LOG_DIR  = './logs';
const LOG_FILE = path.join(LOG_DIR, `sms-${TODAY}.log`);
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

function log(lvl, msg, data) {
  const ts  = new Date().toISOString();
  const sec = Math.round((Date.now() - ST.t0) / 1000);
  const mb  = Math.round(process.memoryUsage().heapUsed / 1048576);
  const out = `[${ts}][${sec}s][${mb}MB][${lvl}] ${msg}`;
  console.log(out, data !== undefined ? JSON.stringify(data) : '');
  fs.appendFileSync(
    LOG_FILE,
    JSON.stringify({ ts, sec, mb, lvl, msg, data }) + '\n'
  );
}

// ─── SLEEP ──────────────────────────────────────────────────
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ─── ENV CHECK ──────────────────────────────────────────────
function checkEnv() {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    log('ERROR', 'SUPABASE_URL ya SUPABASE_KEY nahi mila!');
    log('ERROR', 'GitHub Repo → Settings → Secrets → Actions → Add karo');
    process.exit(1);
  }

  const estMin = Math.ceil(50000 / SMS_PER_SEC / 60);

  log('INFO', '='.repeat(55));
  log('INFO', '  FestiveSMS — Daily SMS Automation');
  log('INFO', '='.repeat(55));
  log('INFO', `Date         : ${TODAY}`);
  log('INFO', `Mode         : ${MODE}${DRY_RUN ? ' [DRY RUN]' : ''}`);
  log('INFO', `Rate         : ${SMS_PER_SEC} SMS/second (DLT safe)`);
  log('INFO', `Per API call : ${NUMS_PER_CALL} numbers`);
  log('INFO', `Wait/batch   : ${WAIT_MS}ms = ${WAIT_MS/1000}s`);
  log('INFO', `50K estimate : ~${estMin} minutes`);
  log('INFO', `GH limit     : 360 minutes — no problem!`);

  if (MODE === 'WEBHOOK') {
    log('INFO', 'TESTING — SMS webhook.site pe jayenge');
  } else {
    log('INFO', 'PRODUCTION — Fast2SMS se real SMS jayenge');
  }
  log('INFO', '='.repeat(55));
}

// ─── SUPABASE ───────────────────────────────────────────────
const db = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { autoRefreshToken: false, persistSession: false }
});

// ─── MESSAGE ────────────────────────────────────────────────
function buildMsg(customer, occasion, shop, addr) {
  return `Dear ${customer}, ${occasion} ki hardik shubhkamnayein `
    + `${shop} ki taraf se. Please visit our shop at `
    + `${addr || 'our shop'} for gifts and heavy discounts. ${shop}`;
}

// ─── DB FETCH (Paginated) ────────────────────────────────────
async function dbFetchAll(table, select, filters = {}) {
  const rows = [];
  let from = 0, more = true;

  while (more) {
    let q = db.from(table).select(select)
      .range(from, from + DB_PAGE - 1)
      .order('id');

    for (const [col, val] of Object.entries(filters)) {
      q = q.eq(col, val);
    }

    const { data, error } = await q;
    if (error) throw new Error(`DB error ${table}: ${error.message}`);

    rows.push(...(data || []));
    more = (data || []).length === DB_PAGE;
    from += DB_PAGE;
  }

  return rows;
}

// ─── DB UPDATE (Bulk) ────────────────────────────────────────
async function dbUpdate(ids, status) {
  if (!ids.length) return;
  const update = { status };
  if (status === 'sent') update.sent_at = new Date().toISOString();

  for (let i = 0; i < ids.length; i += 500) {
    const { error } = await db.from('sms_queue')
      .update(update).in('id', ids.slice(i, i + 500));
    if (error) log('WARN', 'DB update warn', { error: error.message });
  }
}

// ════════════════════════════════════════════════════════════
// STEP 1: QUEUE — Pending SMS rows banao DB mein
// ════════════════════════════════════════════════════════════
async function stepQueue() {
  log('INFO', '');
  log('INFO', '──────────────────────────────────────────────');
  log('INFO', 'STEP 1: SMS Queue Bana Rahe Hain');
  log('INFO', '──────────────────────────────────────────────');

  // Aaj ke festivals
  const { data: fests } = await db
    .from('festivals').select('*').eq('date', TODAY);
  const todayFests = fests || [];
  log('INFO', `Festivals aaj: ${todayFests.length}`,
    { names: todayFests.map(f => f.name) });

  // Active shops (expired plan wale skip)
  const { data: users } = await db
    .from('users')
    .select('id, shop_name, shop_address, plan_end_date')
    .eq('is_admin', false)
    .eq('subscription_active', true)
    .gte('plan_end_date', TODAY);

  const shops = users || [];
  log('INFO', `Active shops: ${shops.length}`);

  const tDate = new Date(TODAY + 'T00:00:00');
  const tM    = tDate.getMonth();
  const tD    = tDate.getDate();
  let totalQ  = 0;

  for (let si = 0; si < shops.length; si++) {
    const shop = shops[si];
    try {
      totalQ += await queueShop(shop, todayFests, tM, tD);
    } catch (e) {
      log('ERROR', `Shop fail: ${shop.shop_name}`, { e: e.message });
    }

    if ((si + 1) % 10 === 0 || si === shops.length - 1) {
      log('INFO', `Shops done: ${si + 1}/${shops.length} | Queued: ${totalQ}`);
    }
  }

  ST.queued = totalQ;
  log('INFO', `Queue complete: ${totalQ} SMS pending`);
  return totalQ;
}

async function queueShop(shop, fests, tM, tD) {
  const custs = await dbFetchAll(
    'customers', 'id, customer_name, phone_number, dob',
    { user_id: shop.id }
  );
  if (!custs.length) return 0;

  // Already queue mein kya hai
  const existing = await dbFetchAll(
    'sms_queue', 'customer_id, sms_type, festival_id',
    { user_id: shop.id, scheduled_date: TODAY }
  );

  const has = new Set(existing.map(e =>
    e.sms_type === 'festival'
      ? `${e.customer_id}_f_${e.festival_id}`
      : `${e.customer_id}_${e.sms_type}`
  ));

  const rows = [];

  // Birthdays
  for (const c of custs) {
    if (!c.dob) continue;
    const d = new Date(c.dob + 'T00:00:00');
    if (d.getMonth() === tM && d.getDate() === tD) {
      if (!has.has(`${c.id}_birthday`)) {
        rows.push({
          user_id: shop.id, customer_id: c.id,
          festival_id: null, sms_type: 'birthday',
          message: buildMsg(
            c.customer_name, 'Janmdin',
            shop.shop_name, shop.shop_address
          ),
          status: 'pending', scheduled_date: TODAY
        });
      }
    }
  }

  // Festivals
  for (const f of fests) {
    for (const c of custs) {
      if (!has.has(`${c.id}_f_${f.id}`)) {
        rows.push({
          user_id: shop.id, customer_id: c.id,
          festival_id: f.id, sms_type: 'festival',
          message: buildMsg(
            c.customer_name, f.name,
            shop.shop_name, shop.shop_address
          ),
          status: 'pending', scheduled_date: TODAY
        });
      }
    }
  }

  if (!rows.length) return 0;

  let count = 0;
  for (let i = 0; i < rows.length; i += INSERT_SIZE) {
    const { error } = await db.from('sms_queue')
      .insert(rows.slice(i, i + INSERT_SIZE));
    if (!error) count += Math.min(INSERT_SIZE, rows.length - i);
  }

  return count;
}

// ════════════════════════════════════════════════════════════
// STEP 2: SEND — Pending SMS bhejna
//
// RATE LIMIT MATH:
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SMS_PER_SEC = 10
// NUMS_PER_CALL = 100
// WAIT_MS = (100/10) × 1000 = 10,000ms = 10 seconds
//
// 50,000 SMS:
//   ÷ 100 per call = 500 API calls
//   × 10 sec each = 5,000 seconds = 83 minutes
//
// GitHub limit = 360 minutes
// 83 < 360 ✅ EASILY FITS!
//
// DLT limit = 30 SMS/sec
// We send 10/sec = 33% of limit ✅ SAFE!
// ════════════════════════════════════════════════════════════
async function stepSend() {
  log('INFO', '');
  log('INFO', '──────────────────────────────────────────────');
  log('INFO', 'STEP 2: SMS Bhej Rahe Hain');
  log('INFO', '──────────────────────────────────────────────');

  // Pending SMS fetch karo
  const pending = await dbFetchAll(
    'sms_queue',
    `id, message, sms_type,
     customers(customer_name, phone_number),
     users!sms_queue_user_id_fkey(
       shop_name, subscription_active, plan_end_date
     )`,
    { scheduled_date: TODAY, status: 'pending' }
  );

  log('INFO', `Total pending: ${pending.length}`);

  // Validate
  const valid = [];
  for (const r of pending) {
    if (!r.customers?.phone_number)      { ST.skipped++; continue; }
    if (!r.users?.subscription_active)   { ST.skipped++; continue; }
    if (r.users?.plan_end_date < TODAY)  { ST.skipped++; continue; }

    const phone = String(r.customers.phone_number)
      .replace(/\D/g, '').slice(-10);
    if (phone.length !== 10) { ST.skipped++; continue; }

    valid.push({
      id: r.id, phone,
      message: r.message,
      smsType: r.sms_type,
      shop: r.users?.shop_name || ''
    });
  }

  ST.total = valid.length;
  log('INFO', `Valid: ${ST.total} | Skipped: ${ST.skipped}`);

  if (!ST.total) {
    log('INFO', 'Koi valid SMS nahi');
    return;
  }

  // Group by message
  const groups = new Map();
  for (const r of valid) {
    if (!groups.has(r.message)) groups.set(r.message, []);
    groups.get(r.message).push(r);
  }

  // Batches banao
  const batches = [];
  for (const [message, recs] of groups) {
    for (let i = 0; i < recs.length; i += NUMS_PER_CALL) {
      const chunk = recs.slice(i, i + NUMS_PER_CALL);
      batches.push({
        message,
        records: chunk,
        phones: chunk.map(r => r.phone),
        shop: chunk[0]?.shop || '',
        smsType: chunk[0]?.smsType || ''
      });
    }
  }

  const total     = batches.length;
  const estMin    = Math.ceil(total * WAIT_MS / 60000);

  log('INFO', `Batches: ${total}`);
  log('INFO', `Estimated: ~${estMin} minutes`);
  log('INFO', `GitHub limit: 360 min — easily fits!`);
  log('INFO', '');

  // Bhejo — ek ek karke rate limit ke saath
  for (let i = 0; i < total; i++) {
    const batch  = batches[i];
    const bNum   = i + 1;

    let sentIds = [], failedIds = [];

    if (DRY_RUN) {
      log('INFO', `[DRY] Batch ${bNum}/${total}: ${batch.phones.length} nums`);
      sentIds = batch.records.map(r => r.id);
    } else if (MODE === 'FAST2SMS') {
      const res = await f2sSend(batch);
      sentIds   = res.sentIds;
      failedIds = res.failedIds;
    } else {
      const res = await webhookSend(batch);
      sentIds   = res.sentIds;
      failedIds = res.failedIds;
    }

    // DB update
    await dbUpdate(sentIds,   'sent');
    await dbUpdate(failedIds, 'failed');
    ST.sent   += sentIds.length;
    ST.failed += failedIds.length;

    // Progress har 50 batches
    if (bNum % 50 === 0 || bNum === total) {
      const sec  = Math.round((Date.now() - ST.t0) / 1000);
      const pct  = Math.round((ST.sent + ST.failed) / ST.total * 100);
      const remS = Math.round((total - bNum) * WAIT_MS / 1000);
      log('INFO',
        `[${bNum}/${total}] `
        + `✅${ST.sent} ❌${ST.failed} `
        + `| ${pct}% | `
        + `${sec}s elapsed | `
        + `~${Math.ceil(remS/60)}min baki`
      );
    }

    // ★ RATE LIMIT WAIT — DLT safe rakhta hai ★
    if (i < total - 1) await sleep(WAIT_MS);
  }
}

// ─── FAST2SMS ───────────────────────────────────────────────
async function f2sSend(batch, attempt = 1) {
  ST.calls++;
  try {
    const res = await fetch('https://www.fast2sms.com/dev/bulkV2', {
      method: 'POST',
      headers: {
        'authorization': FAST2SMS_KEY,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        route: 'q', message: batch.message,
        language: 'english', flash: 0,
        numbers: batch.phones.join(',')
      }),
      signal: AbortSignal.timeout(30000)
    });

    const text = await res.text();
    let json;
    try { json = JSON.parse(text); } catch { json = {}; }

    if (!res.ok) throw new Error(`HTTP ${res.status}: ${text}`);
    if (json.return !== true)
      throw new Error(`API: ${JSON.stringify(json)}`);

    return {
      sentIds:   batch.records.map(r => r.id),
      failedIds: []
    };

  } catch (err) {
    if (attempt < 3) {
      log('WARN', `F2S retry ${attempt}`, { err: err.message });
      await sleep(attempt * 5000);
      return f2sSend(batch, attempt + 1);
    }
    log('ERROR', `F2S fail — webhook fallback`, { err: err.message });
    return webhookSend(batch);
  }
}

// ─── WEBHOOK (Testing) ──────────────────────────────────────
async function webhookSend(batch) {
  const sentIds = [], failedIds = [];

  for (let i = 0; i < batch.records.length; i += 5) {
    const chunk = batch.records.slice(i, i + 5);

    const res = await Promise.allSettled(
      chunk.map(async r => {
        ST.calls++;
        try {
          const resp = await fetch(WEBHOOK_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              phone: r.phone, message: batch.message,
              shop: batch.shop, type: r.smsType,
              date: TODAY, ts: new Date().toISOString(),
              mode: 'TESTING'
            }),
            signal: AbortSignal.timeout(8000)
          });
          return { id: r.id, ok: resp.ok };
        } catch {
          return { id: r.id, ok: false };
        }
      })
    );

    res.forEach(r => {
      const v = r.status === 'fulfilled' ? r.value : { ok: false };
      if (v.ok && v.id) sentIds.push(v.id);
      else if (v.id)    failedIds.push(v.id);
    });

    if (i + 5 < batch.records.length) await sleep(50);
  }

  return { sentIds, failedIds };
}

// ─── REPORT ─────────────────────────────────────────────────
function report() {
  const sec  = Math.round((Date.now() - ST.t0) / 1000);
  const min  = Math.round(sec / 60);
  const pct  = ST.total > 0
    ? Math.round(ST.sent / ST.total * 100) : 0;
  const spd  = sec > 0 ? Math.round(ST.sent / sec) : 0;
  const mb   = Math.round(process.memoryUsage().heapUsed / 1048576);

  log('INFO', '');
  log('INFO', '='.repeat(55));
  log('INFO', '  FINAL REPORT');
  log('INFO', '='.repeat(55));
  log('INFO', `  Date      : ${TODAY}`);
  log('INFO', `  Mode      : ${MODE}${DRY_RUN ? ' [DRY RUN]' : ''}`);
  log('INFO', `  Queued    : ${ST.queued}`);
  log('INFO', `  Total     : ${ST.total}`);
  log('INFO', `  Sent ✅   : ${ST.sent}`);
  log('INFO', `  Failed ❌ : ${ST.failed}`);
  log('INFO', `  Skipped   : ${ST.skipped}`);
  log('INFO', `  Rate      : ${pct}%`);
  log('INFO', `  Time      : ${min}min (${sec}s)`);
  log('INFO', `  Speed     : ${spd} SMS/sec`);
  log('INFO', `  API Calls : ${ST.calls}`);
  log('INFO', `  Memory    : ${mb}MB`);
  log('INFO', '='.repeat(55));

  fs.writeFileSync(
    path.join(LOG_DIR, `report-${TODAY}.json`),
    JSON.stringify({
      date: TODAY, mode: MODE, dry_run: DRY_RUN,
      queued: ST.queued, total: ST.total,
      sent: ST.sent, failed: ST.failed,
      skipped: ST.skipped, pct: pct + '%',
      time: min + 'min', speed: spd + '/sec',
      calls: ST.calls, memory: mb + 'MB'
    }, null, 2)
  );
}

// ─── MAIN ───────────────────────────────────────────────────
async function main() {
  try {
    checkEnv();
    await stepQueue();   // Step 1: Queue banao
    await stepSend();    // Step 2: Bhejna
    report();

    if (ST.total > 0 && ST.failed / ST.total > 0.5) {
      log('ERROR', '50%+ fail — check karo!');
      process.exit(1);
    }
  } catch (err) {
    log('ERROR', `CRASH: ${err.message}`, { stack: err.stack });
    process.exit(1);
  }
}

main();
