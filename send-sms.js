import { createClient } from '@supabase/supabase-js';
import fs from 'fs';
import path from 'path';

var SUPABASE_URL  = process.env.SUPABASE_URL;
var SUPABASE_KEY  = process.env.SUPABASE_KEY;
var FAST2SMS_KEY  = process.env.FAST2SMS_KEY || '';
var WEBHOOK_URL   = process.env.WEBHOOK_URL || 'https://webhook.site/712fc352-a4ed-4da0-b858-e2dac71c371a';
var DRY_RUN       = process.env.DRY_RUN === 'true';
var DATE_OVERRIDE = process.env.DATE_OVERRIDE || '';
var SMS_PER_SEC   = parseInt(process.env.SMS_PER_SECOND || '10');
var TODAY         = DATE_OVERRIDE || new Date().toLocaleDateString('en-CA');
var MODE          = FAST2SMS_KEY ? 'FAST2SMS' : 'WEBHOOK';

// ─── SPEED CONFIG ───────────────────────────────────────
// WEBHOOK = testing, no rate limit needed
// FAST2SMS = production, DLT rate limit needed
var NUMS_PER_CALL, WAIT_MS, PARALLEL;

if (MODE === 'FAST2SMS') {
  NUMS_PER_CALL = 100;  // 100 numbers per API call
  WAIT_MS = Math.ceil((100 / SMS_PER_SEC) * 1000); // ~10 sec
  PARALLEL = 1;         // 1 call at a time (safe)
} else {
  NUMS_PER_CALL = 1;    // 1 number per webhook call
  WAIT_MS = 0;          // NO wait between calls
  PARALLEL = 20;        // 20 parallel calls (FAST!)
}

var DB_PAGE     = 1000;
var INSERT_SIZE = 200;

var ST = { queued:0, total:0, sent:0, failed:0, skipped:0, calls:0, t0:Date.now() };

var LOG_DIR  = './logs';
var LOG_FILE = path.join(LOG_DIR, 'sms-' + TODAY + '.log');
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

function log(lvl, msg, data) {
  var ts  = new Date().toISOString();
  var sec = Math.round((Date.now() - ST.t0) / 1000);
  var mb  = Math.round(process.memoryUsage().heapUsed / 1048576);
  var out = '[' + ts + '][' + sec + 's][' + mb + 'MB][' + lvl + '] ' + msg;
  console.log(out, data !== undefined ? JSON.stringify(data) : '');
  fs.appendFileSync(LOG_FILE, JSON.stringify({ ts:ts, sec:sec, mb:mb, lvl:lvl, msg:msg, data:data }) + '\n');
}

var sleep = function(ms) { return new Promise(function(r) { setTimeout(r, ms); }); };

function checkEnv() {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    log('ERROR', 'SUPABASE_URL ya SUPABASE_KEY missing!');
    process.exit(1);
  }
  log('INFO', '='.repeat(55));
  log('INFO', '  FestiveSMS Automation');
  log('INFO', '='.repeat(55));
  log('INFO', 'Date      : ' + TODAY);
  log('INFO', 'Mode      : ' + MODE + (DRY_RUN ? ' [DRY RUN]' : ''));

  if (MODE === 'FAST2SMS') {
    log('INFO', 'Speed     : ' + SMS_PER_SEC + ' SMS/sec (DLT safe)');
    log('INFO', 'Per call  : ' + NUMS_PER_CALL + ' numbers');
    log('INFO', 'Wait      : ' + WAIT_MS + 'ms between batches');
    log('INFO', '50K est   : ~' + Math.ceil(50000 / SMS_PER_SEC / 60) + ' min');
  } else {
    log('INFO', 'Speed     : FAST (no rate limit)');
    log('INFO', 'Parallel  : ' + PARALLEL + ' at a time');
    log('INFO', '50K est   : ~' + Math.ceil(50000 / PARALLEL / 2 / 60) + ' min');
    log('INFO', 'Webhook   : ' + WEBHOOK_URL);
  }
  log('INFO', '='.repeat(55));
}

var db = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { autoRefreshToken: false, persistSession: false }
});

function buildMsg(customer, occasion, shop, addr) {
  return 'Dear ' + customer + ', ' + occasion + ' ki hardik shubhkamnayein '
    + shop + ' ki taraf se. Please visit our shop at '
    + (addr || 'our shop') + ' for gifts and heavy discounts. ' + shop;
}

async function dbFetchAll(table, select, filters) {
  var rows = [];
  var from = 0;
  var more = true;
  while (more) {
    var q = db.from(table).select(select).range(from, from + DB_PAGE - 1).order('id');
    var keys = Object.keys(filters || {});
    for (var i = 0; i < keys.length; i++) {
      q = q.eq(keys[i], filters[keys[i]]);
    }
    var res = await q;
    if (res.error) throw new Error('DB error ' + table + ': ' + res.error.message);
    rows = rows.concat(res.data || []);
    more = (res.data || []).length === DB_PAGE;
    from += DB_PAGE;
  }
  return rows;
}

async function dbUpdate(ids, status) {
  if (!ids || !ids.length) return;
  var update = { status: status };
  if (status === 'sent') update.sent_at = new Date().toISOString();
  for (var i = 0; i < ids.length; i += 500) {
    var chunk = ids.slice(i, i + 500);
    await db.from('sms_queue').update(update).in('id', chunk);
  }
}

// ════════════════════════════════════════════════
// STEP 1: QUEUE
// ════════════════════════════════════════════════
async function stepQueue() {
  log('INFO', '');
  log('INFO', '-- STEP 1: Queue SMS --');

  var festRes = await db.from('festivals').select('*').eq('date', TODAY);
  var todayFests = festRes.data || [];
  log('INFO', 'Festivals: ' + todayFests.length, { names: todayFests.map(function(f) { return f.name; }) });

  var userRes = await db.from('users')
    .select('id, shop_name, shop_address, plan_end_date')
    .eq('is_admin', false)
    .eq('subscription_active', true)
    .gte('plan_end_date', TODAY);
  var shops = userRes.data || [];
  log('INFO', 'Active shops: ' + shops.length);

  var tDate = new Date(TODAY + 'T00:00:00');
  var tM = tDate.getMonth();
  var tD = tDate.getDate();
  var totalQ = 0;

  for (var si = 0; si < shops.length; si++) {
    var shop = shops[si];
    try {
      var custs = await dbFetchAll('customers', 'id, customer_name, phone_number, dob', { user_id: shop.id });
      if (!custs.length) continue;

      var existing = await dbFetchAll('sms_queue', 'customer_id, sms_type, festival_id', { user_id: shop.id, scheduled_date: TODAY });
      var has = {};
      existing.forEach(function(e) {
        var k = e.sms_type === 'festival' ? e.customer_id + '_f_' + e.festival_id : e.customer_id + '_' + e.sms_type;
        has[k] = true;
      });

      var rows = [];

      custs.forEach(function(c) {
        if (!c.dob) return;
        var d = new Date(c.dob + 'T00:00:00');
        if (d.getMonth() === tM && d.getDate() === tD) {
          if (!has[c.id + '_birthday']) {
            rows.push({
              user_id: shop.id, customer_id: c.id, festival_id: null, sms_type: 'birthday',
              message: buildMsg(c.customer_name, 'Janmdin', shop.shop_name, shop.shop_address),
              status: 'pending', scheduled_date: TODAY
            });
          }
        }
      });

      todayFests.forEach(function(f) {
        custs.forEach(function(c) {
          if (!has[c.id + '_f_' + f.id]) {
            rows.push({
              user_id: shop.id, customer_id: c.id, festival_id: f.id, sms_type: 'festival',
              message: buildMsg(c.customer_name, f.name, shop.shop_name, shop.shop_address),
              status: 'pending', scheduled_date: TODAY
            });
          }
        });
      });

      for (var ri = 0; ri < rows.length; ri += INSERT_SIZE) {
        var batch = rows.slice(ri, ri + INSERT_SIZE);
        var insRes = await db.from('sms_queue').insert(batch);
        if (!insRes.error) totalQ += batch.length;
      }

      if (rows.length) log('INFO', shop.shop_name + ': ' + rows.length + ' queued');
    } catch (e) {
      log('ERROR', 'Shop fail: ' + shop.shop_name, { error: e.message });
    }
  }

  ST.queued = totalQ;
  log('INFO', 'Queue done: ' + totalQ + ' SMS');
}

// ════════════════════════════════════════════════
// STEP 2: SEND
// ════════════════════════════════════════════════
async function stepSend() {
  log('INFO', '');
  log('INFO', '-- STEP 2: Send SMS --');

  var pending = await dbFetchAll('sms_queue',
    'id, message, sms_type, customers(customer_name, phone_number), users!sms_queue_user_id_fkey(shop_name, subscription_active, plan_end_date)',
    { scheduled_date: TODAY, status: 'pending' }
  );

  log('INFO', 'Pending: ' + pending.length);

  var valid = [];
  pending.forEach(function(r) {
    if (!r.customers || !r.customers.phone_number) { ST.skipped++; return; }
    if (!r.users || !r.users.subscription_active) { ST.skipped++; return; }
    if (r.users.plan_end_date && r.users.plan_end_date < TODAY) { ST.skipped++; return; }
    var phone = String(r.customers.phone_number).replace(/\D/g, '').slice(-10);
    if (phone.length !== 10) { ST.skipped++; return; }
    valid.push({ id: r.id, phone: phone, message: r.message, smsType: r.sms_type, shop: r.users.shop_name || '' });
  });

  ST.total = valid.length;
  log('INFO', 'Valid: ' + ST.total + ' | Skipped: ' + ST.skipped);
  if (!ST.total) { log('INFO', 'Nothing to send'); return; }

  if (MODE === 'FAST2SMS') {
    await sendFast2SMSMode(valid);
  } else {
    await sendWebhookMode(valid);
  }
}

// ════════════════════════════════════════════════
// WEBHOOK MODE — FAST! No rate limit
// 20 parallel calls, no waiting
// 13 SMS = ~2 seconds ✅
// 50,000 SMS = ~5 minutes ✅
// ════════════════════════════════════════════════
async function sendWebhookMode(records) {
  log('INFO', 'WEBHOOK MODE — Fast sending');
  log('INFO', 'Parallel: ' + PARALLEL + ' | Total: ' + records.length);

  var estSec = Math.ceil(records.length / PARALLEL * 0.5);
  log('INFO', 'Estimated: ~' + estSec + ' seconds');

  for (var i = 0; i < records.length; i += PARALLEL) {
    var chunk = records.slice(i, i + PARALLEL);

    var results = await Promise.allSettled(
      chunk.map(function(r) {
        ST.calls++;
        return fetch(WEBHOOK_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            phone: r.phone,
            message: r.message,
            shop: r.shop,
            type: r.smsType,
            date: TODAY,
            ts: new Date().toISOString(),
            mode: 'TESTING'
          }),
          signal: AbortSignal.timeout(10000)
        }).then(function(resp) {
          return { id: r.id, ok: resp.ok || resp.status < 400 };
        }).catch(function() {
          return { id: r.id, ok: false };
        });
      })
    );

    var sentIds = [];
    var failedIds = [];

    results.forEach(function(r) {
      var v = r.status === 'fulfilled' ? r.value : { ok: false };
      if (v.ok && v.id) sentIds.push(v.id);
      else if (v.id) failedIds.push(v.id);
    });

    await dbUpdate(sentIds, 'sent');
    await dbUpdate(failedIds, 'failed');
    ST.sent += sentIds.length;
    ST.failed += failedIds.length;

    // Progress har 100 SMS
    if ((i + PARALLEL) % 100 < PARALLEL || i + PARALLEL >= records.length) {
      var done = Math.min(i + PARALLEL, records.length);
      var sec = Math.round((Date.now() - ST.t0) / 1000);
      var pct = Math.round(done / records.length * 100);
      log('INFO', done + '/' + records.length + ' | ' + pct + '% | ' + sec + 's | sent:' + ST.sent + ' fail:' + ST.failed);
    }

    // Tiny delay to not overwhelm webhook.site
    if (i + PARALLEL < records.length) {
      await sleep(100); // Just 100ms between groups
    }
  }
}

// ════════════════════════════════════════════════
// FAST2SMS MODE — Slow and safe for DLT
// 100 numbers per API call
// 10 sec wait between calls
// 50,000 SMS = ~83 minutes ✅
// ════════════════════════════════════════════════
async function sendFast2SMSMode(records) {
  log('INFO', 'FAST2SMS MODE — Rate limited');
  log('INFO', 'Rate: ' + SMS_PER_SEC + '/sec | Wait: ' + WAIT_MS + 'ms');

  // Group by message
  var groups = {};
  records.forEach(function(r) {
    if (!groups[r.message]) groups[r.message] = [];
    groups[r.message].push(r);
  });

  // Make batches of NUMS_PER_CALL
  var batches = [];
  var keys = Object.keys(groups);
  keys.forEach(function(msg) {
    var recs = groups[msg];
    for (var i = 0; i < recs.length; i += NUMS_PER_CALL) {
      var chunk = recs.slice(i, i + NUMS_PER_CALL);
      batches.push({
        message: msg,
        records: chunk,
        phones: chunk.map(function(r) { return r.phone; }),
        shop: chunk[0].shop
      });
    }
  });

  var total = batches.length;
  var estMin = Math.ceil(total * WAIT_MS / 60000);
  log('INFO', 'Batches: ' + total + ' | Est: ~' + estMin + ' min');

  for (var i = 0; i < total; i++) {
    var batch = batches[i];
    ST.calls++;

    var sentIds = [];
    var failedIds = [];

    try {
      var res = await fetch('https://www.fast2sms.com/dev/bulkV2', {
        method: 'POST',
        headers: { 'authorization': FAST2SMS_KEY, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          route: 'q', message: batch.message,
          language: 'english', flash: 0,
          numbers: batch.phones.join(',')
        }),
        signal: AbortSignal.timeout(30000)
      });

      var text = await res.text();
      var json; try { json = JSON.parse(text); } catch(e) { json = {}; }

      if (res.ok && json.return === true) {
        sentIds = batch.records.map(function(r) { return r.id; });
      } else {
        throw new Error('HTTP ' + res.status + ': ' + text);
      }
    } catch (err) {
      log('WARN', 'F2S fail batch ' + (i+1), { err: err.message });
      failedIds = batch.records.map(function(r) { return r.id; });
    }

    await dbUpdate(sentIds, 'sent');
    await dbUpdate(failedIds, 'failed');
    ST.sent += sentIds.length;
    ST.failed += failedIds.length;

    // Progress
    if ((i+1) % 50 === 0 || i === total - 1) {
      var sec = Math.round((Date.now() - ST.t0) / 1000);
      var pct = Math.round((ST.sent + ST.failed) / ST.total * 100);
      var remMin = Math.ceil((total - i - 1) * WAIT_MS / 60000);
      log('INFO', '[' + (i+1) + '/' + total + '] sent:' + ST.sent + ' fail:' + ST.failed + ' ' + pct + '% ~' + remMin + 'min left');
    }

    // DLT safe rate limit
    if (i < total - 1) await sleep(WAIT_MS);
  }
}

// ════════════════════════════════════════════════
// REPORT
// ════════════════════════════════════════════════
function report() {
  var sec = Math.round((Date.now() - ST.t0) / 1000);
  var pct = ST.total > 0 ? Math.round(ST.sent / ST.total * 100) : 0;
  var spd = sec > 0 ? Math.round(ST.sent / sec) : 0;
  var mb  = Math.round(process.memoryUsage().heapUsed / 1048576);

  log('INFO', '');
  log('INFO', '='.repeat(55));
  log('INFO', '  FINAL REPORT');
  log('INFO', '='.repeat(55));
  log('INFO', '  Date    : ' + TODAY);
  log('INFO', '  Mode    : ' + MODE + (DRY_RUN ? ' [DRY]' : ''));
  log('INFO', '  Queued  : ' + ST.queued);
  log('INFO', '  Total   : ' + ST.total);
  log('INFO', '  Sent    : ' + ST.sent);
  log('INFO', '  Failed  : ' + ST.failed);
  log('INFO', '  Skipped : ' + ST.skipped);
  log('INFO', '  Rate    : ' + pct + '%');
  log('INFO', '  Time    : ' + sec + 's');
  log('INFO', '  Speed   : ' + spd + ' SMS/sec');
  log('INFO', '  Calls   : ' + ST.calls);
  log('INFO', '  Memory  : ' + mb + 'MB');
  log('INFO', '='.repeat(55));

  fs.writeFileSync(
    path.join(LOG_DIR, 'report-' + TODAY + '.json'),
    JSON.stringify({
      date: TODAY, mode: MODE, queued: ST.queued,
      total: ST.total, sent: ST.sent, failed: ST.failed,
      skipped: ST.skipped, pct: pct + '%',
      time: sec + 's', speed: spd + '/sec',
      calls: ST.calls, memory: mb + 'MB'
    }, null, 2)
  );
}

// ════════════════════════════════════════════════
// MAIN
// ════════════════════════════════════════════════
async function main() {
  try {
    checkEnv();
    await stepQueue();
    await stepSend();
    report();
    if (ST.total > 0 && ST.failed / ST.total > 0.5) {
      log('ERROR', '>50% failed');
      process.exit(1);
    }
  } catch (err) {
    log('ERROR', 'CRASH: ' + err.message, { stack: err.stack });
    process.exit(1);
  }
}

main();
