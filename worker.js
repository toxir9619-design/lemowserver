// Lemov live-ops Worker (Cloudflare Workers + D1). Binding: DB. Secrets: ADMIN_TOKEN, HMAC_KEY. Optional: ALLOW_ORIGIN, RETAIN_DAYS.
const TE = new TextEncoder(), TD = new TextDecoder();
const DDL = [
  "CREATE TABLE IF NOT EXISTS players(iid TEXT PRIMARY KEY,first_ts INT,last_ts INT,sessions INT DEFAULT 0,play_s INT DEFAULT 0,build INT,ver TEXT,dev TEXT,os TEXT,gpu TEXT,ram INT,q INT,lang TEXT,country TEXT,scr TEXT,fps_w REAL DEFAULT 0,fps_s REAL DEFAULT 0,nick TEXT,lvl INT,xp INT,coins INT,cars TEXT,car TEXT,races INT,wins INT,km REAL,tags TEXT,note TEXT,lockj TEXT)",
  "CREATE INDEX IF NOT EXISTS pl_last ON players(last_ts)",
  "CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT,ts INT,iid TEXT,n TEXT,d TEXT)",
  "CREATE INDEX IF NOT EXISTS ev_ts ON events(ts)",
  "CREATE INDEX IF NOT EXISTS ev_n ON events(n,ts)",
  "CREATE INDEX IF NOT EXISTS ev_iid ON events(iid,ts)",
  "CREATE TABLE IF NOT EXISTS act(day TEXT,iid TEXT,sessions INT DEFAULT 0,play_s INT DEFAULT 0,PRIMARY KEY(day,iid))",
  "CREATE INDEX IF NOT EXISTS act_iid ON act(iid,day)",
  "CREATE TABLE IF NOT EXISTS inbox(id TEXT PRIMARY KEY,iid TEXT,kind TEXT,payload TEXT,created_ts INT,delivered_ts INT)",
  "CREATE INDEX IF NOT EXISTS ib_iid ON inbox(iid)",
  "CREATE TABLE IF NOT EXISTS ad_daily(day TEXT,ad TEXT,place TEXT,country TEXT,imp INT DEFAULT 0,click INT DEFAULT 0,close INT DEFAULT 0,reward INT DEFAULT 0,PRIMARY KEY(day,ad,place,country))",
];
const CRASH = "('crash','unclean','error')";
const ready = new WeakSet();
const rl = new Map(), fails = new Map();

const J = (o, s = 200) => new Response(JSON.stringify(o), { status: s, headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' } });
const E = (err, s = 400, msg) => J({ ok: false, err, msg }, s);
const now = env => (env && env.NOW) || Math.floor(Date.now() / 1000);
const dayOf = ts => new Date(ts * 1000).toISOString().slice(0, 10);
const hex = u8 => [...u8].map(b => b.toString(16).padStart(2, '0')).join('');
const pj = v => { try { return v ? JSON.parse(v) : null; } catch { return null; } };
const num = (v, lo, hi) => (Number.isFinite(+v) && v !== null && v !== '' ? Math.max(lo, Math.min(hi, Math.round(+v))) : null);
const str = (v, n) => (typeof v === 'string' && v.length ? v.slice(0, n) : null);
const rid = () => hex(crypto.getRandomValues(new Uint8Array(4)));
async function hmac(key, data) {
  const k = await crypto.subtle.importKey('raw', TE.encode(key), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  return hex(new Uint8Array(await crypto.subtle.sign('HMAC', k, data)));
}
function ceq(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string' || a.length !== b.length) return false;
  let r = 0;
  for (let i = 0; i < a.length; i++) r |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return r === 0;
}
async function init(env) {
  if (ready.has(env.DB)) return;
  await env.DB.batch(DDL.map(s => env.DB.prepare(s)));
  ready.add(env.DB);
}
const all = async (env, sql, ...a) => (await env.DB.prepare(sql).bind(...a).all()).results || [];
const one = async (env, sql, ...a) => (await env.DB.prepare(sql).bind(...a).first()) || {};
const run = (env, sql, ...a) => env.DB.prepare(sql).bind(...a).run();

export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    const cors = { 'access-control-allow-origin': env.ALLOW_ORIGIN || '*', 'access-control-allow-headers': 'authorization,content-type', 'access-control-allow-methods': 'GET,POST,DELETE,OPTIONS', 'access-control-max-age': '86400', vary: 'origin' };
    let res;
    try {
      if (req.method === 'OPTIONS') res = new Response(null, { status: 204 });
      else if (!env.DB) res = E('no_db', 500);
      else {
        await init(env);
        if (u.pathname === '/e' && req.method === 'POST') res = await ingest(req, env);
        else if (u.pathname === '/h') res = J({ ok: true, t: now(env) });
        else if (u.pathname.startsWith('/api/')) res = await admin(req, env, u);
        else res = E('not_found', 404);
      }
    } catch (e) { if (env.DEBUG) console.error(e); res = E('internal', 500); }
    const r = new Response(res.body, res);
    for (const k in cors) r.headers.set(k, cors[k]);
    return r;
  },
  async scheduled(ev, env, ctx) { ctx.waitUntil(init(env).then(() => purge(env, { events_older_days: +env.RETAIN_DAYS || 120 }))); },
};

const evChunks = (env, iid, evs) => { const out = []; for (let i = 0; i < evs.length; i += 20) { const part = evs.slice(i, i + 20); out.push(env.DB.prepare('INSERT INTO events(ts,iid,n,d) VALUES ' + part.map(() => '(?,?,?,?)').join(',')).bind(...part.flatMap(e => [e[0], iid, e[1], e[2]]))); } return out; };
// targeting (same semantics as the game's WHEN evaluator)
function whenMatch(w, c) {
  if (!w || typeof w !== 'object') return true;
  const n = k => w[k] !== undefined && w[k] !== null, l = k => Array.isArray(w[k]) && w[k].length > 0;
  const ge = (k, v) => !n(k) || (v != null && v >= w[k]), le = (k, v) => !n(k) || (v != null && v <= w[k]);
  const sub = (k, v) => !l(k) || (!!v && w[k].some(s => v.toLowerCase().includes(String(s).toLowerCase())));
  return ge('min_build', c.build) && le('max_build', c.build) && (!l('builds') || w.builds.includes(c.build))
    && (!l('countries') || (!!c.country && w.countries.some(x => String(x).toUpperCase() === c.country.toUpperCase())))
    && (!l('langs') || (!!c.lang && w.langs.some(x => String(x).toLowerCase() === c.lang.toLowerCase())))
    && (!l('quality') || w.quality.includes(c.quality)) && ge('ram_min', c.ram) && le('ram_max', c.ram) && sub('gpu', c.gpu) && sub('dev', c.dev)
    && (!(n('pct') && Array.isArray(w.pct) && w.pct.length === 2) || (c.bucket != null && w.pct[0] <= c.bucket && c.bucket < w.pct[1]))
    && ge('days_min', c.days) && le('days_max', c.days) && ge('lvl_min', c.lvl) && le('lvl_max', c.lvl);
}
async function bucket(iid) { const d = new Uint8Array(await crypto.subtle.digest('SHA-256', TE.encode(iid))); return ((d[0] << 24 | d[1] << 16 | d[2] << 8 | d[3]) >>> 0) % 100; }
async function matchPlayers(env, when, t, cap) {
  const rows = await all(env, 'SELECT iid,first_ts,build,country,lang,q,ram,gpu,dev,lvl,nick FROM players ORDER BY last_ts DESC LIMIT ?', cap);
  const needB = !!(when && Array.isArray(when.pct) && when.pct.length === 2), out = [];
  for (const r of rows) {
    const c = { build: r.build, country: r.country, lang: r.lang, quality: r.q, ram: r.ram, gpu: r.gpu, dev: r.dev, days: Math.floor((t - r.first_ts) / 86400), lvl: r.lvl };
    if (needB) c.bucket = await bucket(r.iid);
    if (whenMatch(when, c)) out.push(r);
  }
  return { total: rows.length, rows: out, capped: rows.length >= cap };
}
// ---------- game -> worker ----------
async function ingest(req, env) {
  const cl = +req.headers.get('content-length') || 0;
  if (cl > 16384) return E('too_big', 413);
  const raw = new Uint8Array(await req.arrayBuffer());
  if (raw.length > 16384) return E('too_big', 413);
  if (!env.HMAC_KEY || !ceq(await hmac(env.HMAC_KEY, raw), (req.headers.get('x-s') || '').toLowerCase())) return E('sig', 401);
  let b;
  try { b = JSON.parse(TD.decode(raw)); } catch { return E('bad_json'); }
  if (!b || typeof b !== 'object' || typeof b.iid !== 'string' || !/^[0-9a-f]{16}$/.test(b.iid)) return E('bad_field');
  const t = now(env), iid = b.iid, today = dayOf(t);
  const rr = rl.get(iid);
  if (rr && t - rr.t < 3600) { if (++rr.n > 120) return E('rate', 429); } else rl.set(iid, { t, n: 1 });
  if (rl.size > 5000) rl.clear();
  const cc = (req.headers.get('cf-ipcountry') || req.headers.get('x-test-country') || '').toUpperCase();
  const country = /^[A-Z]{2}$/.test(cc) && cc !== 'XX' && cc !== 'T1' ? cc : '';
  const p = b.prof && typeof b.prof === 'object' ? b.prof : {};
  const evs = [];
  let sess = 0, dur = 0, fw = 0, fs = 0;
  const stm = [], adm = new Map();
  const adq = "INSERT INTO ad_daily(day,ad,place,country,imp,click,close,reward) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(day,ad,place,country) DO UPDATE SET imp=imp+excluded.imp,click=click+excluded.click,close=close+excluded.close,reward=reward+excluded.reward";
  for (const e of (Array.isArray(b.ev) ? b.ev : []).slice(0, 40)) {
    if (!e || typeof e.n !== 'string' || !/^[a-z0-9_]{1,24}$/.test(e.n)) continue;
    const d = e.d && typeof e.d === 'object' && !Array.isArray(e.d) ? e.d : {};
    let ds = JSON.stringify(d);
    if (ds.length > (e.n === 'crash' ? 4200 : 1024)) ds = JSON.stringify({ cut: 1 });
    const ts = Number.isFinite(+e.t) && Math.abs(+e.t - t) <= 172800 ? Math.round(+e.t) : t;
    evs.push([ts, e.n, ds]);
    if (e.n === 'session') {
      const du = num(d.dur_s, 0, 21600) || 0; sess++; dur += du;
      const f = Number(d.avg_fps);
      if (du > 0 && f > 0 && f < 1000) { fw += f * du; fs += du; }
    }
    const ak = { ad_imp: 2, ad_click: 3, ad_close: 4, ad_reward: 5 }[e.n], ad = str(d.ad, 40);
    if (ak && ad) { const pl = str(d.p, 16) || '', k = ad + '|' + pl, o = adm.get(k) || [ad, pl, 0, 0, 0, 0]; o[ak]++; adm.set(k, o); }
  }
  for (const o of adm.values()) stm.push(env.DB.prepare(adq).bind(today, o[0], o[1], country, o[2], o[3], o[4], o[5]));
  const cars = Array.isArray(p.cars) ? JSON.stringify(p.cars.filter(x => typeof x === 'string').slice(0, 200).map(x => x.slice(0, 40))) : null;
  const first = num(p.first, 1, t + 86400) || t;
  const up = "INSERT INTO players(iid,first_ts,last_ts,sessions,play_s,build,ver,dev,os,gpu,ram,q,lang,country,scr,fps_w,fps_s,nick,lvl,xp,coins,cars,car,races,wins,km) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(iid) DO UPDATE SET last_ts=excluded.last_ts,sessions=sessions+excluded.sessions,play_s=play_s+excluded.play_s,build=COALESCE(excluded.build,build),ver=COALESCE(excluded.ver,ver),dev=COALESCE(excluded.dev,dev),os=COALESCE(excluded.os,os),gpu=COALESCE(excluded.gpu,gpu),ram=COALESCE(excluded.ram,ram),q=COALESCE(excluded.q,q),lang=COALESCE(excluded.lang,lang),country=COALESCE(NULLIF(excluded.country,''),country),scr=COALESCE(excluded.scr,scr),fps_w=fps_w+excluded.fps_w,fps_s=fps_s+excluded.fps_s,nick=COALESCE(excluded.nick,nick),lvl=COALESCE(excluded.lvl,lvl),xp=COALESCE(excluded.xp,xp),coins=COALESCE(excluded.coins,coins),cars=COALESCE(excluded.cars,cars),car=COALESCE(excluded.car,car),races=COALESCE(excluded.races,races),wins=COALESCE(excluded.wins,wins),km=COALESCE(excluded.km,km)";
  const km = Number(p.km);
  const batch = [
    env.DB.prepare(up).bind(iid, first, t, sess, dur, num(b.b, 0, 1e6), str(b.v, 16), str(b.dev, 64), str(b.os, 32), str(b.gpu, 64), num(b.ram, 0, 65536), num(b.q, 0, 3), str(b.lang, 5), country, str(b.scr, 12), fw, fs,
      str(p.nick, 24), num(p.lvl, 0, 1e4), num(p.xp, 0, 1e9), num(p.coins, 0, 1e12), cars, str(p.car, 40), num(p.races, 0, 1e7), num(p.wins, 0, 1e7), Number.isFinite(km) ? Math.max(0, Math.min(1e7, km)) : null),
    env.DB.prepare("INSERT INTO act(day,iid,sessions,play_s) VALUES(?,?,?,?) ON CONFLICT(day,iid) DO UPDATE SET sessions=sessions+excluded.sessions,play_s=play_s+excluded.play_s").bind(today, iid, sess, dur),
    ...evChunks(env, iid, evs),
    ...stm,
  ];
  const ack = (Array.isArray(b.ack) ? b.ack : []).filter(x => typeof x === 'string' && /^[mg]_[0-9a-z]{4,16}$/.test(x)).slice(0, 20);
  if (ack.length) batch.push(env.DB.prepare(`UPDATE inbox SET delivered_ts=? WHERE iid=? AND delivered_ts IS NULL AND id IN (${ack.map(() => '?').join(',')})`).bind(t, iid, ...ack));
  await env.DB.batch(batch);
  const ib = await all(env, "SELECT id,kind,payload,created_ts FROM inbox WHERE iid=? AND delivered_ts IS NULL ORDER BY created_ts LIMIT 20", iid);
  const pl = await one(env, "SELECT lockj FROM players WHERE iid=?", iid);
  const msgs = [], grants = [];
  for (const r of ib) {
    const pay = pj(r.payload) || {};
    if (r.kind === 'msg') msgs.push({ id: r.id, ts: r.created_ts, title: pay.title || '', body: pay.body || '' });
    else grants.push({ id: r.id, ts: r.created_ts, grant: pay.grant, msg: pay.msg || null });
  }
  return J({ ok: true, t, cc: country, inbox: { msgs, grants, lock: pj(pl.lockj) } });
}

// ---------- admin ----------
const P = r => ({ iid: r.iid, nick: r.nick, first_ts: r.first_ts, last_ts: r.last_ts, sessions: r.sessions, play_s: r.play_s, build: r.build, ver: r.ver, dev: r.dev, os: r.os, gpu: r.gpu, ram: r.ram, q: r.q, lang: r.lang, country: r.country, scr: r.scr, avg_fps: r.fps_s > 0 ? Math.round(r.fps_w / r.fps_s * 10) / 10 : null, lvl: r.lvl, xp: r.xp, coins: r.coins, cars: pj(r.cars) || [], car: r.car, races: r.races, wins: r.wins, km: r.km, tags: pj(r.tags) || [], note: r.note || '', lock: pj(r.lockj) });
const L10 = (v, n) => { if (typeof v === 'string') return v.slice(0, n); if (v && typeof v === 'object') { const o = {}; for (const k of ['uz', 'ru', 'en']) if (typeof v[k] === 'string') o[k] = v[k].slice(0, n); return o; } return ''; };
function grantOk(g) {
  const one1 = x => x && ((['coins', 'xp'].includes(x.type) && Number.isInteger(x.amount) && x.amount >= 1 && x.amount <= 1e8) || (x.type === 'car' && typeof x.car === 'string' && /^[a-z0-9_]{1,40}$/.test(x.car)));
  if (Array.isArray(g)) return g.length > 0 && g.length <= 5 && g.every(one1) ? g.map(x => (x.type === 'car' ? { type: 'car', car: x.car } : { type: x.type, amount: x.amount })) : null;
  return one1(g) ? (g.type === 'car' ? { type: 'car', car: g.car } : { type: g.type, amount: g.amount }) : null;
}
async function admin(req, env, u) {
  const ip = req.headers.get('cf-connecting-ip') || 'x', f = fails.get(ip);
  if (f && f.n >= 8 && Date.now() - f.t < 60000) return E('throttled', 429);
  const tok = (req.headers.get('authorization') || '').replace(/^Bearer\s+/i, '');
  if (!env.ADMIN_TOKEN || !ceq(tok, env.ADMIN_TOKEN)) { fails.set(ip, { n: (f && Date.now() - f.t < 60000 ? f.n : 0) + 1, t: Date.now() }); return E('auth', 401); }
  fails.delete(ip);
  const path = u.pathname.slice(5), q = u.searchParams, m = req.method, t = now(env);
  const days = num(q.get('days') || 30, 1, 365), lim = num(q.get('limit') || 50, 1, 200), off = num(q.get('offset') || 0, 0, 1e9);
  let mm;
  if (path === 'health' && m === 'GET') return J({ ok: true, t, ver: 'w2', players: (await one(env, 'SELECT COUNT(*) c FROM players')).c, events: (await one(env, 'SELECT COUNT(*) c FROM events')).c });
  if (path === 'config') return J({ ok: true, retain_days: +env.RETAIN_DAYS || 120, allow_origin: env.ALLOW_ORIGIN || '*', version: 'w2' });
  if (path === 'stats') return stats(env, t, days);
  if (path === 'game') return game(env, t, days);
  if (path === 'ads') return ads(env, t, days);
  if (path === 'players' && m === 'GET') {
    const w = [], a = [];
    const qq = (q.get('q') || '').trim().slice(0, 40);
    if (qq) { const l = '%' + qq.replace(/[\\%_]/g, '\\$&') + '%'; w.push("(iid LIKE ? ESCAPE '\\' OR nick LIKE ? ESCAPE '\\' OR dev LIKE ? ESCAPE '\\' OR note LIKE ? ESCAPE '\\' OR tags LIKE ? ESCAPE '\\')"); a.push(l, l, l, l, l); }
    if (q.get('country')) { w.push('country=?'); a.push(q.get('country').toUpperCase().slice(0, 2)); }
    if (q.get('lang')) { w.push('lang=?'); a.push(q.get('lang').slice(0, 5)); }
    if (q.get('build')) { w.push('build=?'); a.push(num(q.get('build'), 0, 1e6)); }
    if (q.get('locked') === '1') w.push('lockj IS NOT NULL');
    if (q.get('active_days')) { w.push('last_ts>=?'); a.push(t - num(q.get('active_days'), 1, 3650) * 86400); }
    const cols = { last_ts: 'last_ts', first_ts: 'first_ts', play_s: 'play_s', sessions: 'sessions', lvl: 'lvl', coins: 'coins', avg_fps: '(fps_w/NULLIF(fps_s,0))', build: 'build' };
    const sort = cols[q.get('sort')] || 'last_ts', dir = q.get('dir') === 'asc' ? 'ASC' : 'DESC';
    const wh = w.length ? 'WHERE ' + w.join(' AND ') : '';
    const total = (await one(env, `SELECT COUNT(*) c FROM players ${wh}`, ...a)).c;
    const rows = await all(env, `SELECT * FROM players ${wh} ORDER BY ${sort} ${dir} LIMIT ? OFFSET ?`, ...a, lim, off);
    return J({ ok: true, total, rows: rows.map(P) });
  }
  if ((mm = path.match(/^player\/([0-9a-f]{16})(\/send)?$/))) {
    const iid = mm[1];
    const pl = await one(env, 'SELECT * FROM players WHERE iid=?', iid);
    if (!pl.iid) return E('not_found', 404);
    if (!mm[2] && m === 'GET') {
      const events = (await all(env, 'SELECT id,ts,n,d FROM events WHERE iid=? ORDER BY ts DESC,id DESC LIMIT 200', iid)).map(e => ({ ...e, d: pj(e.d) || {} }));
      const inbox = (await all(env, 'SELECT id,kind,payload,created_ts,delivered_ts FROM inbox WHERE iid=? ORDER BY created_ts DESC LIMIT 50', iid)).map(r => ({ id: r.id, kind: r.kind, payload: pj(r.payload), created_ts: r.created_ts, delivered_ts: r.delivered_ts }));
      const daily = await all(env, 'SELECT day,sessions,play_s FROM act WHERE iid=? ORDER BY day DESC LIMIT 60', iid);
      return J({ ok: true, player: P(pl), events, inbox, daily });
    }
    if (m === 'DELETE' && !mm[2]) {
    const r = await run(env, 'DELETE FROM events WHERE iid=?', iid);
    await env.DB.batch([env.DB.prepare('DELETE FROM players WHERE iid=?').bind(iid), env.DB.prepare('DELETE FROM act WHERE iid=?').bind(iid), env.DB.prepare('DELETE FROM inbox WHERE iid=?').bind(iid)]);
    return J({ ok: true, deleted: { events: r.meta.changes } });
    }
    const body = await req.json().catch(() => null);
    if (!body || typeof body !== 'object') return E('bad_json');
    if (!mm[2] && m === 'POST') {
      const set = [], a = [];
      if ('note' in body) { set.push('note=?'); a.push(String(body.note || '').slice(0, 500)); }
      if ('tags' in body) { set.push('tags=?'); a.push(JSON.stringify((Array.isArray(body.tags) ? body.tags : []).filter(x => typeof x === 'string' && x).slice(0, 10).map(x => x.slice(0, 24)))); }
      if ('lock' in body) { set.push('lockj=?'); a.push(body.lock && body.lock.msg ? JSON.stringify({ msg: L10(body.lock.msg, 300) }) : null); }
      if (set.length) await run(env, `UPDATE players SET ${set.join(',')} WHERE iid=?`, ...a, iid);
      return J({ ok: true });
    }
    if (mm[2] && m === 'POST') {
      const id = (body.kind === 'msg' ? 'm_' : 'g_') + rid();
      let pay;
      if (body.kind === 'msg') pay = { title: L10(body.title, 120), body: L10(body.body, 1000) };
      else if (body.kind === 'grant') { const g = grantOk(body.grant); if (!g) return E('bad_grant'); pay = { grant: g, msg: body.msg ? { title: L10(body.msg.title, 120), body: L10(body.msg.body, 1000) } : null }; }
      else return E('bad_kind');
      await run(env, 'INSERT INTO inbox(id,iid,kind,payload,created_ts,delivered_ts) VALUES(?,?,?,?,?,NULL)', id, iid, body.kind, JSON.stringify(pay), t);
      return J({ ok: true, id });
    }
  }
  if (path === 'match' && m === 'POST') {
    const b = await req.json().catch(() => ({})), r = await matchPlayers(env, b.when, t, 5000);
    return J({ ok: true, total: r.total, matched: r.rows.length, capped: r.capped, sample: r.rows.slice(0, 5).map(x => ({ iid: x.iid, nick: x.nick, dev: x.dev, country: x.country })) });
  }
  if (path === 'broadcast' && m === 'POST') {
    const b = await req.json().catch(() => null);
    if (!b) return E('bad_json');
    let pay, pre;
    if (b.kind === 'msg') { pay = { title: L10(b.title, 120), body: L10(b.body, 1000) }; pre = 'm_'; }
    else if (b.kind === 'grant') { const g = grantOk(b.grant); if (!g) return E('bad_grant'); pay = { grant: g, msg: b.msg ? { title: L10(b.msg.title, 120), body: L10(b.msg.body, 1000) } : null }; pre = 'g_'; }
    else return E('bad_kind');
    const r = await matchPlayers(env, b.when, t, 800), js = JSON.stringify(pay), st = [];
    for (let i = 0; i < r.rows.length; i += 19) {
      const part = r.rows.slice(i, i + 19);
      st.push(env.DB.prepare('INSERT OR IGNORE INTO inbox(id,iid,kind,payload,created_ts,delivered_ts) VALUES ' + part.map(() => '(?,?,?,?,?,NULL)').join(',')).bind(...part.flatMap(x => [pre + rid() + rid().slice(0, 4), x.iid, b.kind, js, t])));
    }
    if (st.length) await env.DB.batch(st);
    return J({ ok: true, sent: r.rows.length, capped: r.capped });
  }
  if (path === 'event_names') return J({ ok: true, names: await all(env, 'SELECT n, COUNT(*) count FROM events GROUP BY n ORDER BY count DESC LIMIT 100') });
  if (path === 'events') {
    const w = [], a = [];
    if (q.get('n')) { w.push('n=?'); a.push(q.get('n').slice(0, 24)); }
    if (q.get('iid')) { w.push('iid=?'); a.push(q.get('iid').slice(0, 16)); }
    if (q.get('from')) { w.push('ts>=?'); a.push(num(q.get('from'), 0, 4e9)); }
    if (q.get('to')) { w.push('ts<=?'); a.push(num(q.get('to'), 0, 4e9)); }
    const wh = w.length ? 'WHERE ' + w.join(' AND ') : '';
    const total = (await one(env, `SELECT COUNT(*) c FROM events ${wh}`, ...a)).c;
    const rows = (await all(env, `SELECT id,ts,iid,n,d FROM events ${wh} ORDER BY ts DESC,id DESC LIMIT ? OFFSET ?`, ...a, lim, off)).map(e => ({ ...e, d: pj(e.d) || {} }));
    return J({ ok: true, total, rows });
  }
  if (path === 'crashes') {
    const total = (await one(env, `SELECT COUNT(*) c FROM events WHERE n IN ${CRASH}`)).c;
    const rows = (await all(env, `SELECT e.id,e.ts,e.iid,e.n,e.d,p.build b,p.ver v,p.dev,p.os FROM events e LEFT JOIN players p ON p.iid=e.iid WHERE e.n IN ${CRASH} ORDER BY e.ts DESC,e.id DESC LIMIT ? OFFSET ?`, lim, off))
      .map(r => { const d = pj(r.d) || {}; return { id: r.id, ts: r.ts, iid: r.iid, n: r.n, b: r.b, v: r.v, dev: r.dev, os: r.os, text: String(d.text || d.last_scene || '').slice(0, 4096) }; });
    return J({ ok: true, total, rows });
  }
  if (path === 'export') {
    const tb = q.get('table'), fmt = q.get('format') === 'csv' ? 'csv' : 'json', n = num(q.get('limit') || 100000, 1, 100000);
    const sql = { players: 'SELECT * FROM players ORDER BY last_ts DESC', events: 'SELECT id,ts,iid,n,d FROM events ORDER BY id DESC', crashes: `SELECT id,ts,iid,n,d FROM events WHERE n IN ${CRASH} ORDER BY id DESC`, ads: 'SELECT * FROM ad_daily ORDER BY day DESC', daily: 'SELECT * FROM act ORDER BY day DESC' }[tb];
    if (!sql) return E('bad_table');
    let rows = await all(env, sql + ' LIMIT ?', n);
    if (tb === 'players') rows = rows.map(r => { const { fps_w, fps_s, lockj, ...x } = r; return { ...x, avg_fps: fps_s > 0 ? fps_w / fps_s : null, lock: lockj }; });
    const hd = { 'content-disposition': `attachment; filename="${tb}.${fmt}"` };
    if (fmt === 'json') { const r = J({ ok: true, rows }); for (const k in hd) r.headers.set(k, hd[k]); return r; }
    const cols = rows.length ? Object.keys(rows[0]) : [];
    const cell = v => { if (v == null) return ''; const s = typeof v === 'object' ? JSON.stringify(v) : String(v); return /[",\n\r]/.test(s) || /^[=+\-@]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
    return new Response([cols.join(','), ...rows.map(r => cols.map(c => cell(r[c])).join(','))].join('\n'), { headers: { 'content-type': 'text/csv; charset=utf-8', ...hd } });
  }
  if (path === 'purge' && m === 'POST') {
    const b = await req.json().catch(() => ({}));
    return J({ ok: true, deleted: await purge(env, b || {}) });
  }
  return E('not_found', 404);
}
async function purge(env, b) {
  const t = now(env), out = { events: 0, crashes: 0, players: 0 };
  const ev = num(b.events_older_days, 1, 3650), cr = num(b.crashes_older_days, 1, 3650), pl = num(b.inactive_players_days, 1, 3650);
  if (cr) out.crashes = (await run(env, `DELETE FROM events WHERE n IN ${CRASH} AND ts<?`, t - cr * 86400)).meta.changes;
  if (ev) out.events = (await run(env, `DELETE FROM events WHERE n NOT IN ${CRASH} AND ts<?`, t - ev * 86400)).meta.changes;
  if (pl) {
    const ids = (await all(env, 'SELECT iid FROM players WHERE last_ts<? LIMIT 5000', t - pl * 86400)).map(r => r.iid);
    for (let i = 0; i < ids.length; i += 50) {
      const part = ids.slice(i, i + 50), ph = part.map(() => '?').join(',');
      await env.DB.batch(['events', 'act', 'inbox', 'players'].map(tb => env.DB.prepare(`DELETE FROM ${tb} WHERE iid IN (${ph})`).bind(...part)));
    }
    out.players = ids.length;
  }
  return out;
}
async function stats(env, t, days) {
  const today = dayOf(t), t0 = Math.floor(t / 86400) * 86400, dl = [];
  for (let i = days - 1; i >= 0; i--) dl.push(dayOf(t - i * 86400));
  const start = dl[0], st = t0 - (days - 1) * 86400, d7 = dayOf(t - 6 * 86400), d30 = dayOf(t - 29 * 86400);
  const tot = await one(env, 'SELECT COUNT(*) n, SUM(sessions) s, SUM(play_s) p, SUM(fps_w) w, SUM(fps_s) fs FROM players');
  const act = async d => (await one(env, 'SELECT COUNT(DISTINCT iid) c FROM act WHERE day>=?', d)).c;
  const sa = new Map((await all(env, 'SELECT day,COUNT(*) dau,SUM(sessions) s,SUM(play_s) p FROM act WHERE day>=? GROUP BY day', start)).map(r => [r.day, r]));
  const nw = new Map((await all(env, "SELECT date(first_ts,'unixepoch') day,COUNT(*) c FROM players WHERE first_ts>=? GROUP BY day", st)).map(r => [r.day, r.c]));
  const cr = new Map((await all(env, `SELECT date(ts,'unixepoch') day,COUNT(*) c FROM events WHERE n IN ${CRASH} AND ts>=? GROUP BY day`, st)).map(r => [r.day, r.c]));
  const series = dl.map(d => { const r = sa.get(d) || {}; return { day: d, dau: r.dau || 0, new: nw.get(d) || 0, sessions: r.s || 0, play_h: Math.round((r.p || 0) / 36) / 100, crashes: cr.get(d) || 0 }; });
  const ret = {};
  for (const k of [1, 7, 30]) {
    const r = await one(env, "SELECT COUNT(*) c, SUM(EXISTS(SELECT 1 FROM act a WHERE a.iid=p.iid AND a.day=date(p.first_ts,'unixepoch','+'||?||' day'))) r FROM players p WHERE date(p.first_ts,'unixepoch')<=? AND date(p.first_ts,'unixepoch')>=?", k, dayOf(t - k * 86400), dayOf(t - 60 * 86400));
    ret['d' + k] = r.c > 0 ? Math.round((r.r || 0) / r.c * 1000) / 1000 : null;
  }
  const bd = async col => (await all(env, `SELECT ${col} k, COUNT(*) v FROM players WHERE ${col} IS NOT NULL AND ${col}<>'' GROUP BY ${col} ORDER BY v DESC LIMIT 12`)).map(r => ({ k: String(r.k), v: r.v }));
  const breakdown = { build: await bd('build'), ver: await bd('ver'), device: await bd('dev'), os: await bd('os'), gpu: await bd('gpu'), quality: await bd('q'), lang: await bd('lang'), country: await bd('country') };
  breakdown.car = (await all(env, "SELECT json_extract(d,'$.car') k, COUNT(*) v FROM events WHERE n='car_pick' AND ts>=? AND json_extract(d,'$.car') IS NOT NULL GROUP BY k ORDER BY v DESC LIMIT 12", st)).map(r => ({ k: String(r.k), v: r.v }));
  const fb = await one(env, "SELECT SUM(f<20) a,SUM(f>=20 AND f<30) b,SUM(f>=30 AND f<45) c,SUM(f>=45 AND f<60) d,SUM(f>=60) e FROM (SELECT fps_w/fps_s f FROM players WHERE fps_s>0)");
  breakdown.fps = [['<20', fb.a], ['20-30', fb.b], ['30-45', fb.c], ['45-60', fb.d], ['60+', fb.e]].map(([k, v]) => ({ k, v: v || 0 }));
  return J({
    ok: true, t, days,
    totals: { installs: tot.n || 0, active_1d: await act(today), active_7d: await act(d7), active_30d: await act(d30), new_today: (await one(env, 'SELECT COUNT(*) c FROM players WHERE first_ts>=?', t0)).c, sessions: tot.s || 0, play_hours: Math.round((tot.p || 0) / 36) / 100, avg_session_min: tot.s ? Math.round((tot.p || 0) / tot.s / 6) / 10 : 0, crashes_7d: (await one(env, `SELECT COUNT(*) c FROM events WHERE n IN ${CRASH} AND ts>=?`, t - 7 * 86400)).c, avg_fps: tot.fs > 0 ? Math.round(tot.w / tot.fs * 10) / 10 : null },
    series, retention: ret, breakdown,
  });
}
async function game(env, t, days) {
  const st = t - days * 86400, r1 = x => (x == null ? null : Math.round(x * 100) / 100);
  const picks = await all(env, "SELECT json_extract(d,'$.car') car,COUNT(*) picks FROM events WHERE n='car_pick' AND ts>=? GROUP BY car", st);
  const races = await all(env, "SELECT json_extract(d,'$.car') car,COUNT(*) races,SUM(json_extract(d,'$.pos')=1) wins,AVG(json_extract(d,'$.pos')) avg_pos FROM events WHERE n='race_end' AND ts>=? GROUP BY car", st);
  const cm = new Map();
  for (const r of picks) if (r.car) cm.set(r.car, { car: r.car, picks: r.picks, races: 0, wins: 0, avg_pos: null });
  for (const r of races) if (r.car) { const o = cm.get(r.car) || { car: r.car, picks: 0 }; cm.set(r.car, { ...o, races: r.races, wins: r.wins || 0, avg_pos: r1(r.avg_pos) }); }
  const tracks = (await all(env, "SELECT json_extract(d,'$.track') track,COUNT(*) races,AVG(json_extract(d,'$.time')) avg_time,MIN(json_extract(d,'$.time')) best_time FROM events WHERE n='race_end' AND ts>=? AND json_extract(d,'$.track') IS NOT NULL GROUP BY track ORDER BY races DESC LIMIT 50", st)).map(r => ({ track: String(r.track), races: r.races, avg_time: r1(r.avg_time), best_time: r1(r.best_time) }));
  const ec = await one(env, 'SELECT SUM(coins) s,AVG(coins) a,COUNT(coins) n FROM players WHERE coins IS NOT NULL');
  const med = ec.n ? (await one(env, 'SELECT coins FROM players WHERE coins IS NOT NULL ORDER BY coins LIMIT 1 OFFSET ?', Math.floor(ec.n / 2))).coins : null;
  const pur = await all(env, "SELECT json_extract(d,'$.car') car,COUNT(*) n FROM events WHERE n='purchase' AND ts>=? GROUP BY car ORDER BY n DESC LIMIT 10", st);
  const pf = await all(env, 'SELECT q,COUNT(*) n,SUM(fps_w)/SUM(fps_s) f FROM players WHERE fps_s>0 AND q IS NOT NULL GROUP BY q ORDER BY q');
  const p1 = new Map((await all(env, "SELECT json_extract(d,'$.q') q,AVG(json_extract(d,'$.p1_fps')) p FROM events WHERE n='session' AND ts>=? GROUP BY q", st)).map(r => [r.q, r.p]));
  const dv = await all(env, 'SELECT dev,COUNT(*) n,SUM(fps_w)/NULLIF(SUM(fps_s),0) f,AVG(q) qa FROM players WHERE dev IS NOT NULL GROUP BY dev ORDER BY n DESC LIMIT 20');
  const fn = await one(env, 'SELECT COUNT(*) a,SUM(races>=1) b,SUM(races>=5) c FROM players');
  return J({
    ok: true, days,
    cars: [...cm.values()].sort((a, b) => b.picks - a.picks || b.races - a.races), tracks,
    economy: { coins_sum: ec.s || 0, coins_avg: r1(ec.a) || 0, coins_p50: med || 0, purchases: pur.reduce((s, r) => s + r.n, 0), top_purchases: pur.map(r => ({ car: r.car, n: r.n })) },
    perf: pf.map(r => ({ q: r.q, n: r.n, avg_fps: r1(r.f), p1_fps: r1(p1.get(r.q)) })),
    devices: dv.map(r => ({ dev: r.dev, n: r.n, avg_fps: r1(r.f), q_avg: r1(r.qa) })),
    funnel: { installs: fn.a || 0, first_race: fn.b || 0, five_races: fn.c || 0 },
  });
}
async function ads(env, t, days) {
  const start = dayOf(t - (days - 1) * 86400), rows = await all(env, 'SELECT * FROM ad_daily WHERE day>=?', start);
  const ctr = (c, i) => (i > 0 ? Math.round(c / i * 10000) / 10000 : 0);
  const add = (m, k, r) => { const o = m.get(k) || { imp: 0, click: 0, close: 0, reward: 0 }; o.imp += r.imp; o.click += r.click; o.close += r.close; o.reward += r.reward; m.set(k, o); };
  const A = new Map(), PL = new Map(), D = new Map(), C = new Map(), AS = {};
  const tot = { imp: 0, click: 0, close: 0, reward: 0 };
  for (const r of rows) {
    add(A, r.ad, r); add(PL, r.place, r); add(D, r.day, r); add(C, r.country || '??', r);
    tot.imp += r.imp; tot.click += r.click; tot.close += r.close; tot.reward += r.reward;
    const s = AS[r.ad] || (AS[r.ad] = new Map()); add(s, r.day, r);
  }
  const dl = []; for (let i = days - 1; i >= 0; i--) dl.push(dayOf(t - i * 86400));
  const ser = m => dl.map(d => ({ day: d, imp: (m.get(d) || {}).imp || 0, click: (m.get(d) || {}).click || 0 }));
  const lst = (m, key) => [...m.entries()].map(([k, v]) => ({ [key]: k, ...v, ctr: ctr(v.click, v.imp) })).sort((a, b) => b.imp - a.imp);
  const by_ad_series = {}; for (const k in AS) by_ad_series[k] = ser(AS[k]);
  return J({ ok: true, days, totals: { ...tot, ctr: ctr(tot.click, tot.imp) }, by_ad: lst(A, 'ad'), by_place: lst(PL, 'place'), series: ser(D), by_country: lst(C, 'k').map(r => ({ k: r.k, imp: r.imp, click: r.click })), by_ad_series });
}
