/**
 * Towel Freshness Tracker — Cloudflare Worker (маг-ссылка + JWT-вход + пакетное обновление по комнате)
 *
 * Эндпоинты:
 *  - GET  /health                — проверка живости
 *  - POST /tg/webhook            — Telegram апдейты (c секретом)
 *  - GET  /__cron                — ручной запуск напоминаний (для тестов)
 *  - GET  /login?token=...       — вход по маг-ссылке (ставит cookie "sid")
 *  - GET  /dashboard             — защищённая панель (cookie обязателен)
 *  - GET  /api/slots             — список слотов владельца (по cookie)
 *  - POST /api/slots             — создать слот { name, room?, threshold_days? } (владелец = из cookie)
 *  - POST /api/slots/:id/refresh — обновить один слот
 *  - POST /api/rooms/refresh     — пакетно «Обновил» все слоты в комнате { room }
 *  - GET  /diag                  — диагностика (публично)
 *
 * Крон: scheduled() раз в час → проверяет локальный час пользователя и шлёт напоминания.
 *
 * Секреты/переменные (wrangler secret put):
 *  TELEGRAM_TOKEN
 *  TELEGRAM_WEBHOOK_SECRET
 *  GOOGLE_CLIENT_EMAIL
 *  GOOGLE_PRIVATE_KEY
 *  SPREADSHEET_ID
 *  WEB_JWT_SECRET                — секрет для HS256 (любой длинный случайный)
 *  WORKER_URL                    — базовый URL воркера, напр. https://towel-tracker.<acc>.workers.dev
 *  (опц.) DEFAULT_TZ, DEFAULT_NOTIFY_HOUR
 */

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);

    // health
    if (url.pathname === "/health") return new Response("healthy", { status: 200 });

    try {
      // Telegram webhook
      if (path === '/tg/webhook' && req.method === 'POST') {
  const sec = req.headers.get('X-Telegram-Bot-Api-Secret-Token');
  if (!sec || sec !== env.TELEGRAM_WEBHOOK_SECRET) return new Response('forbidden', { status: 403 });

  let update = null;
  try { update = await req.json(); } catch {}
   // Обработку делаем асинхронно, а HTTP-ответ — сразу 200
  ctx.waitUntil(safeHandle(update, env));
  return json({ ok: true }); // <-- Telegram всегда видит 200 OK
}
async function safeHandle(update, env) {
  try {
    await handleTelegramUpdate(update, env);
  } catch (e) {
    console.error('[tg webhook] handler error:', e);
    // Пытаемся культурно уведомить пользователя, но без паники
    try {
      const chatId = extractChatId(update);
      if (chatId) await tgSend(env, chatId, 'Техническая заминка. Уже чищу перья и вернусь 🙏');
    } catch (e2) {
      console.error('notify failed', e2);
    }
  }
}

function extractChatId(update) {
  return update?.message?.chat?.id ?? update?.callback_query?.message?.chat?.id ?? null;
}

      // Ручной вызов крон-логики
      if (url.pathname === "/__cron") {
        await runHourlyReminders(env);
        return json({ ok: true, cron: true });
      }

      // Маг-линк → ставим cookie и редиректим в /dashboard
      if (url.pathname === "/login" && req.method === "GET") {
        const token = url.searchParams.get("token");
        if (!token) return new Response("token required", { status: 400 });
        const magic = await jwtVerifyHS256(token, env.WEB_JWT_SECRET);
        if (!magic || !magic.sub) return new Response("invalid token", { status: 401 });
        // создаём сессионный токен на 7 дней
        const session = await jwtSignHS256({ sub: String(magic.sub) }, env.WEB_JWT_SECRET, 60 * 60 * 24 * 7);
        const res = new Response(null, { status: 302, headers: { Location: "/dashboard" } });
        setCookie(res, "sid", session, { httpOnly: true, secure: true, sameSite: "Lax", maxAge: 60 * 60 * 24 * 7, path: "/" });
        return res;
      }

      // Защищённые маршруты: требуем сессию
      const uid = await parseSession(req, env); // null если нет cookie/некорректно

      if ((url.pathname === "/" || url.pathname === "/dashboard") && req.method === "GET") {
        if (!uid) return needAuthPage(env);
        const html = await renderDashboard(env, uid);
        return htmlResponse(html);
      }

      if (url.pathname === "/api/slots" && req.method === "GET") {
        if (!uid) return new Response("unauthorized", { status: 401 });
        const slots = await listSlots(env, uid);
        return json(slots);
      }

      if (url.pathname === "/api/slots" && req.method === "POST") {
        if (!uid) return new Response("unauthorized", { status: 401 });
        const body = await readBody(req);
        const name = (body.name || "").trim();
        if (!name) return new Response("name required", { status: 400 });
        const threshold_days = body.threshold_days != null ? Number(body.threshold_days) : 3;
        const room = (body.room || "").trim();
        const slot = await createSlot(env, { name, owner_tg_id: uid, room, threshold_days });
        return json(slot);
      }

      if (url.pathname.startsWith("/api/slots/") && req.method === "DELETE") {
        if (!uid) return new Response("unauthorized", { status: 401 });
        const id = url.pathname.split("/")[3];
        try {
          await deleteSlot(env, id, { actor: String(uid) });
        } catch (e) {
          if (e && e.message === 'forbidden') return new Response("forbidden", { status: 403 });
          throw e;
        }
        return json({ ok: true });
      }

      if (url.pathname.startsWith("/api/slots/") && url.pathname.endsWith("/refresh") && req.method === "POST") {
        if (!uid) return new Response("unauthorized", { status: 401 });
        const id = url.pathname.split("/")[3];
        let res;
        try {
          res = await refreshSlot(env, id, { actor: String(uid) });
        } catch (e) {
          if (e && e.message === 'forbidden') return new Response("forbidden", { status: 403 });
          throw e;
        }
        return json(res);
      }

      if (url.pathname === "/api/rooms/refresh" && req.method === "POST") {
        if (!uid) return new Response("unauthorized", { status: 401 });
        const body = await readBody(req);
        const room = (body.room || "").trim();
        if (!room) return new Response("room required", { status: 400 });
        const out = await refreshByRoom(env, uid, room);
        return json(out);
      }

      // Диагностика публично
      if (url.pathname === "/diag") {
        const report = await runDiag(env);
        return htmlResponse(report);
      }

      return new Response("Not found", { status: 404 });
    } catch (e) {
      console.error("[fetch] error:", e);
      return new Response("Internal error", { status: 500 });
    }
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(runHourlyReminders(env));
  },
};

/* =========================
 * Telegram: апдейты и UI
 * ========================= */
async function handleTelegramUpdate(update, env) {
  if (update.message) return onMessage(update.message, env);
  if (update.callback_query) return onCallback(update.callback_query, env);
}

async function onMessage(msg, env) {
  const chatId = msg.chat.id;
  const text = (msg.text || "").trim();

  try { await ensureUser(env, chatId); } catch (e) { console.error("ensureUser failed", e); }

  if (text.startsWith("/start")) {
    const link = await magicLink(env, chatId, 45 * 60); // 15 минут на вход
    const buttons = [
      [ { text: "➕ Добавить слот", callback_data: "ui:add" }, { text: "📋 Список", callback_data: "ui:list" } ],
      [ { text: "⏰ Настроить время", callback_data: "ui:settings" } ],
      ...(link ? [ [ { text: "🌐 Веб-панель (вход)", url: link } ] ] : []),
    ];
    await tgSend(env, chatId,
      "Привет! Я слежу за свежестью слотов полотенец.\n\n"+
      "— Создай слот: <code>/add Название | Комната | Дни</code>\n"+
      (link ? "— Открой панель по кнопке ниже (маг-ссылка действует 45 минут)." : "— Админ: установите WORKER_URL, чтобы появилась кнопка входа."),
      buttons
    );
    return;
  }

  if (text.startsWith("/add")) {
    const m = text.match(/^\/add\s+(.+?)\s*\|\s*(.+?)\s*\|\s*(\d{1,3})$/);
    if (!m) {
      await tgSend(env, chatId, "Формат:\n<code>/add Для рук | Ванная | 3</code>");
      return;
    }
    const name = m[1].trim();
    const room = m[2].trim();
    const threshold_days = parseInt(m[3], 10);
    const slot = await createSlot(env, { name, owner_tg_id: chatId, room, threshold_days });
    await tgSend(env, chatId, `Слот «${escapeHtml(slot.name)}» (${escapeHtml(slot.room||'—')}) создан. Порог: ${slot.threshold_days} дн.`);
    return;
  }

  if (text.startsWith("/list")) return sendList(env, chatId);

  if (text.startsWith("/sethour")) {
    const m = text.match(/^\/sethour\s+(\d{1,2})$/);
    if (!m) return tgSend(env, chatId, "Формат: <code>/sethour 10</code>");
    const hour = clampInt(parseInt(m[1], 10), 0, 23);
    await upsertUser(env, { tg_user_id: chatId, notify_hour: hour });
    return tgSend(env, chatId, `Буду писать в ${pad2(hour)}:00.`);
  }

  if (text.startsWith("/settz")) {
    const tz = text.replace("/settz", "").trim();
    if (!tz) return tgSend(env, chatId, "Формат: <code>/settz Europe/Moscow</code>");
    await upsertUser(env, { tg_user_id: chatId, tz });
    return tgSend(env, chatId, `Часовой пояс обновлён: ${escapeHtml(tz)}`);
  }

  if (text === "📋 Список" || text === "Список") return sendList(env, chatId);
}

async function onCallback(cb, env) {
  const chatId = cb.message.chat.id;
  const data = cb.data || "";

  if (data === "ui:add") {
    await tgSend(env, chatId, "Создай слот командой:\n<code>/add Название | Комната | Дни</code>\nНапример: <code>/add Для рук | Ванная | 3</code>");
    return tgAnswer(env, cb.id, "Жду /add");
  }
  if (data === "ui:list") { await sendList(env, chatId); return tgAnswer(env, cb.id); }
  if (data === "ui:settings") { await tgSend(env, chatId, "Время: <code>/sethour 10</code>\nПояс: <code>/settz Europe/Moscow</code>"); return tgAnswer(env, cb.id); }

  if (data === "ui:dashboard") {
    const link = await magicLink(env, chatId, 45 * 60);
    if (link) await tgSend(env, chatId, `Вход в веб-панель: ${link}\n(Ссылка действует 45 минут)`);
    else await tgSend(env, chatId, `У администратора не задан WORKER_URL — кнопка входа недоступна.`);
    return tgAnswer(env, cb.id);
  }

  if (data.startsWith("refresh:")) {
    const id = data.split(":")[1];
    try {
      await refreshSlot(env, id, { actor: String(chatId) });
      await tgAnswer(env, cb.id, "Обновлено");
      return sendList(env, chatId);
    } catch (e) {
      console.error("refreshSlot failed", e);
      return tgAnswer(env, cb.id, "Нет доступа", true);
    }
  }
  if (data.startsWith("del:")) {
    const id = data.split(":")[1];
    try {
      await deleteSlot(env, id, { actor: String(chatId) });
      await tgAnswer(env, cb.id, "Удалено");
      return sendList(env, chatId);
    } catch (e) {
      console.error("deleteSlot failed", e);
      return tgAnswer(env, cb.id, "Не удалось удалить", true);
    }
  }
  if (data.startsWith("setth:")) {
    const [_, id, days] = data.split(":");
    try {
      await updateSlot(env, id, { threshold_days: parseInt(days, 10) }, { actor: String(chatId) });
      await tgAnswer(env, cb.id, `Порог: ${days} дн`);
      return sendList(env, chatId);
    } catch (e) {
      console.error("updateSlot failed", e);
      return tgAnswer(env, cb.id, "Нет доступа", true);
    }
  }
}

async function sendList(env, chatId) {
  let slots = [];
  try { slots = await listSlots(env, chatId); }
  catch (e) {
    console.error("listSlots failed", e);
    return tgSend(env, chatId, "Не удалось прочитать таблицу. Дай сервисному аккаунту доступ Редактора к Google Sheets.");
  }
  if (!slots.length) return tgSend(env, chatId, "Слотов пока нет. Создай: <code>/add Название | Комната | Дни</code>");

  const lines = slots.sort((a,b)=>a.score-b.score).map(s=>{
    const age = daysSince(s.last_change_at);
    const room = s.room ? ` • ${escapeHtml(s.room)}` : "";
    return `${statusEmoji(s.status)} ${escapeHtml(s.name)}${room} — ${age} дн / порог ${s.threshold_days}`;
  }).join("\n");

  const buttons = [];
  for (const s of slots.slice(0,6)) {
    buttons.push([
      { text: `🔄 ${shorten(s.name,14)}`, callback_data: `refresh:${s.id}` },
      { text: "🗑", callback_data: `del:${s.id}` },
    ]);
    buttons.push([
      { text: "1д", callback_data: `setth:${s.id}:1` },
      { text: "2д", callback_data: `setth:${s.id}:2` },
      { text: "3д", callback_data: `setth:${s.id}:3` },
      { text: "5д", callback_data: `setth:${s.id}:5` },
    ]);
  }

  await tgSend(env, chatId, lines, buttons);
}

/* =========================
 * Напоминания по крону
 * ========================= */
async function runHourlyReminders(env) {
  const users = await listUsers(env);
  const nowUTC = new Date();

  for (const u of users) {
    const tz = u.tz || env.DEFAULT_TZ || "Europe/Moscow";
    const targetHour = u.notify_hour != null ? Number(u.notify_hour) : Number(env.DEFAULT_NOTIFY_HOUR || 10);
    const localHour = hourInTz(nowUTC, tz);
    if (localHour !== targetHour) continue;

    const slots = await listSlots(env, u.tg_user_id);
    const overdue = slots.filter(s => daysSince(s.last_change_at) >= Number(s.threshold_days || 0));
    if (!overdue.length) continue;

    const body = overdue
      .sort((a,b)=>a.score-b.score)
      .map(s=>{
        const age = daysSince(s.last_change_at);
        const room = s.room ? ` (${escapeHtml(s.room)})` : "";
        return `${statusEmoji(s.status)} ${escapeHtml(s.name)}${room} — ${age} дн (порог ${s.threshold_days})`;
      })
      .join("\n");
    const buttons = overdue.slice(0,6).map(s=>[{ text: `🔄 ${shorten(s.name,18)}`, callback_data: `refresh:${s.id}` }]);

    await tgSend(env, u.tg_user_id, `Напоминание:\n${body}`, buttons);
  }
}

/* =========================
 * Авторизация: JWT HS256 + cookie
 * ========================= */
async function magicLink(env, tgUserId, ttlSec) {
  if (!env.WORKER_URL || !env.WEB_JWT_SECRET) return "";
  const token = await jwtSignHS256({ sub: String(tgUserId) }, env.WEB_JWT_SECRET, ttlSec);
  const base = env.WORKER_URL.replace(/\/+$/,'');
  return `${base}/login?token=${token}`;
}

async function parseSession(req, env) {
  const cookies = parseCookies(req.headers.get("cookie") || "");
  const tok = cookies["sid"]; if (!tok) return null;
  const payload = await jwtVerifyHS256(tok, env.WEB_JWT_SECRET);
  if (!payload) return null;
  return Number(payload.sub);
}

async function jwtSignHS256(payload, secret, ttlSec) {
  const header = { alg: "HS256", typ: "JWT" };
  const now = Math.floor(Date.now()/1000);
  const body = { ...payload, iat: now, exp: now + Number(ttlSec || 900) };
  const h = b64url(new TextEncoder().encode(JSON.stringify(header)));
  const p = b64url(new TextEncoder().encode(JSON.stringify(body)));
  const sig = await hmacSign(secret, new TextEncoder().encode(`${h}.${p}`));
  return `${h}.${p}.${sig}`;
}

async function jwtVerifyHS256(token, secret) {
  const parts = String(token).split(".");
  if (parts.length !== 3) return null;
  const [h, p, s] = parts;
  const expected = await hmacSign(secret, new TextEncoder().encode(`${h}.${p}`));
  if (!timingSafeEq(b64ToBytes(s), b64ToBytes(expected))) return null;
  try {
    const payload = JSON.parse(new TextDecoder().decode(b64ToBytes(p)));
    if (!payload || !payload.exp || Math.floor(Date.now()/1000) > Number(payload.exp)) return null;
    return payload;
  } catch { return null; }
}

async function hmacSign(secret, dataBytes) {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(String(secret)),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, dataBytes);
  return b64url(sig);
}

function timingSafeEq(a, b) {
  if (!(a instanceof Uint8Array)) a = new Uint8Array(a);
  if (!(b instanceof Uint8Array)) b = new Uint8Array(b);
  if (a.length !== b.length) return false;
  let diff = 0; for (let i=0;i<a.length;i++) diff |= a[i]^b[i];
  return diff === 0;
}

function setCookie(res, name, value, { httpOnly=true, secure=true, sameSite="Lax", maxAge=0, path="/" }={}) {
  const parts = [`${name}=${value}`];
  if (maxAge>0) parts.push(`Max-Age=${maxAge}`);
  if (path) parts.push(`Path=${path}`);
  if (secure) parts.push("Secure");
  if (httpOnly) parts.push("HttpOnly");
  if (sameSite) parts.push(`SameSite=${sameSite}`);
  res.headers.append("Set-Cookie", parts.join("; "));
}

function parseCookies(str) {
  const out = {}; if (!str) return out;
  str.split(/;\s*/).forEach(p=>{ const i=p.indexOf('='); if(i>0) out[p.slice(0,i)] = decodeURIComponent(p.slice(i+1)); });
  return out;
}

/* =========================
 * Расчёты статуса/оценки
 * ========================= */
function calcStatus(threshold_days, last_change_at) {
  const d = daysSince(last_change_at);
  const load = d / Math.max(1, threshold_days);
  const score = Math.max(0, 100 - load * 100);
  const status = score >= 40 ? "OK" : score >= 20 ? "WARN" : "EXPIRED";
  return { d, score, status };
}

function daysSince(iso) {
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return 0;
  return Math.floor((Date.now() - t) / 86400000);
}

function statusEmoji(status) {
  if (status === "EXPIRED") return "🔴";
  if (status === "WARN") return "🟡";
  return "🟢";
}

function hourInTz(date, tz) {
  try { return Number(new Intl.DateTimeFormat('ru-RU',{hour:'2-digit',hour12:false,timeZone:tz}).format(date)); }
  catch { return date.getUTCHours(); }
}

function pad2(n){ return n.toString().padStart(2,'0'); }
function clampInt(x,min,max){ return Math.max(min, Math.min(max, x|0)); }
function shorten(s,n){ return s.length>n ? s.slice(0,n-1)+'…' : s; }

/* =========================
 * Google Sheets API (JWT) + слой доступа
 * ========================= */
const SLOT_RANGE = 'slots!A2:F';
const SLOT_APPEND_RANGE = 'slots!A:F';
const ACCESS_RANGE = 'access!A2:B';
const ACCESS_APPEND_RANGE = 'access!A:B';
const SLOT_ROOM_COLUMN = 'D';
const SLOT_THRESHOLD_COLUMN = 'E';
const SLOT_LAST_COLUMN = 'F';
const sheetIdCache = new Map();

async function listSlots(env, userFilter /* tg_user_id */, opts = {}) {
  const table = await getSlotsTable(env);
  let filtered = table;
  if (userFilter != null) {
    const groups = await listGroupsForUser(env, userFilter, { ensure: false });
    const groupSet = new Set(groups);
    if (groupSet.size === 0) groupSet.add(`tg:${userFilter}`);
    filtered = table.filter(slot => hasSlotAccess(slot, userFilter, groupSet));
  }
  const includeMeta = Boolean(opts.includeMeta);
  return filtered.map(slot => formatSlotForOutput(slot, includeMeta));
}

function hasSlotAccess(slot, userId, groupSet) {
  if (userId == null) return true;
  if (slot.group_id && groupSet.has(slot.group_id)) return true;
  if (slot.owner_fallback != null && Number(slot.owner_fallback) === Number(userId)) return true;
  return false;
}

function formatSlotForOutput(slot, includeMeta) {
  const metrics = calcStatus(slot.threshold_days, slot.last_change_at);
  const base = {
    id: slot.id,
    name: slot.name,
    group_id: slot.group_id,
    room: slot.room,
    threshold_days: slot.threshold_days,
    last_change_at: slot.last_change_at,
    ...metrics,
  };
  if (includeMeta) base.sheet_row = slot.sheet_row;
  return base;
}

async function createSlot(env, { name, owner_tg_id, room = '', threshold_days = 3 }) {
  const normalizedThreshold = Math.max(1, Number(threshold_days) || 1);
  const id = ulid();
  const now = new Date().toISOString();
  const group_id = owner_tg_id != null ? await getPrimaryGroupId(env, owner_tg_id) : '';
  const normalizedRoom = (room || '').trim();
  await sheetsAppend(env, SLOT_APPEND_RANGE, [[id, name, group_id, normalizedRoom, String(normalizedThreshold), now]]);
  await logEvent(env, { slot_id: id, action: 'CREATE', actor: String(owner_tg_id||''), note: name });
  const metrics = calcStatus(normalizedThreshold, now);
  return { id, name, group_id, room: normalizedRoom, threshold_days: normalizedThreshold, last_change_at: now, ...metrics };
}

async function refreshSlot(env, id, { actor = '' } = {}) {
  const slot = await getSlotById(env, id);
  if (!slot) throw new Error('slot not found');
  if (actor != null && actor !== '') {
    const groups = await listGroupsForUser(env, actor, { ensure: false });
    const groupSet = new Set(groups);
    if (groupSet.size === 0) groupSet.add(`tg:${actor}`);
    if (!hasSlotAccess(slot, actor, groupSet)) throw new Error('forbidden');
  }
  const now = new Date().toISOString();
  await sheetsUpdate(env, [ { range: `slots!${SLOT_LAST_COLUMN}${slot.sheet_row}:${SLOT_LAST_COLUMN}${slot.sheet_row}`, values: [[now]] } ]);
  await logEvent(env, { slot_id: id, action: 'REFRESH', actor: String(actor||''), note: '' });
  return { ok: true };
}

async function refreshByRoom(env, actorId, room) {
  const all = await listSlots(env, actorId, { includeMeta: true });
  const targets = all.filter(s => (s.room||'') === room);
  if (!targets.length) return { updated: 0 };
  const now = new Date().toISOString();
  const updates = targets.map(t => ({ range: `slots!${SLOT_LAST_COLUMN}${t.sheet_row}:${SLOT_LAST_COLUMN}${t.sheet_row}`, values: [[now]] }));
  if (updates.length) await sheetsUpdate(env, updates);
  for (const t of targets) await logEvent(env, { slot_id: t.id, action: 'REFRESH', actor: String(actorId), note: `room:${room}` });
  return { updated: targets.length };
}

async function updateSlot(env, id, patch = {}, { actor = '' } = {}) {
  const slot = await getSlotById(env, id);
  if (!slot) throw new Error('slot not found');
  if (actor != null && actor !== '') {
    const groups = await listGroupsForUser(env, actor, { ensure: false });
    const groupSet = new Set(groups);
    if (groupSet.size === 0) groupSet.add(`tg:${actor}`);
    if (!hasSlotAccess(slot, actor, groupSet)) throw new Error('forbidden');
  }
  const updates = [];
  if (patch.room != null) updates.push({ range: `slots!${SLOT_ROOM_COLUMN}${slot.sheet_row}:${SLOT_ROOM_COLUMN}${slot.sheet_row}`, values: [[String(patch.room || '')]] });
  if (patch.threshold_days != null) {
    const val = Math.max(1, Number(patch.threshold_days) || 1);
    updates.push({ range: `slots!${SLOT_THRESHOLD_COLUMN}${slot.sheet_row}:${SLOT_THRESHOLD_COLUMN}${slot.sheet_row}`, values: [[String(val)]] });
  }
  if (updates.length) {
    await sheetsUpdate(env, updates);
    await logEvent(env, { slot_id: id, action: 'UPDATE', actor: String(actor||''), note: JSON.stringify(patch) });
  }
  return { ok: true };
}

async function deleteSlot(env, id, { actor = '' } = {}) {
  const slot = await getSlotById(env, id);
  if (!slot) throw new Error('slot not found');
  if (actor != null && actor !== '') {
    const groups = await listGroupsForUser(env, actor, { ensure: false });
    const groupSet = new Set(groups);
    if (groupSet.size === 0) groupSet.add(`tg:${actor}`);
    if (!hasSlotAccess(slot, actor, groupSet)) throw new Error('forbidden');
  }
  await sheetsDeleteRow(env, 'slots', slot.sheet_row);
  await logEvent(env, { slot_id: id, action: 'DELETE', actor: String(actor||''), note: slot.name || '' });
  return { ok: true };
}

async function ensureUser(env, tg_user_id) {
  const rows = await sheetsGet(env, 'users!A2:C');
  const exists = (rows||[]).some(r => r[0] === String(tg_user_id));
  if (!exists) await sheetsAppend(env, 'users!A:C', [[String(tg_user_id), env.DEFAULT_TZ || 'Europe/Moscow', String(env.DEFAULT_NOTIFY_HOUR || 10)]]);
  await listGroupsForUser(env, tg_user_id, { ensure: true });
}

async function upsertUser(env, { tg_user_id, tz, notify_hour }) {
  const { rowIndex } = await findRowById(env, 'users', String(tg_user_id));
  if (!rowIndex) {
    await sheetsAppend(env, 'users!A:C', [[String(tg_user_id), tz || (env.DEFAULT_TZ || 'Europe/Moscow'), String(notify_hour != null ? notify_hour : (env.DEFAULT_NOTIFY_HOUR || 10))]]);
  } else {
    const updates = [];
    if (tz != null) updates.push({ range: `users!B${rowIndex}:B${rowIndex}`, values: [[tz]] });
    if (notify_hour != null) updates.push({ range: `users!C${rowIndex}:C${rowIndex}`, values: [[String(notify_hour)]] });
    if (updates.length) await sheetsUpdate(env, updates);
  }
  await listGroupsForUser(env, tg_user_id, { ensure: true });
}

async function listUsers(env) {
  const rows = await sheetsGet(env, 'users!A2:C');
  return (rows || []).map(r => ({ tg_user_id: Number(r[0]), tz: r[1] || 'Europe/Moscow', notify_hour: r[2] ? Number(r[2]) : 10 }));
}

async function logEvent(env, { slot_id, action, actor, note }) {
  const ts = new Date().toISOString();
  await sheetsAppend(env, 'events!A:E', [[ts, slot_id, action, actor, note || '']]);
}

async function getPrimaryGroupId(env, tg_user_id) {
  const groups = await listGroupsForUser(env, tg_user_id, { ensure: true });
  return groups[0] || `tg:${tg_user_id}`;
}

async function listGroupsForUser(env, tg_user_id, { ensure = false } = {}) {
  if (tg_user_id == null) return [];
  const desired = String(tg_user_id);
  const rows = await sheetsGet(env, ACCESS_RANGE);
  const matches = (rows || []).filter(r => (r[1] || '') === desired).map(r => r[0]).filter(Boolean);
  if (matches.length || !ensure) return matches;
  const group_id = `tg:${desired}`;
  await sheetsAppend(env, ACCESS_APPEND_RANGE, [[group_id, desired]]);
  return [group_id];
}

async function getSlotsTable(env) {
  const rows = await sheetsGet(env, SLOT_RANGE);
  const table = [];
  const fixes = [];
  for (let i = 0; i < (rows || []).length; i++) {
    const rowNumber = i + 2;
    const row = rows[i] ? [...rows[i]] : [];
    const currentId = row[0] ? String(row[0]).trim() : '';
    if (!currentId && row[1] && String(row[1]).trim()) {
      const newId = ulid();
      row[0] = newId;
      fixes.push({ range: `slots!A${rowNumber}:A${rowNumber}`, values: [[newId]] });
    }
    const slot = parseSlotRow(row, rowNumber);
    if (slot) table.push(slot);
  }
  if (fixes.length) await sheetsUpdate(env, fixes);
  return table;
}

async function getSlotById(env, id) {
  const table = await getSlotsTable(env);
  return table.find(s => s.id === id) || null;
}

function parseSlotRow(row, rowNumber) {
  const id = (row && row[0] ? String(row[0]).trim() : '');
  if (!id) return null;
  const name = row[1] ? String(row[1]).trim() : '';
  let group_id = row[2] ? String(row[2]).trim() : '';
  let owner_fallback = null;
  if (group_id && /^\d+$/.test(group_id)) {
    owner_fallback = Number(group_id);
    group_id = `tg:${group_id}`;
  }
  const room = row[3] ? String(row[3]).trim() : '';
  const threshold_days = row[4] != null && row[4] !== '' ? Number(row[4]) : 3;
  const last_change_at = row[5] ? String(row[5]) : new Date().toISOString();
  return { id, name, group_id, room, threshold_days, last_change_at, sheet_row: rowNumber, owner_fallback };
}

async function sheetsDeleteRow(env, sheetTitle, rowIndex) {
  const token = await getAccessToken(env);
  const sheetId = await getSheetId(env, sheetTitle, token);
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${env.SPREADSHEET_ID}:batchUpdate`;
  const body = { requests: [ { deleteDimension: { range: { sheetId, dimension: 'ROWS', startIndex: rowIndex - 1, endIndex: rowIndex } } } ] };
  const resp = await fetch(url, { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body: JSON.stringify(body) });
  if (!resp.ok) throw new Error('sheets delete row error: ' + resp.status);
}

async function getSheetId(env, title, token) {
  if (sheetIdCache.has(title)) return sheetIdCache.get(title);
  const auth = token || await getAccessToken(env);
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${env.SPREADSHEET_ID}?fields=sheets.properties`;
  const resp = await fetch(url, { headers: { Authorization: `Bearer ${auth}` } });
  if (!resp.ok) throw new Error('sheets metadata error: ' + resp.status);
  const data = await resp.json();
  const sheet = (data.sheets || []).map(s => s.properties).find(p => p.title === title);
  if (!sheet) throw new Error('sheet not found: ' + title);
  sheetIdCache.set(title, sheet.sheetId);
  return sheet.sheetId;
}

async function findRowById(env, sheetName, id) {
  const rows = await sheetsGet(env, `${sheetName}!A2:A`);
  let idx = 0; for (const r of (rows||[])) { idx++; if ((r[0]||'') === String(id)) return { rowIndex: idx + 1 }; }
  return { rowIndex: null };
}

/* =========================
 * Google Sheets: OAuth по JWT
 * ========================= */
async function getAccessToken(env) {
  const iss = env.GOOGLE_CLIENT_EMAIL; if (!iss) throw new Error('GOOGLE_CLIENT_EMAIL не задан');
  if (!env.GOOGLE_PRIVATE_KEY) throw new Error('GOOGLE_PRIVATE_KEY не задан');

  // Нормализуем ключ: поддержка \n, реальных переводов, «голого» base64 (без BEGIN/END)
  let pkRaw = String(env.GOOGLE_PRIVATE_KEY);
  if (pkRaw.includes('\\n')) pkRaw = pkRaw.replace(/\\n/g, '\n').trim();

  let pk;
  if (pkRaw.includes('BEGIN PRIVATE KEY')) {
    pk = pkRaw.trim();
  } else {
    const body = pkRaw.replace(/-----(BEGIN|END)[\s\S]*?-----/g,'').replace(/\s+/g,'');
    if (!/^[a-zA-Z0-9+/=_-]+$/.test(body) || body.length < 100) {
      throw new Error('GOOGLE_PRIVATE_KEY не похож на PKCS8. Вставь значение поля private_key из JSON.');
    }
    pk = `-----BEGIN PRIVATE KEY-----\n${(body.match(/.{1,64}/g)||[body]).join('\n')}\n-----END PRIVATE KEY-----`;
  }

  const now = Math.floor(Date.now()/1000);
  const header = { alg: 'RS256', typ: 'JWT' };
  const claim  = { iss, scope: 'https://www.googleapis.com/auth/spreadsheets', aud: 'https://oauth2.googleapis.com/token', exp: now + 3600, iat: now };

  const encHeader = b64url(JSON.stringify(header));
  const encClaim  = b64url(JSON.stringify(claim));
  const signingInput = `${encHeader}.${encClaim}`;

  const key = await importPKCS8(pk, 'RSASSA-PKCS1-v1_5');
  const signature = await crypto.subtle.sign({ name: 'RSASSA-PKCS1-v1_5' }, key, new TextEncoder().encode(signingInput));
  const jwt = `${signingInput}.${b64url(signature)}`;

  const resp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: jwt })
  });
  if (!resp.ok) { const txt = await resp.text().catch(()=>String(resp.status)); throw new Error('oauth token error: '+resp.status+' '+txt); }
  const data = await resp.json();
  return data.access_token;
}

async function importPKCS8(pem, algName) {
  let body = pem;
  if (pem.includes('BEGIN')) body = pem.replace(/-----BEGIN [\s\S]*?-----/g,'').replace(/-----END [\s\S]*?-----/g,'').replace(/\r?\n|\r/g,'').trim();
  if (!/^[a-zA-Z0-9+/=_-]+$/.test(body) || body.length < 100) throw new Error('Invalid PKCS8 body');
  const raw = Uint8Array.from(atob(body), c => c.charCodeAt(0));
  return crypto.subtle.importKey('pkcs8', raw, { name: algName, hash: 'SHA-256' }, false, ['sign']);
}

// вспомогательный base64url от строки/ArrayBuffer
function b64url(input) {
  const bytes = input instanceof ArrayBuffer ? new Uint8Array(input) : new TextEncoder().encode(String(input));
  let bin = ''; for (let i=0;i<bytes.length;i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/g,'');
}

  const resp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: jwt })
  });
  if (!resp.ok) { const txt = await resp.text().catch(()=>String(resp.status)); throw new Error('oauth token error: '+resp.status+' '+txt); }
  const data = await resp.json();
  return data.access_token;
}

async function sheetsGet(env, rangeA1) {
  if (!env.SPREADSHEET_ID) throw new Error('SPREADSHEET_ID не задан');
  const token = await getAccessToken(env);
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${env.SPREADSHEET_ID}/values/${encodeURIComponent(rangeA1)}?majorDimension=ROWS`;
  const resp = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  if (!resp.ok) throw new Error('sheets get error: ' + resp.status);
  const data = await resp.json();
  return data.values || [];
}

async function sheetsAppend(env, rangeA1, values) {
  const token = await getAccessToken(env);
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${env.SPREADSHEET_ID}/values/${encodeURIComponent(rangeA1)}:append?valueInputOption=RAW`;
  const resp = await fetch(url, { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body: JSON.stringify({ range: rangeA1, majorDimension: 'ROWS', values }) });
  if (!resp.ok) throw new Error('sheets append error: ' + resp.status);
}

async function sheetsUpdate(env, updates) {
  const token = await getAccessToken(env);
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${env.SPREADSHEET_ID}/values:batchUpdate`;
  const resp = await fetch(url, { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body: JSON.stringify({ valueInputOption: 'RAW', data: updates }) });
  if (!resp.ok) throw new Error('sheets update error: ' + resp.status);
}

/* =========================
 * Мини-дашборд (HTML)
 * ========================= */
async function renderDashboard(env, uid) {
  const slots = await listSlots(env, uid ? Number(uid) : null);
  const rooms = Array.from(new Set(slots.map(s=>s.room).filter(Boolean)));
  const rows = slots.map(s=>{
    const age = daysSince(s.last_change_at);
    return `<tr>
      <td>${statusEmoji(s.status)}</td>
      <td>${escapeHtml(s.name)}</td>
      <td>${escapeHtml(s.room||'—')}</td>
      <td>${age}</td>
      <td>${s.threshold_days}</td>
      <td>${Math.round(s.score)}%</td>
      <td><form method="post" action="/api/slots/${s.id}/refresh"><button>Обновил</button></form></td>
    </tr>`;
  }).join("");

  const roomBtns = rooms.map(r=>`<form method="post" action="/api/rooms/refresh" style="display:inline-block;margin:0 8px 8px 0"><input type="hidden" name="room" value="${escapeHtml(r)}"><button>Обновить: ${escapeHtml(r)}</button></form>`).join("");

  return `<!doctype html><html lang="ru"><meta charset="utf-8"/><title>Towel Tracker</title>
  <style>:root{color-scheme:dark}body{font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;padding:20px;background:#0b0b0b;color:#fafafa}
  table{width:100%;border-collapse:collapse;margin-top:12px}th,td{border-bottom:1px solid #333;padding:8px}th{color:#bbb;text-align:left}
  button{background:#1f6feb;border:0;color:#fff;padding:6px 10px;border-radius:8px;cursor:pointer}button:hover{opacity:.9}
  .tip{color:#aaa}.bar{margin:12px 0}
  form.inline{display:inline-flex;gap:8px;align-items:center}
  input[type=text]{background:#111;border:1px solid #333;color:#fff;border-radius:8px;padding:6px 8px}
  </style>
  <h1>Свежесть слотов полотенец</h1>
  <div class="bar">
    <form class="inline" method="post" action="/api/rooms/refresh">
      <label>Быстро обновить комнату:&nbsp;</label>
      <input name="room" type="text" placeholder="например: Ванная"/>
      <button>Обновить все</button>
    </form>
  </div>
  <div class="bar">${roomBtns}</div>
  <table><thead><tr><th>Статус</th><th>Слот</th><th>Комната</th><th>Возраст, дн</th><th>Порог, дн</th><th>Оценка</th><th>Действие</th></tr></thead><tbody>${rows}</tbody></table>
  </html>`;
}

function needAuthPage(env){
  const hint = env.WORKER_URL? `Открой <code>/start</code> у бота и нажми «Веб-панель (вход)».` : `Админ: установите секрет <code>WORKER_URL</code> и переотправьте /start в боте`;
  return htmlResponse(`<!doctype html><meta charset="utf-8"><title>Нужен вход</title><style>body{font-family:system-ui;padding:24px;background:#0b0b0b;color:#fafafa}</style><h1>Требуется вход</h1><p>${hint}</p>` ,401);
}

function escapeHtml(s=''){return s.replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;"}[c]));}
function htmlResponse(html, status=200){return new Response(html,{status,headers:{'content-type':'text/html; charset=UTF-8'}})}
function json(obj){return new Response(JSON.stringify(obj),{headers:{'content-type':'application/json; charset=UTF-8'}})}

/* =========================
 * Крипто/утилиты
 * ========================= */
function b64url(bufOrBytes){ const bytes = bufOrBytes instanceof ArrayBuffer ? new Uint8Array(bufOrBytes) : new Uint8Array(bufOrBytes); let binary=''; for(let i=0;i<bytes.length;i++) binary+=String.fromCharCode(bytes[i]); let base64=btoa(binary); return base64.replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/g,''); }
function b64ToBytes(b64){ b64=b64.replace(/-/g,'+').replace(/_/g,'/'); const pad = b64.length%4===2?'==':b64.length%4===3?'=':''; const s=b64+pad; const bin=atob(s); const out=new Uint8Array(bin.length); for(let i=0;i<bin.length;i++) out[i]=bin.charCodeAt(i); return out; }
function chunk64(s){ return s.match(/.{1,64}/g)?.join('\n') || s; }

async function importPKCS8(pem, algName){
  let body=pem;
  if (pem.includes('BEGIN')) body=pem.replace(/-----BEGIN [\s\S]*?-----/g,'').replace(/-----END [\s\S]*?-----/g,'').replace(/\r?\n|\r/g,'').trim();
  if (!/^[a-zA-Z0-9+/=_-]+$/.test(body) || body.length<100) throw new Error('Invalid PKCS8 body');
  const raw=Uint8Array.from(atob(body),c=>c.charCodeAt(0));
  return crypto.subtle.importKey('pkcs8', raw, { name: algName, hash: 'SHA-256' }, false, ['sign']);
}

function ulid(){ const now=Date.now().toString(36); const rand=crypto.getRandomValues(new Uint8Array(16)); return (now+Array.from(rand).map(b=>b.toString(36).padStart(2,'0')).join('')).slice(0,26); }

/* =========================
 * Telegram API
 * ========================= */
async function tgSend(env, chat_id, text, keyboard = [], opts = {}) {
  const payload = { chat_id, text, parse_mode: opts.parse_mode || 'HTML', reply_markup: keyboard.length ? { inline_keyboard: keyboard } : undefined, disable_web_page_preview: true };
  await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/sendMessage`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload) });
}
async function tgAnswer(env, cbQueryId, text = '', showAlert = false) {
  await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/answerCallbackQuery`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ callback_query_id: cbQueryId, text, show_alert: showAlert }) });
}

/* =========================
 * Диагностика
 * ========================= */
async function runDiag(env){
  const lines=[]; const ok=(k,v,e='')=>`<tr><td>${escapeHtml(k)}</td><td>${v?'✓':'✗'}</td><td>${escapeHtml(e)}</td></tr>`;
  lines.push(ok('SPREADSHEET_ID', !!env.SPREADSHEET_ID));
  lines.push(ok('GOOGLE_CLIENT_EMAIL', !!env.GOOGLE_CLIENT_EMAIL, env.GOOGLE_CLIENT_EMAIL||''));
  lines.push(ok('GOOGLE_PRIVATE_KEY', !!env.GOOGLE_PRIVATE_KEY, env.GOOGLE_PRIVATE_KEY?'set':''));
  lines.push(ok('WEB_JWT_SECRET', !!env.WEB_JWT_SECRET));
  lines.push(ok('WORKER_URL', !!env.WORKER_URL, env.WORKER_URL||''));
  let sheetsOk=false, note='';
  try { const vals=await sheetsGet(env,'slots!A1:F1'); sheetsOk=Array.isArray(vals); note=JSON.stringify(vals||[]); } catch(e){ sheetsOk=false; note=(e&&e.message)||String(e); }
  lines.push(ok('Sheets — slots!A1:F1', sheetsOk, note));
  let accessOk=false, accessNote='';
  try { const vals=await sheetsGet(env,'access!A1:B1'); accessOk=Array.isArray(vals); accessNote=JSON.stringify(vals||[]); }
  catch(e){ accessOk=false; accessNote=(e&&e.message)||String(e); }
  lines.push(ok('Sheets — access!A1:B1', accessOk, accessNote));
  return `<!doctype html><meta charset="utf-8"><title>Диагностика</title><style>body{font-family:system-ui;padding:20px;background:#0b0b0b;color:#fafafa}table{border-collapse:collapse}td,th{border:1px solid #333;padding:6px 8px}</style><h1>Диагностика</h1><table><thead><tr><th>Проверка</th><th>OK?</th><th>Детали</th></tr></thead><tbody>${lines.join('')}</tbody></table>`;
}

async function readBody(req){
  const ct = (req.headers.get('content-type')||'').toLowerCase();
  if (ct.includes('application/json')) return await req.json();
  if (ct.includes('application/x-www-form-urlencoded')) { const form=await req.formData(); const o={}; for (const [k,v] of form.entries()) o[k]=v; return o; }
  if (ct.includes('multipart/form-data')) { const form=await req.formData(); const o={}; for (const [k,v] of form.entries()) o[k]=v; return o; }
  try { const url=new URL(req.url); const o={}; url.searchParams.forEach((v,k)=>o[k]=v); return o; } catch { return {}; }
}
