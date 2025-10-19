/**
 * Towel Freshness Tracker — Cloudflare Worker
 * Слоты полотенец без QR: отслеживаем дату последней замены и напоминаем.
 *
 * Эндпоинты:
 *  - POST /tg/webhook      — Telegram апдейты (секрет обязателен)
 *  - GET  /dashboard?uid=  — мини-панель (чтение, кнопка "Обновил")
 *  - GET  /api/slots?owner=TG_USER_ID
 *  - POST /api/slots       — { name, owner_tg_id, room?, threshold_days? }
 *  - POST /api/slots/:id/refresh
 *  - GET  /health          — проверка живости
 *
 * Крон:
 *  - scheduled() + triggers.crons → раз в час проверяем локальный час каждого пользователя и шлём напоминания.
 *
 * Требуемые секреты/переменные (wrangler secret put):
 *  TELEGRAM_TOKEN
 *  TELEGRAM_WEBHOOK_SECRET
 *  GOOGLE_CLIENT_EMAIL
 *  GOOGLE_PRIVATE_KEY
 *  SPREADSHEET_ID
 *  (опционально) DEFAULT_TZ, DEFAULT_NOTIFY_HOUR, WORKER_URL (для красивой ссылки на дашборд из бота)
 */

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);

    // Быстрая проверка
    if (url.pathname === "/health") {
      return new Response("healthy", { status: 200 });
    }

    try {
      // Telegram webhook
      if (url.pathname === "/tg/webhook" && req.method === "POST") {
        const sec = req.headers.get("X-Telegram-Bot-Api-Secret-Token");
        if (!sec || sec !== env.TELEGRAM_WEBHOOK_SECRET) {
          return new Response("forbidden", { status: 403 });
        }
        const update = await req.json();
        await handleTelegramUpdate(update, env);
        return json({ ok: true });
      }

      // Ручной запуск крон-обработчика (удобно в тестах)
      if (url.pathname === "/__cron") {
        await runHourlyReminders(env);
        return json({ ok: true, cron: true });
      }

      // Мини-дашборд
      if (url.pathname === "/" || url.pathname === "/dashboard") {
        const uid = url.searchParams.get("uid");
        const html = await renderDashboard(env, uid);
        return htmlResponse(html);
      }

      // API: список слотов
      if (url.pathname === "/api/slots" && req.method === "GET") {
        const owner = url.searchParams.get("owner");
        const slots = await listSlots(env, owner);
        return json(slots);
      }

      // API: создание слота
      if (url.pathname === "/api/slots" && req.method === "POST") {
        const body = await req.json();
        const slot = await createSlot(env, body);
        return json(slot);
      }

      // API: обновление слота (обновил/заменил)
      if (url.pathname.startsWith("/api/slots/") && url.pathname.endsWith("/refresh") && req.method === "POST") {
        const id = url.pathname.split("/")[3];
        const res = await refreshSlot(env, id, { actor: "web" });
        return json(res);
      }

      return new Response("Not found", { status: 404 });
    } catch (e) {
      console.error("[fetch] error:", e);
      return new Response("Internal error", { status: 500 });
    }
  },

  async scheduled(event, env, ctx) {
    // Cloudflare крон вызывает этот хендлер. Напоминания выполняем асинхронно.
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

  await ensureUser(env, chatId);

  if (text.startsWith("/start")) {
    const link = await dashboardLink(env, chatId);
    const buttons = [
      [
        { text: "➕ Добавить слот", callback_data: "ui:add" },
        { text: "📋 Список", callback_data: "ui:list" },
      ],
      [
        { text: "⏰ Настроить время", callback_data: "ui:settings" },
        ...(link ? [{ text: "🌐 Веб-панель", url: link }] : []),
      ],
    ];
    await tgSend(
      env,
      chatId,
      [
        "Привет! Я слежу за «свежестью» слотов полотенец.",
        "",
        "— Слот = личное или функциональное место для полотенца (без QR).",
        "— Команда для создания: <code>/add Название | Дни</code>",
        "— Я напомню, когда пора заменить.",
      ].join("\n"),
      buttons
    );
    return;
  }

  if (text.startsWith("/add")) {
    // Формат: /add Название | 3
    const m = text.match(/^\/add\s+(.+?)\s*\|\s*(\d{1,3})$/);
    if (!m) {
      await tgSend(env, chatId, "Формат:\n<code>/add Ванная — банное | 3</code>");
      return;
    }
    const name = m[1].trim();
    const threshold_days = parseInt(m[2], 10);
    const slot = await createSlot(env, { name, owner_tg_id: chatId, threshold_days });
    await tgSend(env, chatId, `Слот «${escapeHtml(slot.name)}» создан. Порог: ${slot.threshold_days} дн. Отсчёт начат.`);
    return;
  }

  if (text.startsWith("/list")) {
    return sendList(env, chatId);
  }

  if (text.startsWith("/sethour")) {
    const m = text.match(/^\/sethour\s+(\d{1,2})$/);
    if (!m) {
      await tgSend(env, chatId, "Формат: <code>/sethour 10</code>");
      return;
    }
    const hour = clampInt(parseInt(m[1], 10), 0, 23);
    await upsertUser(env, { tg_user_id: chatId, notify_hour: hour });
    await tgSend(env, chatId, `Ок, буду писать в ${pad2(hour)}:00.`);
    return;
  }

  if (text.startsWith("/settz")) {
    const tz = text.replace("/settz", "").trim();
    if (!tz) {
      await tgSend(env, chatId, "Формат: <code>/settz Europe/Moscow</code>");
      return;
    }
    await upsertUser(env, { tg_user_id: chatId, tz });
    await tgSend(env, chatId, `Часовой пояс обновлён: ${escapeHtml(tz)}`);
    return;
  }

  if (text.startsWith("/help")) {
    await tgSend(
      env,
      chatId,
      [
        "Команды:",
        "/start — меню",
        "/add Название | Дни — создать слот",
        "/list — список слотов",
        "/sethour 10 — час напоминаний",
        "/settz Europe/Moscow — часовой пояс",
      ].join("\n")
    );
    return;
  }

  // Быстрый синоним
  if (text === "📋 Список" || text === "Список") return sendList(env, chatId);
}

async function onCallback(cb, env) {
  const chatId = cb.message.chat.id;
  const data = cb.data || "";

  if (data === "ui:add") {
    await tgSend(
      env,
      chatId,
      "Создай слот командой:\n<code>/add Название | Дни</code>\nНапример: <code>/add Ванная — банное | 3</code>"
    );
    return tgAnswer(env, cb.id, "Жду команду /add");
  }

  if (data === "ui:list") {
    await sendList(env, chatId);
    return tgAnswer(env, cb.id);
  }

  if (data === "ui:settings") {
    await tgSend(
      env,
      chatId,
      "Время: <code>/sethour 10</code>\nЧасовой пояс: <code>/settz Europe/Moscow</code>"
    );
    return tgAnswer(env, cb.id);
  }

  if (data.startsWith("refresh:")) {
    const id = data.split(":")[1];
    await refreshSlot(env, id, { actor: String(chatId) });
    await tgAnswer(env, cb.id, "Обновлено");
    return sendList(env, chatId);
  }

  if (data.startsWith("setth:")) {
    const [_, id, days] = data.split(":");
    await updateSlot(env, id, { threshold_days: parseInt(days, 10) });
    await tgAnswer(env, cb.id, `Порог: ${days} дн`);
    return sendList(env, chatId);
  }
}

async function sendList(env, chatId) {
  const slots = (await listSlots(env, chatId)).sort((a, b) => a.score - b.score);
  if (!slots.length) {
    return tgSend(env, chatId, "Слотов пока нет. Создай: <code>/add Название | Дни</code>");
  }

  const lines = slots
    .map((s) => {
      const age = daysSince(s.last_change_at);
      return `${statusEmoji(s.status)} ${escapeHtml(s.name)} — ${age} дн / порог ${s.threshold_days} • ${Math.round(
        s.score
      )}%`;
    })
    .join("\n");

  // На первые 6 слотов — кнопки "Обновил" + быстрые пороги
  const buttons = slots.slice(0, 6).map((s) => [
    { text: `🔄 ${shorten(s.name, 18)}`, callback_data: `refresh:${s.id}` },
    { text: "1д", callback_data: `setth:${s.id}:1` },
    { text: "2д", callback_data: `setth:${s.id}:2` },
    { text: "3д", callback_data: `setth:${s.id}:3` },
    { text: "5д", callback_data: `setth:${s.id}:5` },
  ]);

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
    const targetHour =
      u.notify_hour != null ? Number(u.notify_hour) : Number(env.DEFAULT_NOTIFY_HOUR || 10);
    const localHour = hourInTz(nowUTC, tz);
    if (localHour !== targetHour) continue;

    const slots = await listSlots(env, u.tg_user_id);
    const overdue = slots.filter((s) => s.status !== "OK");
    if (!overdue.length) continue;

    const body = overdue
      .sort((a, b) => a.score - b.score)
      .map((s) => `${statusEmoji(s.status)} ${escapeHtml(s.name)} — ${daysSince(s.last_change_at)} дн (порог ${s.threshold_days}
)`)
      .join("\n");

    const buttons = overdue.slice(0, 6).map((s) => [
      { text: `🔄 ${shorten(s.name, 18)}`, callback_data: `refresh:${s.id}` },
    ]);

    await tgSend(env, u.tg_user_id, `Напоминание:\n${body}`, buttons);
  }
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
  try {
    return Number(
      new Intl.DateTimeFormat("ru-RU", { hour: "2-digit", hour12: false, timeZone: tz }).format(
        date
      )
    );
  } catch {
    return date.getUTCHours();
  }
}

function pad2(n) {
  return n.toString().padStart(2, "0");
}

function clampInt(x, min, max) {
  return Math.max(min, Math.min(max, x | 0));
}

function shorten(s, n) {
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}

/* =========================
 * Google Sheets API (Service Account JWT)
 * ========================= */
async function listSlots(env, ownerFilter /* tg_user_id или null */) {
  const rows = await sheetsGet(env, "slots!A2:F");
  const slots = (rows || []).map((r) => ({
    id: r[0],
    name: r[1],
    owner_tg_id: r[2] ? Number(r[2]) : null,
    room: r[3] || "",
    threshold_days: r[4] ? Number(r[4]) : 3,
    last_change_at: r[5] || new Date().toISOString(),
  }));
  const filtered = ownerFilter ? slots.filter((s) => s.owner_tg_id === Number(ownerFilter)) : slots;
  return filtered.map((s) => ({ ...s, ...calcStatus(s.threshold_days, s.last_change_at) }));
}

async function createSlot(env, { name, owner_tg_id, room = "", threshold_days = 3 }) {
  const id = ulid();
  const now = new Date().toISOString();
  await sheetsAppend(env, "slots!A:F", [
    [id, name, String(owner_tg_id || ""), room, String(threshold_days), now],
  ]);
  await logEvent(env, { slot_id: id, action: "CREATE", actor: String(owner_tg_id || ""), note: name });
  return {
    id,
    name,
    owner_tg_id,
    room,
    threshold_days,
    last_change_at: now,
    score: 100,
    status: "OK",
  };
}

async function refreshSlot(env, id, { actor = "" } = {}) {
  const now = new Date().toISOString();
  const { rowIndex } = await findRowById(env, "slots", id);
  if (!rowIndex) throw new Error("slot not found");
  await sheetsUpdate(env, [{ range: `slots!F${rowIndex}:F${rowIndex}`, values: [[now]] }]);
  await logEvent(env, { slot_id: id, action: "REFRESH", actor: String(actor), note: "" });
  return { ok: true };
}

async function updateSlot(env, id, patch) {
  const { rowIndex } = await findRowById(env, "slots", id);
  if (!rowIndex) throw new Error("slot not found");
  const updates = [];
  if (patch.name != null) updates.push({ range: `slots!B${rowIndex}:B${rowIndex}`, values: [[patch.name]] });
  if (patch.owner_tg_id != null)
    updates.push({ range: `slots!C${rowIndex}:C${rowIndex}`, values: [[String(patch.owner_tg_id)]] });
  if (patch.room != null) updates.push({ range: `slots!D${rowIndex}:D${rowIndex}`, values: [[patch.room]] });
  if (patch.threshold_days != null)
    updates.push({ range: `slots!E${rowIndex}:E${rowIndex}`, values: [[String(patch.threshold_days)]] });
  if (patch.last_change_at != null)
    updates.push({ range: `slots!F${rowIndex}:F${rowIndex}`, values: [[patch.last_change_at]] });
  if (updates.length) await sheetsUpdate(env, updates);
  return { ok: true };
}

async function ensureUser(env, tg_user_id) {
  const rows = await sheetsGet(env, "users!A2:C");
  const exists = (rows || []).some((r) => r[0] === String(tg_user_id));
  if (!exists) {
    await sheetsAppend(env, "users!A:C", [
      [String(tg_user_id), env.DEFAULT_TZ || "Europe/Moscow", String(env.DEFAULT_NOTIFY_HOUR || 10)],
    ]);
  }
}

async function upsertUser(env, { tg_user_id, tz, notify_hour }) {
  const { rowIndex } = await findRowById(env, "users", String(tg_user_id));
  if (!rowIndex) {
    await sheetsAppend(env, "users!A:C", [
      [
        String(tg_user_id),
        tz || env.DEFAULT_TZ || "Europe/Moscow",
        String(notify_hour != null ? notify_hour : env.DEFAULT_NOTIFY_HOUR || 10),
      ],
    ]);
  } else {
    const updates = [];
    if (tz != null) updates.push({ range: `users!B${rowIndex}:B${rowIndex}`, values: [[tz]] });
    if (notify_hour != null)
      updates.push({ range: `users!C${rowIndex}:C${rowIndex}`, values: [[String(notify_hour)]] });
    if (updates.length) await sheetsUpdate(env, updates);
  }
}

async function listUsers(env) {
  const rows = await sheetsGet(env, "users!A2:C");
  return (rows || []).map((r) => ({
    tg_user_id: Number(r[0]),
    tz: r[1] || "Europe/Moscow",
    notify_hour: r[2] ? Number(r[2]) : 10,
  }));
}

async function logEvent(env, { slot_id, action, actor, note }) {
  const ts = new Date().toISOString();
  await sheetsAppend(env, "events!A:E", [[ts, slot_id, action, actor, note || ""]]);
}

async function findRowById(env, sheetName, id) {
  // Ищем по колонке A, возвращаем фактический номер строки (учитывая заголовок).
  const rows = await sheetsGet(env, `${sheetName}!A2:A`);
  let idx = 0;
  for (const r of rows || []) {
    idx++;
    if ((r[0] || "") === String(id)) return { rowIndex: idx + 1 }; // +1 за заголовок
  }
  return { rowIndex: null };
}

/* =========================
 * Google Sheets REST обёртки
 * ========================= */
async function getAccessToken(env) {
  const iss = env.GOOGLE_CLIENT_EMAIL;
  const pk = (env.GOOGLE_PRIVATE_KEY || "").replace(/\\n/g, "\n");
  const now = Math.floor(Date.now() / 1000);

  const header = { alg: "RS256", typ: "JWT" };
  const claim = {
    iss,
    scope: "https://www.googleapis.com/auth/spreadsheets",
    aud: "https://oauth2.googleapis.com/token",
    exp: now + 3600,
    iat: now,
  };

  const encHeader = b64url(new TextEncoder().encode(JSON.stringify(header)));
  const encClaim = b64url(new TextEncoder().encode(JSON.stringify(claim)));
  const signingInput = `${encHeader}.${encClaim}`;

  const key = await importPKCS8(pk, "RSASSA-PKCS1-v1_5");
  const signature = await crypto.subtle.sign(
    { name: "RSASSA-PKCS1-v1_5" },
    key,
    new TextEncoder().encode(signingInput)
  );
  const jwt = `${signingInput}.${b64url(signature)}`;

  const resp = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion: jwt,
    }),
  });
  if (!resp.ok) throw new Error("oauth token error: " + resp.status);
  const data = await resp.json();
  return data.access_token;
}

async function sheetsGet(env, rangeA1) {
  const token = await getAccessToken(env);
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${env.SPREADSHEET_ID}/values/${encodeURIComponent(
    rangeA1
  )}?majorDimension=ROWS`;
  const resp = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  if (!resp.ok) throw new Error("sheets get error: " + resp.status);
  const data = await resp.json();
  return data.values || [];
}

async function sheetsAppend(env, rangeA1, values) {
  const token = await getAccessToken(env);
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${env.SPREADSHEET_ID}/values/${encodeURIComponent(
    rangeA1
  )}:append?valueInputOption=RAW`;
  const resp = await fetch(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({ range: rangeA1, majorDimension: "ROWS", values }),
  });
  if (!resp.ok) throw new Error("sheets append error: " + resp.status);
}

async function sheetsUpdate(env, updates /* [{ range, values }] */) {
  const token = await getAccessToken(env);
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${env.SPREADSHEET_ID}/values:batchUpdate`;
  const resp = await fetch(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({ valueInputOption: "RAW", data: updates }),
  });
  if (!resp.ok) throw new Error("sheets update error: " + resp.status);
}

/* =========================
 * Мини-дашборд (HTML)
 * ========================= */
async function renderDashboard(env, uid) {
  const slots = await listSlots(env, uid ? Number(uid) : null);
  const rows = slots
    .map((s) => {
      const age = daysSince(s.last_change_at);
      return `
      <tr>
        <td>${statusEmoji(s.status)}</td>
        <td>${escapeHtml(s.name)}</td>
        <td>${age}</td>
        <td>${s.threshold_days}</td>
        <td>${Math.round(s.score)}%</td>
        <td>
          <form method="post" action="/api/slots/${s.id}/refresh">
            <button>Обновил</button>
          </form>
        </td>
      </tr>`;
    })
    .join("");

  return `<!doctype html>
<html lang="ru">
<meta charset="utf-8"/>
<title>Towel Tracker</title>
<style>
  :root { color-scheme: dark; }
  body{font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;padding:20px;background:#0b0b0b;color:#fafafa}
  table{width:100%;border-collapse:collapse;margin-top:12px}
  th,td{border-bottom:1px solid #333;padding:8px}
  th{color:#bbb;text-align:left}
  button{background:#1f6feb;border:0;color:#fff;padding:6px 10px;border-radius:8px;cursor:pointer}
  button:hover{opacity:.9}
  .tip{color:#aaa}
</style>
<h1>Свежесть слотов полотенец</h1>
<p class="tip">Фильтр по владельцу: ${uid ? `tg_user_id=${escapeHtml(String(uid))}` : "все"}.</p>
<table>
  <thead>
    <tr><th>Статус</th><th>Слот</th><th>Возраст, дн</th><th>Порог, дн</th><th>Оценка</th><th>Действие</th></tr>
  </thead>
  <tbody>${rows}</tbody>
</table>
</html>`;
}

/* =========================
 * Ссылки/утилиты
 * ========================= */
async function dashboardLink(env, tgUserId) {
  // Если задан WORKER_URL (например, https://towel-tracker.yourname.workers.dev), используем его.
  // Иначе вернём пустую строку — в /start покажем кнопки без ссылки (не критично).
  if (env.WORKER_URL) {
    return `${env.WORKER_URL.replace(/\/+$/,'')}/dashboard?uid=${tgUserId}`;
  }
  return "";
}

function htmlResponse(html) {
  return new Response(html, { headers: { "content-type": "text/html; charset=UTF-8" } });
}

function json(obj) {
  return new Response(JSON.stringify(obj), {
    headers: { "content-type": "application/json; charset=UTF-8" },
  });
}

function escapeHtml(s = "") {
  return s.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

/* =========================
 * Крипто/идентификаторы
 * ========================= */
function b64url(bufOrBytes) {
  const bytes =
    bufOrBytes instanceof ArrayBuffer ? new Uint8Array(bufOrBytes) : new Uint8Array(bufOrBytes);
  let binary = "";
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  let base64 = btoa(binary);
  return base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

async function importPKCS8(pem, algName) {
  const pemBody = pem
    .replace(/-----BEGIN [\s\S]*?-----/g, "")
    .replace(/-----END [\s\S]*?-----/g, "")
    .replace(/\s+/g, "");
  const raw = Uint8Array.from(atob(pemBody), (c) => c.charCodeAt(0));
  return crypto.subtle.importKey(
    "pkcs8",
    raw,
    { name: algName, hash: "SHA-256" },
    false,
    ["sign"]
  );
}

function ulid() {
  // Простой ULID-подобный id (достаточен для нашей задачи)
  const now = Date.now().toString(36);
  const rand = crypto.getRandomValues(new Uint8Array(16));
  return (
    now +
    Array.from(rand)
      .map((b) => b.toString(36).padStart(2, "0"))
      .join("")
  ).slice(0, 26);
}

/* =========================
 * Telegram API
 * ========================= */
async function tgSend(env, chat_id, text, keyboard = [], opts = {}) {
  const payload = {
    chat_id,
    text,
    parse_mode: opts.parse_mode || "HTML",
    reply_markup: keyboard.length ? { inline_keyboard: keyboard } : undefined,
    disable_web_page_preview: true,
  };
  await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
}

async function tgAnswer(env, cbQueryId, text = "", showAlert = false) {
  await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/answerCallbackQuery`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ callback_query_id: cbQueryId, text, show_alert: showAlert }),
  });
}
