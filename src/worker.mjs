// ====== Утилиты ======
function b64url(input) {
const bytes = input instanceof ArrayBuffer ? new Uint8Array(input) : new TextEncoder().encode(String(input));
// В воркерах есть btoa только для строки; поэтому используем встроенный base64 из ArrayBuffer
let binary = '';
for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
let base64 = btoa(binary);
return base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}


async function importPKCS8(pem, algName) {
const pemBody = pem.replace(/-----BEGIN [\s\S]*?-----/g, '').replace(/-----END [\s\S]*?-----/g, '').replace(/\s+/g, '');
const raw = Uint8Array.from(atob(pemBody), c => c.charCodeAt(0));
return crypto.subtle.importKey('pkcs8', raw, { name: algName, hash: 'SHA-256' }, false, ['sign']);
}


function ulid() {
// Простейшая ULID-подобная строка для локального использования
const now = Date.now().toString(36);
const rand = crypto.getRandomValues(new Uint8Array(10));
return now + Array.from(rand).map(b => b.toString(36).padStart(2, '0')).join('').slice(0, 18);
}


// ====== Telegram API ======
async function tgSend(env, chat_id, text, keyboard = [], opts = {}) {
const payload = {
chat_id,
text,
parse_mode: opts.parse_mode || 'HTML',
reply_markup: keyboard.length ? { inline_keyboard: keyboard } : undefined
};
await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/sendMessage`, {
method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload)
});
}


async function tgAnswer(env, cbQueryId, text = '', showAlert = false) {
await fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/answerCallbackQuery`, {
method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ callback_query_id: cbQueryId, text, show_alert: showAlert })
});
}


// ====== Мини‑дашборд ======
async function renderDashboard(env, uid) {
const slots = await listSlots(env, uid ? Number(uid) : null);
const rows = slots.map(s => {
const age = daysSince(s.last_change_at);
return `<tr>
<td>${statusEmoji(s.status)}</td>
<td>${escapeHtml(s.name)}</td>
<td>${age}</td>
<td>${s.threshold_days}</td>
<td>${Math.round(s.score)}%</td>
<td><form method="post" action="/api/slots/${s.id}/refresh"><button>Обновил</button></form></td>
</tr>`;
}).join('');
return `<!doctype html><html lang="ru"><meta charset="utf-8"/><title>Towel Tracker</title>
<style>body{font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;padding:20px;background:#0b0b0b;color:#fafafa}
table{width:100%;border-collapse:collapse;margin-top:12px}th,td{border-bottom:1px solid #333;padding:8px}th{color:#bbb;text-align:left}button{background:#1f6feb;border:0;color:#fff;padding:6px 10px;border-radius:8px;cursor:pointer}button:hover{opacity:.9}
.tip{color:#aaa}
</style>
<h1>Свежесть слотов полотенец</h1>
<p class="tip">Фильтр по владельцу: ${uid ? `tg_user_id=${uid}` : 'все'}. Для персонального вида откройте ссылку из бота.</p>
<table><thead><tr><th>Статус</th><th>Слот</th><th>Возраст, дн</th><th>Порог, дн</th><th>Оценка</th><th>Действие</th></tr></thead><tbody>${rows}</tbody></table>
</html>`;
}


function escapeHtml(s=''){return s.replace(/[&<>\"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;"}[c]));}


function json(obj){return new Response(JSON.stringify(obj),{headers:{'content-type':'application/json; charset=UTF-8'}})}
