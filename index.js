require("dotenv").config();
const { Client, GatewayIntentBits, PermissionsBitField } = require("discord.js");
const sqlite3 = require("sqlite3").verbose();

/* ================= CONFIG ================= */

const PREFIX = "!";
const TIMEZONE = "America/Chicago";

const LEADERSHIP_ROLES = ["Leadership"];
const CLOSER_ROLES = ["Closer"];
const OPPONENT_ROLE_ID = "1479314578642042964";

const SALES_CHANNEL_ID = "1458250404835098795";
const APPOINTMENTS_CHANNEL_ID = "1458250231354495150";
const GENERAL_CHAT_CHANNEL_ID = "1458248543000068228";

const TOKEN = process.env.DISCORD_TOKEN;
if (!TOKEN) {
  console.error("❌ Missing DISCORD_TOKEN environment variable.");
  process.exit(1);
}

const DB_PATH = process.env.DB_PATH || "./bot.db";

/* ================= DISCORD ================= */

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
});

/* ================= DB ================= */

const db = new sqlite3.Database(DB_PATH);

function run(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

function get(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

function all(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

/* ================= TIME HELPERS ================= */

function ctDateKey(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);

  const y = parts.find((p) => p.type === "year")?.value;
  const m = parts.find((p) => p.type === "month")?.value;
  const d = parts.find((p) => p.type === "day")?.value;
  return `${y}-${m}-${d}`;
}

function ctTimestampString(date = new Date()) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function nowIso() {
  return new Date().toISOString();
}

function nowMs() {
  return Date.now();
}

/* ================= PERMISSIONS ================= */

function hasRole(member, roleNames) {
  if (!member?.roles?.cache) return false;
  const lower = roleNames.map((r) => r.toLowerCase());
  return member.roles.cache.some((r) => lower.includes(r.name.toLowerCase()));
}

function isAdmin(member) {
  return !!member?.permissions?.has?.(PermissionsBitField.Flags.Administrator);
}

function isLeadership(member) {
  if (!member) return false;
  if (isAdmin(member)) return true;
  return hasRole(member, LEADERSHIP_ROLES);
}

function canSetSale(member) {
  if (!member) return false;
  if (isLeadership(member)) return true;
  return hasRole(member, CLOSER_ROLES);
}

function canUseOpponent(member) {
  if (!member) return false;
  if (isAdmin(member)) return true;
  if (isLeadership(member)) return true;
  return member.roles.cache.some((r) => r.id === OPPONENT_ROLE_ID);
}

/* ================= DISPLAY NAME ================= */

async function displayNameFor(guild, userId) {
  try {
    const member = await guild.members.fetch(userId);
    return member.displayName || member.user.username;
  } catch {
    return `<@${userId}>`;
  }
}

/* ================= DB INIT ================= */

async function initDb() {
  await run(`
    CREATE TABLE IF NOT EXISTS sales (
      guild_id TEXT NOT NULL,
      user_id  TEXT NOT NULL,
      total_sales INTEGER NOT NULL DEFAULT 0,
      self_gen INTEGER NOT NULL DEFAULT 0,
      set_sales INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (guild_id, user_id)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS gym (
      guild_id TEXT NOT NULL,
      user_id  TEXT NOT NULL,
      checkins INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (guild_id, user_id)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS daily_appts (
      guild_id TEXT NOT NULL,
      date_key TEXT NOT NULL,
      user_id  TEXT NOT NULL,
      count    INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (guild_id, date_key, user_id)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS appt_blitz (
      guild_id TEXT NOT NULL,
      blitz_name TEXT NOT NULL,
      start_ts TEXT NOT NULL,
      end_ts   TEXT,
      is_active INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (guild_id, blitz_name)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS blitz_appts (
      guild_id TEXT NOT NULL,
      blitz_name TEXT NOT NULL,
      date_key TEXT NOT NULL,
      user_id  TEXT NOT NULL,
      count    INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (guild_id, blitz_name, date_key, user_id)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS op_sales (
      guild_id TEXT NOT NULL,
      user_id  TEXT NOT NULL,
      total_sales INTEGER NOT NULL DEFAULT 0,
      self_gen INTEGER NOT NULL DEFAULT 0,
      set_sales INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (guild_id, user_id)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS op_daily_appts (
      guild_id TEXT NOT NULL,
      date_key TEXT NOT NULL,
      user_id  TEXT NOT NULL,
      count    INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (guild_id, date_key, user_id)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS processed_messages (
      message_id TEXT PRIMARY KEY,
      created_at TEXT NOT NULL
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS board_messages (
      guild_id TEXT NOT NULL,
      board_key TEXT NOT NULL,
      channel_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      PRIMARY KEY (guild_id, board_key)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS event_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      guild_id TEXT NOT NULL,
      event_type TEXT NOT NULL,
      user_id TEXT NOT NULL,
      amount INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS alert_log (
      alert_key TEXT PRIMARY KEY,
      created_at TEXT NOT NULL
    )
  `);

  try {
    await run(`ALTER TABLE sales ADD COLUMN total_sales INTEGER NOT NULL DEFAULT 0`);
  } catch {}
  await run(`
    UPDATE sales
    SET total_sales = (self_gen + set_sales)
    WHERE total_sales = 0 AND (self_gen + set_sales) > 0
  `);
}

/* ================= MESSAGE DEDUPE ================= */

async function claimMessage(messageId) {
  try {
    await run(
      `INSERT INTO processed_messages (message_id, created_at)
       VALUES (?, ?)`,
      [messageId, nowIso()]
    );
    return true;
  } catch {
    return false;
  }
}

/* ================= SALES HELPERS ================= */

async function ensureSalesRow(guildId, userId) {
  await run(
    `INSERT OR IGNORE INTO sales (guild_id, user_id, total_sales, self_gen, set_sales)
     VALUES (?, ?, 0, 0, 0)`,
    [guildId, userId]
  );
}

async function recordSetSale(guildId, closerId, setterId) {
  await ensureSalesRow(guildId, closerId);
  await ensureSalesRow(guildId, setterId);

  await run(
    `UPDATE sales SET total_sales = total_sales + 1 WHERE guild_id = ? AND user_id = ?`,
    [guildId, closerId]
  );

  await run(
    `UPDATE sales SET total_sales = total_sales + 1 WHERE guild_id = ? AND user_id = ?`,
    [guildId, setterId]
  );

  await run(
    `UPDATE sales SET set_sales = set_sales + 1 WHERE guild_id = ? AND user_id = ?`,
    [guildId, setterId]
  );
}

async function recordSelfGen(guildId, userId) {
  await ensureSalesRow(guildId, userId);
  await run(
    `UPDATE sales
     SET total_sales = total_sales + 1,
         self_gen = self_gen + 1,
         set_sales = set_sales + 1
     WHERE guild_id = ? AND user_id = ?`,
    [guildId, userId]
  );
}

/* ================= OPPONENT SALES HELPERS ================= */

async function ensureOpSalesRow(guildId, userId) {
  await run(
    `INSERT OR IGNORE INTO op_sales (guild_id, user_id, total_sales, self_gen, set_sales)
     VALUES (?, ?, 0, 0, 0)`,
    [guildId, userId]
  );
}

async function recordOpSetSale(guildId, closerId, setterId) {
  await ensureOpSalesRow(guildId, closerId);
  await ensureOpSalesRow(guildId, setterId);

  await run(
    `UPDATE op_sales SET total_sales = total_sales + 1 WHERE guild_id = ? AND user_id = ?`,
    [guildId, closerId]
  );

  await run(
    `UPDATE op_sales SET total_sales = total_sales + 1 WHERE guild_id = ? AND user_id = ?`,
    [guildId, setterId]
  );

  await run(
    `UPDATE op_sales SET set_sales = set_sales + 1 WHERE guild_id = ? AND user_id = ?`,
    [guildId, setterId]
  );
}

async function recordOpSelfGen(guildId, userId) {
  await ensureOpSalesRow(guildId, userId);
  await run(
    `UPDATE op_sales
     SET total_sales = total_sales + 1,
         self_gen = self_gen + 1,
         set_sales = set_sales + 1
     WHERE guild_id = ? AND user_id = ?`,
    [guildId, userId]
  );
}

/* ================= GYM HELPERS ================= */

async function ensureGymRow(guildId, userId) {
  await run(
    `INSERT OR IGNORE INTO gym (guild_id, user_id, checkins)
     VALUES (?, ?, 0)`,
    [guildId, userId]
  );
}

async function getGymCount(guildId, userId) {
  const row = await get(
    `SELECT checkins FROM gym WHERE guild_id = ? AND user_id = ?`,
    [guildId, userId]
  );
  return row?.checkins ?? 0;
}

async function setGymCount(guildId, userId, nextCount) {
  await ensureGymRow(guildId, userId);
  await run(
    `UPDATE gym SET checkins = ? WHERE guild_id = ? AND user_id = ?`,
    [nextCount, guildId, userId]
  );
}

async function addGymDelta(guildId, userId, delta) {
  await ensureGymRow(guildId, userId);
  const current = await getGymCount(guildId, userId);
  const next = Math.max(0, current + delta);
  await setGymCount(guildId, userId, next);
  return next;
}

/* ================= APPT HELPERS ================= */

async function addDailyAppt(guildId, userId, dateKey, delta) {
  await run(
    `INSERT OR IGNORE INTO daily_appts (guild_id, date_key, user_id, count)
     VALUES (?, ?, ?, 0)`,
    [guildId, dateKey, userId]
  );

  const row = await get(
    `SELECT count FROM daily_appts
     WHERE guild_id = ? AND date_key = ? AND user_id = ?`,
    [guildId, dateKey, userId]
  );

  const current = row?.count ?? 0;
  const next = Math.max(0, current + delta);

  await run(
    `UPDATE daily_appts
     SET count = ?
     WHERE guild_id = ? AND date_key = ? AND user_id = ?`,
    [next, guildId, dateKey, userId]
  );

  return next;
}

async function dailyApptsLeaderboard(guildId, dateKey) {
  return all(
    `SELECT user_id, count
     FROM daily_appts
     WHERE guild_id = ? AND date_key = ?
     ORDER BY count DESC, user_id ASC`,
    [guildId, dateKey]
  );
}

async function clearDailyAppts(guildId, dateKey) {
  await run(
    `DELETE FROM daily_appts WHERE guild_id = ? AND date_key = ?`,
    [guildId, dateKey]
  );
}

/* ================= OPPONENT APPT HELPERS ================= */

async function addOpDailyAppt(guildId, userId, dateKey, delta) {
  await run(
    `INSERT OR IGNORE INTO op_daily_appts (guild_id, date_key, user_id, count)
     VALUES (?, ?, ?, 0)`,
    [guildId, dateKey, userId]
  );

  const row = await get(
    `SELECT count FROM op_daily_appts
     WHERE guild_id = ? AND date_key = ? AND user_id = ?`,
    [guildId, dateKey, userId]
  );

  const current = row?.count ?? 0;
  const next = Math.max(0, current + delta);

  await run(
    `UPDATE op_daily_appts
     SET count = ?
     WHERE guild_id = ? AND date_key = ? AND user_id = ?`,
    [next, guildId, dateKey, userId]
  );

  return next;
}

async function opDailyApptsLeaderboard(guildId, dateKey) {
  return all(
    `SELECT user_id, count
     FROM op_daily_appts
     WHERE guild_id = ? AND date_key = ?
     ORDER BY count DESC, user_id ASC`,
    [guildId, dateKey]
  );
}

/* ================= BLITZ HELPERS ================= */

async function getActiveBlitz(guildId) {
  return get(
    `SELECT blitz_name, start_ts, end_ts, is_active
     FROM appt_blitz
     WHERE guild_id = ? AND is_active = 1
     LIMIT 1`,
    [guildId]
  );
}

async function getMostRecentEndedBlitz(guildId) {
  return get(
    `SELECT blitz_name, start_ts, end_ts
     FROM appt_blitz
     WHERE guild_id = ? AND is_active = 0 AND end_ts IS NOT NULL
     ORDER BY end_ts DESC
     LIMIT 1`,
    [guildId]
  );
}

async function blitzExists(guildId, blitzName) {
  const row = await get(
    `SELECT blitz_name
     FROM appt_blitz
     WHERE guild_id = ? AND blitz_name = ?
     LIMIT 1`,
    [guildId, blitzName]
  );
  return !!row;
}

async function startBlitz(guildId, blitzName) {
  if (await blitzExists(guildId, blitzName)) {
    return { ok: false, reason: "exists" };
  }

  const active = await getActiveBlitz(guildId);
  if (active) {
    return { ok: false, reason: "active", activeName: active.blitz_name };
  }

  const startTs = new Date().toISOString();
  await run(
    `INSERT INTO appt_blitz (guild_id, blitz_name, start_ts, end_ts, is_active)
     VALUES (?, ?, ?, NULL, 1)`,
    [guildId, blitzName, startTs]
  );

  return { ok: true, blitzName, startTs };
}

async function endBlitz(guildId) {
  const active = await getActiveBlitz(guildId);
  if (!active) return { ok: false, reason: "none" };

  const endTs = new Date().toISOString();
  await run(
    `UPDATE appt_blitz
     SET end_ts = ?, is_active = 0
     WHERE guild_id = ? AND blitz_name = ?`,
    [endTs, guildId, active.blitz_name]
  );

  return { ok: true, blitzName: active.blitz_name, endTs };
}

async function addBlitzAppt(guildId, blitzName, dateKey, userId, delta) {
  await run(
    `INSERT OR IGNORE INTO blitz_appts (guild_id, blitz_name, date_key, user_id, count)
     VALUES (?, ?, ?, ?, 0)`,
    [guildId, blitzName, dateKey, userId]
  );

  const row = await get(
    `SELECT count
     FROM blitz_appts
     WHERE guild_id = ? AND blitz_name = ? AND date_key = ? AND user_id = ?`,
    [guildId, blitzName, dateKey, userId]
  );

  const current = row?.count ?? 0;
  const next = Math.max(0, current + delta);

  await run(
    `UPDATE blitz_appts
     SET count = ?
     WHERE guild_id = ? AND blitz_name = ? AND date_key = ? AND user_id = ?`,
    [next, guildId, blitzName, dateKey, userId]
  );

  return next;
}

async function blitzApptsByDate(guildId, blitzName) {
  return all(
    `SELECT date_key, user_id, count
     FROM blitz_appts
     WHERE guild_id = ? AND blitz_name = ?
     ORDER BY date_key ASC, count DESC`,
    [guildId, blitzName]
  );
}

async function clearBlitzApptsTarget(guildId) {
  const active = await getActiveBlitz(guildId);
  if (active) {
    await run(
      `DELETE FROM blitz_appts WHERE guild_id = ? AND blitz_name = ?`,
      [guildId, active.blitz_name]
    );
    return { ok: true, blitzName: active.blitz_name, mode: "active" };
  }

  const recent = await getMostRecentEndedBlitz(guildId);
  if (recent) {
    await run(
      `DELETE FROM blitz_appts WHERE guild_id = ? AND blitz_name = ?`,
      [guildId, recent.blitz_name]
    );
    return { ok: true, blitzName: recent.blitz_name, mode: "recent" };
  }

  return { ok: false, reason: "none" };
}

/* ================= ACTIVITY / ALERT HELPERS ================= */

async function logEvent(guildId, eventType, userId, amount = 1) {
  await run(
    `INSERT INTO event_log (guild_id, event_type, user_id, amount, created_at)
     VALUES (?, ?, ?, ?, ?)`,
    [guildId, eventType, userId, amount, nowIso()]
  );
}

async function recentEventCount(guildId, eventType, minutes) {
  return get(
    `SELECT COALESCE(SUM(amount), 0) AS total
     FROM event_log
     WHERE guild_id = ?
       AND event_type = ?
       AND created_at >= datetime('now', ?)`,
    [guildId, eventType, `-${minutes} minutes`]
  );
}

async function claimAlert(alertKey) {
  try {
    await run(
      `INSERT INTO alert_log (alert_key, created_at)
       VALUES (?, ?)`,
      [alertKey, nowIso()]
    );
    return true;
  } catch {
    return false;
  }
}

async function getChannelSafe(guild, channelId) {
  try {
    return await guild.channels.fetch(channelId);
  } catch {
    return null;
  }
}

async function postGeneralHype(guild, content) {
  const channel = await getChannelSafe(guild, GENERAL_CHAT_CHANNEL_ID);
  if (!channel?.isTextBased?.()) return;
  await channel.send(content);
}

/* ================= LIVE BOARD HELPERS ================= */

async function getBoardMessageRow(guildId, boardKey) {
  return get(
    `SELECT channel_id, message_id
     FROM board_messages
     WHERE guild_id = ? AND board_key = ?`,
    [guildId, boardKey]
  );
}

async function setBoardMessageRow(guildId, boardKey, channelId, messageId) {
  await run(
    `INSERT INTO board_messages (guild_id, board_key, channel_id, message_id)
     VALUES (?, ?, ?, ?)
     ON CONFLICT(guild_id, board_key)
     DO UPDATE SET channel_id = excluded.channel_id, message_id = excluded.message_id`,
    [guildId, boardKey, channelId, messageId]
  );
}

async function ensurePinnedBoard(guild, boardKey, channelId, content) {
  const channel = await getChannelSafe(guild, channelId);
  if (!channel?.isTextBased?.()) return;

  const existing = await getBoardMessageRow(guild.id, boardKey);

  if (existing?.message_id) {
    try {
      const msg = await channel.messages.fetch(existing.message_id);
      await msg.edit(content);
      return;
    } catch {}
  }

  const newMsg = await channel.send(content);
  try {
    await newMsg.pin();
  } catch {}
  await setBoardMessageRow(guild.id, boardKey, channel.id, newMsg.id);
}

async function buildSalesBoard(guildId, guild) {
  const rows = await all(
    `SELECT user_id, total_sales, self_gen, set_sales
     FROM sales
     WHERE guild_id = ?
     ORDER BY total_sales DESC, self_gen DESC, set_sales DESC`,
    [guildId]
  );

  let output = "🏆 **LIVE SALES LEADERBOARD**\n";
  output += `Updated: ${ctTimestampString()} CT\n\n`;

  if (!rows.length) {
    output += "No sales recorded yet.";
    return output;
  }

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const name = await displayNameFor(guild, r.user_id);
    output += `${i + 1}. ${name} — ${r.total_sales} sale${r.total_sales === 1 ? "" : "s"} (Self-gen: ${r.self_gen}, Set: ${r.set_sales})\n`;
  }

  return output.slice(0, 1900);
}

async function buildApptsBoard(guildId, guild) {
  const dateKey = ctDateKey();
  const rows = await all(
    `SELECT user_id, count
     FROM daily_appts
     WHERE guild_id = ? AND date_key = ?
     ORDER BY count DESC, user_id ASC`,
    [guildId, dateKey]
  );

  let output = "📅 **LIVE APPOINTMENTS LEADERBOARD**\n";
  output += `Date: ${dateKey} (CT)\n`;
  output += `Updated: ${ctTimestampString()} CT\n\n`;

  if (!rows.length) {
    output += "No appointments recorded yet today.";
    return output;
  }

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const name = await displayNameFor(guild, r.user_id);
    output += `${i + 1}. ${name} — ${r.count}\n`;
  }

  return output.slice(0, 1900);
}

async function refreshLiveBoards(guild) {
  const salesBoard = await buildSalesBoard(guild.id, guild);
  const apptsBoard = await buildApptsBoard(guild.id, guild);

  await ensurePinnedBoard(guild, "team_sales_live", SALES_CHANNEL_ID, salesBoard);
  await ensurePinnedBoard(guild, "team_appts_live", APPOINTMENTS_CHANNEL_ID, apptsBoard);
}

/* ================= HYPE / MILESTONE HELPERS ================= */

async function evaluateSalesMoments(guild, guildId, userId) {
  const userRow = await get(
    `SELECT total_sales, self_gen, set_sales
     FROM sales
     WHERE guild_id = ? AND user_id = ?`,
    [guildId, userId]
  );
  if (!userRow) return;

  const userName = await displayNameFor(guild, userId);
  const dateKey = ctDateKey();

  const firstSaleKey = `sale:first:${guildId}:${dateKey}:${userId}`;
  if ((userRow.total_sales || 0) === 1 && (await claimAlert(firstSaleKey))) {
    await postGeneralHype(guild, `🔥 **First sale on the board!** ${userName} just got the team rolling!`);
  }

  for (const milestone of [2, 3, 5, 10]) {
    const key = `sale:rep:${guildId}:${dateKey}:${userId}:${milestone}`;
    if ((userRow.total_sales || 0) >= milestone && (await claimAlert(key))) {
      await postGeneralHype(guild, `🏆 **Sales Milestone!** ${userName} just hit **${milestone} sales**! Keep the pressure on!`);
    }
  }

  const teamRow = await get(
    `SELECT COALESCE(SUM(total_sales - set_sales + self_gen), 0) AS team_sales
     FROM sales
     WHERE guild_id = ?`,
    [guildId]
  );

  for (const milestone of [3, 5, 10, 15, 20]) {
    const key = `sale:team:${guildId}:${dateKey}:${milestone}`;
    if ((teamRow?.team_sales || 0) >= milestone && (await claimAlert(key))) {
      await postGeneralHype(guild, `🚀 **TEAM SALES MILESTONE!** Solrite just reached **${milestone} true team sales**! Keep stacking wins!`);
    }
  }

  const recentSales = await recentEventCount(guildId, "sale", 10);
  const surgeBucket = Math.floor(nowMs() / (15 * 60 * 1000));
  if ((recentSales?.total || 0) >= 2 && (await claimAlert(`sale:surge:${guildId}:${surgeBucket}`))) {
    await postGeneralHype(guild, `⚡ **SALES SURGE!** The team has stacked **${recentSales.total} sales** in the last 10 minutes! Momentum is building!`);
  }
}

async function evaluateApptMoments(guild, guildId, userId) {
  const dateKey = ctDateKey();

  const userRow = await get(
    `SELECT count
     FROM daily_appts
     WHERE guild_id = ? AND date_key = ? AND user_id = ?`,
    [guildId, dateKey, userId]
  );
  if (!userRow) return;

  const userName = await displayNameFor(guild, userId);

  const firstApptKey = `appt:first:${guildId}:${dateKey}:${userId}`;
  if ((userRow.count || 0) === 1 && (await claimAlert(firstApptKey))) {
    await postGeneralHype(guild, `📞 **First appointment locked in!** ${userName} is on the board!`);
  }

  for (const milestone of [3, 5, 10]) {
    const key = `appt:rep:${guildId}:${dateKey}:${userId}:${milestone}`;
    if ((userRow.count || 0) >= milestone && (await claimAlert(key))) {
      await postGeneralHype(guild, `🎯 **Appointments Milestone!** ${userName} just reached **${milestone} appointments today**!`);
    }
  }

  const teamRow = await get(
    `SELECT COALESCE(SUM(count), 0) AS team_appts
     FROM daily_appts
     WHERE guild_id = ? AND date_key = ?`,
    [guildId, dateKey]
  );

  for (const milestone of [5, 10, 15, 20, 30]) {
    const key = `appt:team:${guildId}:${dateKey}:${milestone}`;
    if ((teamRow?.team_appts || 0) >= milestone && (await claimAlert(key))) {
      await postGeneralHype(guild, `🔥 **TEAM APPOINTMENT MILESTONE!** Solrite just reached **${milestone} appointments today**!`);
    }
  }

  const recentAppts = await recentEventCount(guildId, "appt", 10);
  const surgeBucket = Math.floor(nowMs() / (15 * 60 * 1000));
  if ((recentAppts?.total || 0) >= 3 && (await claimAlert(`appt:surge:${guildId}:${surgeBucket}`))) {
    await postGeneralHype(guild, `🚀 **APPOINTMENT SURGE!** The team just put up **${recentAppts.total} appointments** in the last 10 minutes! Keep the gas down!`);
  }
}

/* ================= PARSING HELPERS ================= */

function parsePositiveInt(s) {
  if (!s) return null;
  const n = Number(s);
  if (!Number.isInteger(n)) return null;
  if (n <= 0) return null;
  return n;
}

/* ================= MESSAGE HANDLER ================= */

client.on("messageCreate", async (msg) => {
  try {
    if (!msg.guild) return;
    if (msg.author.bot) return;

    const content = (msg.content || "").trim();
    if (!content.startsWith(PREFIX)) return;

    const firstProcess = await claimMessage(msg.id);
    if (!firstProcess) return;

    const parts = content.slice(PREFIX.length).trim().split(/\s+/);
    const command = (parts.shift() || "").toLowerCase();
    const guildId = msg.guild.id;

    /* ===== OUR SALES ===== */

    if (command === "setsale") {
      if (!canSetSale(msg.member)) {
        return msg.reply("❌ Only Leadership/Closer can use `!setsale`.");
      }

      const target = msg.mentions.users.first();
      if (!target) return msg.reply("Usage: `!setsale @user`");
      if (target.id === msg.author.id) {
        return msg.reply("Use `!selfgen` for self-generated sales.");
      }

      await recordSetSale(guildId, msg.author.id, target.id);
      await logEvent(guildId, "sale", msg.author.id, 1);

      const closerName = msg.member?.displayName || msg.author.username;
      const setterName = await displayNameFor(msg.guild, target.id);

      await refreshLiveBoards(msg.guild);
      await evaluateSalesMoments(msg.guild, guildId, msg.author.id);

      return msg.reply(
        `✅ Sale recorded. +1 sale to ${closerName} & ${setterName}. Set credited to ${setterName}.`
      );
    }

    if (command === "selfgen") {
      if (!canSetSale(msg.member)) {
        return msg.reply("❌ Only Leadership/Closer can use `!selfgen`.");
      }

      await recordSelfGen(guildId, msg.author.id);
      await logEvent(guildId, "sale", msg.author.id, 1);

      const name = msg.member?.displayName || msg.author.username;

      await refreshLiveBoards(msg.guild);
      await evaluateSalesMoments(msg.guild, guildId, msg.author.id);

      return msg.reply(`✅ Self-gen recorded for ${name}.`);
    }

    if (command === "sales") {
      const rows = await all(
        `SELECT user_id, total_sales, self_gen, set_sales
         FROM sales
         WHERE guild_id = ?
         ORDER BY total_sales DESC, self_gen DESC, set_sales DESC`,
        [guildId]
      );

      if (!rows.length) {
        return msg.reply("**Sales Leaderboard**\n(No sales recorded yet.)");
      }

      const lines = [];
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const name = await displayNameFor(msg.guild, r.user_id);
        const t = r.total_sales || 0;
        lines.push(
          `${i + 1}. ${name}: ${t} sale${t === 1 ? "" : "s"} (Self-gen: ${r.self_gen || 0}, Set: ${r.set_sales || 0})`
        );
      }

      return msg.reply(`**Sales Leaderboard**\n${lines.join("\n")}`.slice(0, 1900));
    }

    if (command === "clearsales") {
      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership can use `!clearsales`.");
      }

      await run(`DELETE FROM sales WHERE guild_id = ?`, [guildId]);
      await refreshLiveBoards(msg.guild);
      return msg.reply("🧹 Sales leaderboard cleared.");
    }

    /* ===== OPPONENT SALES ===== */

    if (command === "opsale") {
      if (!canUseOpponent(msg.member)) return;

      const target = msg.mentions.users.first();
      if (!target) return msg.reply("Usage: `!opsale @user`");
      if (target.id === msg.author.id) {
        return msg.reply("Use `!opselfgen` for self-generated opponent sales.");
      }

      await recordOpSetSale(guildId, msg.author.id, target.id);

      const closerName = msg.member?.displayName || msg.author.username;
      const setterName = await displayNameFor(msg.guild, target.id);

      return msg.reply(
        `🔥 Opponent sale recorded. +1 sale to ${closerName} & ${setterName}. Set credited to ${setterName}.`
      );
    }

    if (command === "opselfgen") {
      if (!canUseOpponent(msg.member)) return;

      await recordOpSelfGen(guildId, msg.author.id);
      const name = msg.member?.displayName || msg.author.username;
      return msg.reply(`🔥 Opponent self-gen recorded for ${name}.`);
    }

    if (command === "opsales") {
      if (!canUseOpponent(msg.member)) return;

      const rows = await all(
        `SELECT user_id, total_sales, self_gen, set_sales
         FROM op_sales
         WHERE guild_id = ?
         ORDER BY total_sales DESC, self_gen DESC, set_sales DESC`,
        [guildId]
      );

      if (!rows.length) {
        return msg.reply("**Opponent Sales Leaderboard**\n(No sales yet)");
      }

      const lines = [];
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const name = await displayNameFor(msg.guild, r.user_id);
        lines.push(
          `${i + 1}. ${name}: ${r.total_sales} sale${r.total_sales === 1 ? "" : "s"} (Self-gen: ${r.self_gen || 0}, Set: ${r.set_sales || 0})`
        );
      }

      return msg.reply(`**Opponent Sales Leaderboard**\n${lines.join("\n")}`.slice(0, 1900));
    }

    if (command === "clearopsales") {
      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership can use this command");
      }

      await run(`DELETE FROM op_sales WHERE guild_id = ?`, [guildId]);
      return msg.reply("🧹 Opponent sales leaderboard cleared.");
    }

if (command === "compsales") {
  const ourSales = await all(
    `SELECT user_id, total_sales, self_gen, set_sales
     FROM sales
     WHERE guild_id = ?
     ORDER BY total_sales DESC, self_gen DESC, set_sales DESC`,
    [guildId]
  );

  const opSales = await all(
    `SELECT user_id, total_sales, self_gen, set_sales
     FROM op_sales
     WHERE guild_id = ?
     ORDER BY total_sales DESC, self_gen DESC, set_sales DESC`,
    [guildId]
  );

  let output = "**🔥 Competition Sales Leaderboard**\n\n";

  output += "**Solrite Team**\n";
  if (!ourSales.length) {
    output += "(No sales yet)\n";
  } else {
    for (let i = 0; i < ourSales.length; i++) {
      const r = ourSales[i];
      const name = await displayNameFor(msg.guild, r.user_id);
      const t = r.total_sales || 0;
      output += `${i + 1}. ${name}: ${t} sale${t === 1 ? "" : "s"} (Self-gen: ${r.self_gen || 0}, Set: ${r.set_sales || 0})\n`;
    }
  }

  output += "\n**Opponent Team**\n";
  if (!opSales.length) {
    output += "(No sales yet)";
  } else {
    for (let i = 0; i < opSales.length; i++) {
      const r = opSales[i];
      const name = await displayNameFor(msg.guild, r.user_id);
      const t = r.total_sales || 0;
      output += `${i + 1}. ${name}: ${t} sale${t === 1 ? "" : "s"} (Self-gen: ${r.self_gen || 0}, Set: ${r.set_sales || 0})\n`;
    }
  }

  return msg.reply(output.slice(0, 1900));
}

    /* ===== OUR APPTS ===== */

    if (command === "setappt") {
      const dateKey = ctDateKey();
      const newCount = await addDailyAppt(guildId, msg.author.id, dateKey, +1);

      const active = await getActiveBlitz(guildId);
      if (active) {
        await addBlitzAppt(guildId, active.blitz_name, dateKey, msg.author.id, +1);
      }

      await logEvent(guildId, "appt", msg.author.id, 1);
      await refreshLiveBoards(msg.guild);
      await evaluateApptMoments(msg.guild, guildId, msg.author.id);

      const name = msg.member?.displayName || msg.author.username;
      return msg.reply(`✅ Appointment added for ${name}. Today: ${newCount}`);
    }

    if (command === "appts") {
      const dateKey = ctDateKey();
      const rows = await dailyApptsLeaderboard(guildId, dateKey);

      const header = `📅 Daily Appointments — ${dateKey} (CT)`;
      if (!rows.length) {
        return msg.reply(`${header}\n(No appointments yet today.)`);
      }

      const lines = [];
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const name = await displayNameFor(msg.guild, r.user_id);
        lines.push(`${i + 1}. ${name} — ${r.count}`);
      }

      return msg.reply(`${header}\n${lines.join("\n")}`.slice(0, 1900));
    }

    if (command === "cleardailyappts") {
      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership can use `!cleardailyappts`.");
      }

      const dateKey = ctDateKey();
      await clearDailyAppts(guildId, dateKey);
      await refreshLiveBoards(msg.guild);
      return msg.reply("🧹 Daily appointments cleared for today (CT).");
    }

    if (command === "removeappt") {
      const dateKey = ctDateKey();
      const mentioned = msg.mentions.users.first();
      const targetUserId = mentioned ? mentioned.id : msg.author.id;

      if (mentioned && !isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership can use `!removeappt @user`.");
      }

      const newCount = await addDailyAppt(guildId, targetUserId, dateKey, -1);

      const active = await getActiveBlitz(guildId);
      if (active) {
        await addBlitzAppt(guildId, active.blitz_name, dateKey, targetUserId, -1);
      }

      await refreshLiveBoards(msg.guild);

      const name = mentioned
        ? await displayNameFor(msg.guild, targetUserId)
        : (msg.member?.displayName || msg.author.username);

      return msg.reply(`✅ Removed 1 appointment from ${name}. Today: ${newCount}`);
    }

    /* ===== OPPONENT APPTS ===== */

    if (command === "opsetappt") {
      if (!canUseOpponent(msg.member)) return;

      const dateKey = ctDateKey();
      const newCount = await addOpDailyAppt(guildId, msg.author.id, dateKey, +1);

      const name = msg.member?.displayName || msg.author.username;
      return msg.reply(`Opponent appointment added for ${name}. Today: ${newCount}`);
    }

    if (command === "opappts") {
      if (!canUseOpponent(msg.member)) return;

      const dateKey = ctDateKey();
      const rows = await opDailyApptsLeaderboard(guildId, dateKey);

      const header = `Opponent Appointments — ${dateKey}`;
      if (!rows.length) {
        return msg.reply(`${header}\n(No appointments today)`);
      }

      const lines = [];
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const name = await displayNameFor(msg.guild, r.user_id);
        lines.push(`${i + 1}. ${name} — ${r.count}`);
      }

      return msg.reply(`${header}\n${lines.join("\n")}`.slice(0, 1900));
    }

    if (command === "clearopappts") {
      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership can use this command");
      }

      const dateKey = ctDateKey();
      await run(
        `DELETE FROM op_daily_appts
         WHERE guild_id = ? AND date_key = ?`,
        [guildId, dateKey]
      );

      return msg.reply("🧹 Opponent daily appointments cleared.");
    }

    if (command === "compappts") {
      const dateKey = ctDateKey();

      const ourAppts = await all(
        `SELECT user_id, count
         FROM daily_appts
         WHERE guild_id = ? AND date_key = ?
         ORDER BY count DESC`,
        [guildId, dateKey]
      );

      const opAppts = await all(
        `SELECT user_id, count
         FROM op_daily_appts
         WHERE guild_id = ? AND date_key = ?
         ORDER BY count DESC`,
        [guildId, dateKey]
      );

      let output = `📅 Competition Appointments — ${dateKey}\n\n`;

      output += "**Solrite Team**\n";
      if (!ourAppts.length) {
        output += "(No appointments)\n";
      } else {
        for (let i = 0; i < ourAppts.length; i++) {
          const r = ourAppts[i];
          const name = await displayNameFor(msg.guild, r.user_id);
          output += `${i + 1}. ${name} — ${r.count}\n`;
        }
      }

      output += "\n**Opponent Team**\n";
      if (!opAppts.length) {
        output += "(No appointments)";
      } else {
        for (let i = 0; i < opAppts.length; i++) {
          const r = opAppts[i];
          const name = await displayNameFor(msg.guild, r.user_id);
          output += `${i + 1}. ${name} — ${r.count}\n`;
        }
      }

      return msg.reply(output.slice(0, 1900));
    }

    if (command === "comp") {
      const dateKey = ctDateKey();

      const ourSalesRows = await all(
        `SELECT total_sales, self_gen, set_sales
         FROM sales
         WHERE guild_id = ?`,
        [guildId]
      );

      const opSalesRows = await all(
        `SELECT total_sales, self_gen, set_sales
         FROM op_sales
         WHERE guild_id = ?`,
        [guildId]
      );

      const ourApptRows = await all(
        `SELECT count
         FROM daily_appts
         WHERE guild_id = ? AND date_key = ?`,
        [guildId, dateKey]
      );

      const opApptRows = await all(
        `SELECT count
         FROM op_daily_appts
         WHERE guild_id = ? AND date_key = ?`,
        [guildId, dateKey]
      );

      const ourSalesTotal = ourSalesRows.reduce(
        (sum, r) => sum + ((r.total_sales || 0) - (r.set_sales || 0) + (r.self_gen || 0)),
        0
      );

      const opSalesTotal = opSalesRows.reduce(
        (sum, r) => sum + ((r.total_sales || 0) - (r.set_sales || 0) + (r.self_gen || 0)),
        0
      );

      const ourApptsTotal = ourApptRows.reduce((sum, r) => sum + (r.count || 0), 0);
      const opApptsTotal = opApptRows.reduce((sum, r) => sum + (r.count || 0), 0);

const output = `🔥 **Blitz Score**

**Solrite**
Sales: ${ourSalesTotal}
Appts: ${ourApptsTotal}

**Opponent**
Sales: ${opSalesTotal}
Appts: ${opApptsTotal}`;

return msg.reply(output);
    }

    /* ===== BLITZ ===== */

    if (command === "startappts") {
      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership can use `!startappts`.");
      }

      const blitzName = (parts.join(" ") || "").trim();
      if (!blitzName) return msg.reply("Usage: `!startappts <blitz_name>`");

      const result = await startBlitz(guildId, blitzName);
      if (!result.ok && result.reason === "exists") {
        return msg.reply(`⚠️ A blitz named **${blitzName}** already exists. Please choose a unique name.`);
      }
      if (!result.ok && result.reason === "active") {
        return msg.reply(`⚠️ A blitz is already active: **${result.activeName}**. Use \`!endappts\` first.`);
      }

      return msg.reply(`🟢 Blitz appointments started: **${blitzName}** (Starts: ${ctTimestampString()} CT)`);
    }

    if (command === "endappts") {
      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership can use `!endappts`.");
      }

      const result = await endBlitz(guildId);
      if (!result.ok) return msg.reply("⚠️ No active blitz to end.");

      return msg.reply(
        `🔴 Blitz appointments ended: **${result.blitzName}** (Ended: ${ctTimestampString()} CT)\nUse \`!blitzappts\` to view results.`
      );
    }

if (command === "blitzappts") {
  const argName = (parts.join(" ") || "").trim();

  let blitz = null;
  if (argName) {
    const exists = await blitzExists(guildId, argName);
    if (!exists) return msg.reply(`⚠️ No blitz found with ID: **${argName}**`);
    blitz = { blitz_name: argName };
  } else {
    blitz = await getActiveBlitz(guildId);
    if (!blitz) blitz = await getMostRecentEndedBlitz(guildId);
    if (!blitz) return msg.reply("⚠️ No blitz data found yet.");
  }

  const active = await getActiveBlitz(guildId);
  const isActive = active && active.blitz_name === blitz.blitz_name;

  const rows = await blitzApptsByDate(guildId, blitz.blitz_name);
  const title = `📊 Blitz Appointments — **${blitz.blitz_name}** (${isActive ? "ACTIVE" : "ENDED"})`;

  if (!rows.length) {
    return msg.reply(`${title}\n(No appointments recorded for this blitz.)`);
  }

  const byDate = new Map();
  for (const r of rows) {
    if (!byDate.has(r.date_key)) byDate.set(r.date_key, []);
    byDate.get(r.date_key).push(r);
  }

  const chunks = [];
  let currentChunk = `${title}\n`;
  const maxLen = 1900;

  const dates = Array.from(byDate.keys()).sort();

  for (const dateKey of dates) {
    const list = byDate
      .get(dateKey)
      .slice()
      .sort((a, b) => (b.count || 0) - (a.count || 0));

    let section = `\n**${dateKey} (CT)**\n`;

    for (let i = 0; i < list.length; i++) {
      const r = list[i];
      const name = await displayNameFor(msg.guild, r.user_id);
      section += `${i + 1}. ${name} — ${r.count}\n`;
    }

    if ((currentChunk + section).length > maxLen) {
      chunks.push(currentChunk.trimEnd());
      currentChunk = section.trimStart();
    } else {
      currentChunk += section;
    }
  }

  if (currentChunk.trim()) {
    chunks.push(currentChunk.trimEnd());
  }

  await msg.reply(chunks[0]);

  for (let i = 1; i < chunks.length; i++) {
    await msg.channel.send(chunks[i]);
  }

  return;
}

    if (command === "clearblitzappts") {
      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership can use `!clearblitzappts`.");
      }

      const result = await clearBlitzApptsTarget(guildId);
      if (!result.ok) return msg.reply("⚠️ No blitz data to clear.");

      if (result.mode === "active") {
        return msg.reply(`🧹 Cleared blitz appointments for active blitz: **${result.blitzName}**`);
      }

      return msg.reply(`🧹 Cleared blitz appointments for most recent ended blitz: **${result.blitzName}**`);
    }

    /* ===== GYM ===== */

    if (command === "gym") {
      const mentioned = msg.mentions.users.first();

      if (!mentioned) {
        const next = await addGymDelta(guildId, msg.author.id, +1);
        const name = msg.member?.displayName || msg.author.username;
        return msg.reply(`🏋️ Gym check-in logged for ${name}. Total: ${next}`);
      }

      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership can use `!gym @user`.");
      }

      const maybeNum = parts.find((p) => /^[0-9]+$/.test(p));
      const amount = parsePositiveInt(maybeNum) ?? 1;

      const next = await addGymDelta(guildId, mentioned.id, +amount);
      const targetName = await displayNameFor(msg.guild, mentioned.id);

      return msg.reply(
        `✅ Added ${amount} gym check-in${amount === 1 ? "" : "s"} to ${targetName}. Total: ${next}`
      );
    }

    if (command === "removegym") {
      const mentioned = msg.mentions.users.first();

      if (!mentioned) {
        const maybeNum = parts.find((p) => /^[0-9]+$/.test(p));
        const amount = parsePositiveInt(maybeNum) ?? 1;

        const next = await addGymDelta(guildId, msg.author.id, -amount);
        const name = msg.member?.displayName || msg.author.username;

        return msg.reply(
          `✅ Removed ${amount} gym check-in${amount === 1 ? "" : "s"} from ${name}. Total: ${next}`
        );
      }

      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership can use `!removegym @user`.");
      }

      const maybeNum = parts.find((p) => /^[0-9]+$/.test(p));
      const amount = parsePositiveInt(maybeNum) ?? 1;

      const next = await addGymDelta(guildId, mentioned.id, -amount);
      const targetName = await displayNameFor(msg.guild, mentioned.id);

      return msg.reply(
        `✅ Removed ${amount} gym check-in${amount === 1 ? "" : "s"} from ${targetName}. Total: ${next}`
      );
    }

    if (command === "gymrank") {
      const rows = await all(
        `SELECT user_id, checkins
         FROM gym
         WHERE guild_id = ?
         ORDER BY checkins DESC`,
        [guildId]
      );

      if (!rows.length) {
        return msg.reply("**Gym Leaderboard**\n(No check-ins yet.)");
      }

      const lines = [];
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const name = await displayNameFor(msg.guild, r.user_id);
        lines.push(`${i + 1}. ${name}: ${r.checkins} check-ins`);
      }

      return msg.reply(`**Gym Leaderboard**\n${lines.join("\n")}`.slice(0, 1900));
    }

    if (command === "cleargym") {
      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership can use `!cleargym`.");
      }

      await run(`DELETE FROM gym WHERE guild_id = ?`, [guildId]);
      return msg.reply("🧹 Gym leaderboard cleared.");
    }

    return;
  } catch (err) {
    console.error("Command error:", err);
    try {
      return msg.reply("⚠️ Something went wrong running that command.");
    } catch {}
  }
});

/* ================= READY ================= */

client.once("clientReady", async () => {
  console.log(`✅ Logged in as ${client.user.tag}`);

  for (const guild of client.guilds.cache.values()) {
    try {
      await refreshLiveBoards(guild);
    } catch (err) {
      console.error("Live board init error:", err);
    }
  }
});

/* ================= START ================= */

(async () => {
  await initDb();
  await client.login(TOKEN);
})();
