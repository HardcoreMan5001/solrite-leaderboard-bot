require("dotenv").config();
const { Client, GatewayIntentBits } = require("discord.js");
const sqlite3 = require("sqlite3").verbose();

// ===== CONFIG =====
const PREFIX = "!";
const TIMEZONE = "America/Chicago"; // Central Time
const LEADERSHIP_ROLES = ["Leadership"]; // adjust if needed
const CLOSER_ROLES = ["Closer"]; // adjust if needed

const TOKEN = process.env.DISCORD_TOKEN;
if (!TOKEN) {
  console.error("❌ Missing DISCORD_TOKEN environment variable.");
  process.exit(1);
}

// Keep same DB file name so gym data persists when storage persists
const DB_PATH = process.env.DB_PATH || "./bot.db";

// ===== DISCORD =====
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
});

// ===== DB =====
const db = new sqlite3.Database(DB_PATH);

// Promisified helpers
function run(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
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

// ===== TIME HELPERS (Central Time) =====
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
  return `${y}-${m}-${d}`; // YYYY-MM-DD
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

// ===== PERMISSIONS =====
function hasRole(member, roleNames) {
  if (!member?.roles?.cache) return false;
  const lower = roleNames.map((r) => r.toLowerCase());
  return member.roles.cache.some((r) => lower.includes(r.name.toLowerCase()));
}

function isLeadership(member) {
  if (!member) return false;
  if (member.permissions?.has?.("Administrator")) return true;
  return hasRole(member, LEADERSHIP_ROLES);
}

function canSetSale(member) {
  if (!member) return false;
  if (isLeadership(member)) return true;
  return hasRole(member, CLOSER_ROLES);
}

// ===== DISPLAY NAME =====
async function displayNameFor(guild, userId) {
  try {
    const member = await guild.members.fetch(userId);
    return member.displayName || member.user.username;
  } catch {
    return `<@${userId}>`;
  }
}

// ===== INIT DB =====
async function initDb() {
  // SALES
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

  // migrate if older schema
  try {
    await run(`ALTER TABLE sales ADD COLUMN total_sales INTEGER NOT NULL DEFAULT 0`);
  } catch {}
  await run(`
    UPDATE sales
    SET total_sales = (self_gen + set_sales)
    WHERE total_sales = 0 AND (self_gen + set_sales) > 0
  `);

  // GYM (do not rename)
  await run(`
    CREATE TABLE IF NOT EXISTS gym (
      guild_id TEXT NOT NULL,
      user_id  TEXT NOT NULL,
      checkins INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (guild_id, user_id)
    )
  `);

  // DAILY APPTS
  await run(`
    CREATE TABLE IF NOT EXISTS daily_appts (
      guild_id TEXT NOT NULL,
      date_key TEXT NOT NULL,
      user_id  TEXT NOT NULL,
      count    INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (guild_id, date_key, user_id)
    )
  `);

  // BLITZ META
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

  // BLITZ APPTS (by day)
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
}

// ===== SALES HELPERS =====
async function ensureSalesRow(guildId, userId) {
  await run(
    `INSERT OR IGNORE INTO sales (guild_id, user_id, total_sales, self_gen, set_sales)
     VALUES (?, ?, 0, 0, 0)`,
    [guildId, userId]
  );
}

// !setsale @setter
// - both get +1 total sale
// - mentioned user gets +1 set
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

// !selfgen
// - +1 sale, +1 self-gen, +1 set for sender
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

// ===== GYM HELPERS =====
async function ensureGymRow(guildId, userId) {
  await run(
    `INSERT OR IGNORE INTO gym (guild_id, user_id, checkins) VALUES (?, ?, 0)`,
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

// ===== APPT HELPERS =====
async function addDailyAppt(guildId, userId, dateKey, delta) {
  await run(
    `INSERT OR IGNORE INTO daily_appts (guild_id, date_key, user_id, count) VALUES (?, ?, ?, 0)`,
    [guildId, dateKey, userId]
  );

  const row = await get(
    `SELECT count FROM daily_appts WHERE guild_id = ? AND date_key = ? AND user_id = ?`,
    [guildId, dateKey, userId]
  );
  const current = row?.count ?? 0;
  const next = Math.max(0, current + delta);

  await run(
    `UPDATE daily_appts SET count = ? WHERE guild_id = ? AND date_key = ? AND user_id = ?`,
    [next, guildId, dateKey, userId]
  );

  return next;
}

async function dailyApptsLeaderboard(guildId, dateKey) {
  return await all(
    `SELECT user_id, count
     FROM daily_appts
     WHERE guild_id = ? AND date_key = ?
     ORDER BY count DESC`,
    [guildId, dateKey]
  );
}

async function clearDailyAppts(guildId, dateKey) {
  await run(`DELETE FROM daily_appts WHERE guild_id = ? AND date_key = ?`, [
    guildId,
    dateKey,
  ]);
}

// ---- Blitz meta
async function getActiveBlitz(guildId) {
  return await get(
    `SELECT blitz_name, start_ts, end_ts, is_active
     FROM appt_blitz
     WHERE guild_id = ? AND is_active = 1
     LIMIT 1`,
    [guildId]
  );
}

async function getMostRecentEndedBlitz(guildId) {
  return await get(
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
    `SELECT blitz_name FROM appt_blitz WHERE guild_id = ? AND blitz_name = ? LIMIT 1`,
    [guildId, blitzName]
  );
  return !!row;
}

async function startBlitz(guildId, blitzName) {
  if (await blitzExists(guildId, blitzName)) return { ok: false, reason: "exists" };

  const active = await getActiveBlitz(guildId);
  if (active) return { ok: false, reason: "active", activeName: active.blitz_name };

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

// ---- Blitz appts
async function addBlitzAppt(guildId, blitzName, dateKey, userId, delta) {
  await run(
    `INSERT OR IGNORE INTO blitz_appts (guild_id, blitz_name, date_key, user_id, count)
     VALUES (?, ?, ?, ?, 0)`,
    [guildId, blitzName, dateKey, userId]
  );

  const row = await get(
    `SELECT count FROM blitz_appts
     WHERE guild_id = ? AND blitz_name = ? AND date_key = ? AND user_id = ?`,
    [guildId, blitzName, dateKey, userId]
  );
  const current = row?.count ?? 0;
  const next = Math.max(0, current + delta);

  await run(
    `UPDATE blitz_appts SET count = ?
     WHERE guild_id = ? AND blitz_name = ? AND date_key = ? AND user_id = ?`,
    [next, guildId, blitzName, dateKey, userId]
  );

  return next;
}

async function blitzApptsByDate(guildId, blitzName) {
  return await all(
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
    await run(`DELETE FROM blitz_appts WHERE guild_id = ? AND blitz_name = ?`, [
      guildId,
      active.blitz_name,
    ]);
    return { ok: true, blitzName: active.blitz_name, mode: "active" };
  }

  const recent = await getMostRecentEndedBlitz(guildId);
  if (recent) {
    await run(`DELETE FROM blitz_appts WHERE guild_id = ? AND blitz_name = ?`, [
      guildId,
      recent.blitz_name,
    ]);
    return { ok: true, blitzName: recent.blitz_name, mode: "recent" };
  }

  return { ok: false, reason: "none" };
}

// ===== COMMAND PARSING HELPERS =====
function parsePositiveInt(s) {
  if (!s) return null;
  const n = Number(s);
  if (!Number.isInteger(n)) return null;
  if (n <= 0) return null;
  return n;
}

function parseNonNegativeInt(s) {
  if (!s) return null;
  const n = Number(s);
  if (!Number.isInteger(n)) return null;
  if (n < 0) return null;
  return n;
}

// ===== MESSAGE HANDLER =====
client.on("messageCreate", async (msg) => {
  try {
    if (!msg.guild) return;
    if (msg.author.bot) return;

    const content = (msg.content || "").trim();
    if (!content.startsWith(PREFIX)) return;

    const parts = content.slice(PREFIX.length).trim().split(/\s+/);
    const command = (parts.shift() || "").toLowerCase();
    const guildId = msg.guild.id;

    // ===================== SALES =====================
    if (command === "setsale") {
      if (!canSetSale(msg.member)) {
        return msg.reply("❌ Only Leadership/Closer can use `!setsale`.");
      }

      const target = msg.mentions.users.first();
      if (!target) return msg.reply("Usage: `!setsale @user`");
      if (target.id === msg.author.id) return msg.reply("Use `!selfgen` for self-generated sales.");

      await recordSetSale(guildId, msg.author.id, target.id);

      const closerName = msg.member?.displayName || msg.author.username;
      const setterName = await displayNameFor(msg.guild, target.id);

      return msg.reply(
        `✅ Sale recorded. +1 sale to ${closerName} & ${setterName}. Set credited to ${setterName}.`
      );
    }

    if (command === "selfgen") {
      if (!canSetSale(msg.member)) {
        return msg.reply("❌ Only Leadership/Closer can use `!selfgen`.");
      }
      await recordSelfGen(guildId, msg.author.id);
      const name = msg.member?.displayName || msg.author.username;
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

      if (!rows.length) return msg.reply("**Sales Leaderboard**\n(No sales recorded yet.)");

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
      if (!isLeadership(msg.member)) return msg.reply("❌ Only Leadership can use `!clearsales`.");
      await run(`DELETE FROM sales WHERE guild_id = ?`, [guildId]);
      return msg.reply("🧹 Sales leaderboard cleared.");
    }

    // ===================== GYM =====================
    // !gym
    // - no mention: self +1
    // - mention: leadership adds to mentioned user (default +1, optional #)
    if (command === "gym") {
      const mentioned = msg.mentions.users.first();

      if (!mentioned) {
        // self +1
        const next = await addGymDelta(guildId, msg.author.id, +1);
        const name = msg.member?.displayName || msg.author.username;
        return msg.reply(`🏋️ Gym check-in logged for ${name}. Total: ${next}`);
      }

      // mention present => leadership-only admin add
      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership can use `!gym @user`.");
      }

      // number argument is the first token after mention (if any)
      const maybeNum = parts.find((p) => /^[0-9]+$/.test(p));
      const amount = parsePositiveInt(maybeNum) ?? 1;

      const next = await addGymDelta(guildId, mentioned.id, +amount);
      const targetName = await displayNameFor(msg.guild, mentioned.id);

      return msg.reply(`✅ Added ${amount} gym check-in${amount === 1 ? "" : "s"} to ${targetName}. Total: ${next}`);
    }

    // !removegym
    // - no mention: self remove (default 1, optional #), anyone can use
    // - mention: leadership remove from mentioned (default 1, optional #)
    if (command === "removegym") {
      const mentioned = msg.mentions.users.first();

      if (!mentioned) {
        // self remove
        const maybeNum = parts.find((p) => /^[0-9]+$/.test(p));
        const amount = parsePositiveInt(maybeNum) ?? 1;

        const next = await addGymDelta(guildId, msg.author.id, -amount);
        const name = msg.member?.displayName || msg.author.username;

        return msg.reply(`✅ Removed ${amount} gym check-in${amount === 1 ? "" : "s"} from ${name}. Total: ${next}`);
      }

      // remove from mentioned user (leadership-only)
      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership can use `!removegym @user`.");
      }

      const maybeNum = parts.find((p) => /^[0-9]+$/.test(p));
      const amount = parsePositiveInt(maybeNum) ?? 1;

      const next = await addGymDelta(guildId, mentioned.id, -amount);
      const targetName = await displayNameFor(msg.guild, mentioned.id);

      return msg.reply(`✅ Removed ${amount} gym check-in${amount === 1 ? "" : "s"} from ${targetName}. Total: ${next}`);
    }

    if (command === "gymrank") {
      const rows = await all(
        `SELECT user_id, checkins
         FROM gym
         WHERE guild_id = ?
         ORDER BY checkins DESC`,
        [guildId]
      );

      if (!rows.length) return msg.reply("**Gym Leaderboard**\n(No check-ins yet.)");

      const lines = [];
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const name = await displayNameFor(msg.guild, r.user_id);
        lines.push(`${i + 1}. ${name}: ${r.checkins} check-ins`);
      }

      return msg.reply(`**Gym Leaderboard**\n${lines.join("\n")}`.slice(0, 1900));
    }

    if (command === "cleargym") {
      if (!isLeadership(msg.member)) return msg.reply("❌ Only Leadership can use `!cleargym`.");
      await run(`DELETE FROM gym WHERE guild_id = ?`, [guildId]);
      return msg.reply("🧹 Gym leaderboard cleared.");
    }

    // ===================== DAILY APPTS =====================
    if (command === "setappt") {
      const dateKey = ctDateKey();
      const newCount = await addDailyAppt(guildId, msg.author.id, dateKey, +1);

      // also add to active blitz (if exists)
      const active = await getActiveBlitz(guildId);
      if (active) {
        await addBlitzAppt(guildId, active.blitz_name, dateKey, msg.author.id, +1);
      }

      const name = msg.member?.displayName || msg.author.username;
      return msg.reply(`✅ Appointment added for ${name}. Today: ${newCount}`);
    }

    if (command === "appts") {
      const dateKey = ctDateKey();
      const rows = await dailyApptsLeaderboard(guildId, dateKey);

      const header = `📅 Daily Appointments — ${dateKey} (CT)`;
      if (!rows.length) return msg.reply(`${header}\n(No appointments yet today.)`);

      const lines = [];
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const name = await displayNameFor(msg.guild, r.user_id);
        lines.push(`${i + 1}. ${name} — ${r.count}`);
      }

      return msg.reply(`${header}\n${lines.join("\n")}`.slice(0, 1900));
    }

    if (command === "cleardailyappts") {
      if (!isLeadership(msg.member)) return msg.reply("❌ Only Leadership can use `!cleardailyappts`.");
      const dateKey = ctDateKey();
      await clearDailyAppts(guildId, dateKey);
      return msg.reply("🧹 Daily appointments cleared for today (CT).");
    }

    // !removeappt or !removeappt @user
    if (command === "removeappt") {
      const dateKey = ctDateKey();

      const mentioned = msg.mentions.users.first();
      const targetUserId = mentioned ? mentioned.id : msg.author.id;

      // if mention is used, only leadership can do it
      if (mentioned && !isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership can use `!removeappt @user`.");
      }

      const newCount = await addDailyAppt(guildId, targetUserId, dateKey, -1);

      // also subtract from active blitz only (if exists)
      const active = await getActiveBlitz(guildId);
      if (active) {
        await addBlitzAppt(guildId, active.blitz_name, dateKey, targetUserId, -1);
      }

      const name = mentioned
        ? await displayNameFor(msg.guild, targetUserId)
        : (msg.member?.displayName || msg.author.username);

      return msg.reply(`✅ Removed 1 appointment from ${name}. Today: ${newCount}`);
    }

    // ===================== BLITZ APPTS =====================
    if (command === "startappts") {
      if (!isLeadership(msg.member)) return msg.reply("❌ Only Leadership can use `!startappts`.");
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
      if (!isLeadership(msg.member)) return msg.reply("❌ Only Leadership can use `!endappts`.");

      const result = await endBlitz(guildId);
      if (!result.ok) return msg.reply("⚠️ No active blitz to end.");

      return msg.reply(
        `🔴 Blitz appointments ended: **${result.blitzName}** (Ended: ${ctTimestampString()} CT)\nUse \`!blitzappts\` to view results.`
      );
    }

    // !blitzappts or !blitzappts <id>
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

      if (!rows.length) return msg.reply(`${title}\n(No appointments recorded for this blitz.)`);

      // Group by date_key
      const byDate = new Map();
      for (const r of rows) {
        if (!byDate.has(r.date_key)) byDate.set(r.date_key, []);
        byDate.get(r.date_key).push(r);
      }

      let out = `${title}\n`;
      const dates = Array.from(byDate.keys()).sort();
      for (const dateKey of dates) {
        const list = byDate.get(dateKey).slice().sort((a, b) => (b.count || 0) - (a.count || 0));
        out += `\n**${dateKey} (CT)**\n`;
        for (let i = 0; i < list.length; i++) {
          const r = list[i];
          const name = await displayNameFor(msg.guild, r.user_id);
          out += `${i + 1}. ${name} — ${r.count}\n`;
          if (out.length > 1850) {
            out += `\n…(truncated)\n`;
            return msg.reply(out);
          }
        }
      }

      return msg.reply(out.slice(0, 1900));
    }

    if (command === "clearblitzappts") {
      if (!isLeadership(msg.member)) return msg.reply("❌ Only Leadership can use `!clearblitzappts`.");

      const result = await clearBlitzApptsTarget(guildId);
      if (!result.ok) return msg.reply("⚠️ No blitz data to clear.");

      if (result.mode === "active") {
        return msg.reply(`🧹 Cleared blitz appointments for active blitz: **${result.blitzName}**`);
      }
      return msg.reply(`🧹 Cleared blitz appointments for most recent ended blitz: **${result.blitzName}**`);
    }

    // Unknown command: do nothing
    return;
  } catch (err) {
    console.error("Command error:", err);
    try {
      return msg.reply("⚠️ Something went wrong running that command.");
    } catch {}
  }
});

client.once("ready", async () => {
  console.log(`✅ Logged in as ${client.user.tag}`);
});

(async () => {
  await initDb();
  await client.login(TOKEN);
})();
