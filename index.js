require("dotenv").config();
const { Client, GatewayIntentBits } = require("discord.js");
const sqlite3 = require("sqlite3").verbose();

/* ================= CONFIG ================= */

const PREFIX = "!";

const ROLE_IDS = {
  leadership: "1458245230598946940",
  admin: "1458245454482640966",
  rep: "1458245642026750178",
  closer: "1458245812827062342",
  bot: "1458969879406444751",
  recruit: "1479313032961064970",
  opponent: "1479314578642042964",
};

const CHANNELS = {
  appointments: "1458250231354495150",
  general: "1458248543000068228",
  sales: "1458250404835098795",
  liveScoreboard: "1479273397698564179",
  compSales: "1479295628528844921",
  compAppts: "1479308152951410768",
};

const TIMEZONE = "America/Chicago";

const TOKEN = process.env.DISCORD_TOKEN;
const DB_PATH = process.env.DB_PATH || "./bot.db";

/* ================= CLIENT ================= */

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
});

/* ================= DATABASE ================= */

const db = new sqlite3.Database(DB_PATH);

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

/* ================= PERMISSIONS ================= */

function permissionDenied(msg) {
  msg.reply("You do not have role permissions to use this command");
}

function hasRole(member, roleId) {
  return member.roles.cache.has(roleId);
}

function isLeadership(member) {
  return (
    hasRole(member, ROLE_IDS.leadership) ||
    hasRole(member, ROLE_IDS.admin)
  );
}

function canClose(member) {
  return (
    hasRole(member, ROLE_IDS.closer) ||
    isLeadership(member)
  );
}

function canOpponent(member) {
  return (
    hasRole(member, ROLE_IDS.opponent) ||
    isLeadership(member)
  );
}

/* ================= TIME HELPERS ================= */

function ctNow() {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: TIMEZONE,
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date());
}

function ctDateKey() {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());

  const y = parts.find(p => p.type === "year").value;
  const m = parts.find(p => p.type === "month").value;
  const d = parts.find(p => p.type === "day").value;

  return `${y}-${m}-${d}`;
}

/* ================= PROGRESS BAR ================= */

function progressBar(current, goal) {

  const width = 10;

  if (!goal) return "░░░░░░░░░░ 0%";

  const percent = Math.min(current / goal, 1);

  const filled = Math.round(percent * width);

  const bar =
    "█".repeat(filled) +
    "░".repeat(width - filled);

  const pct = Math.round(percent * 100);

  return `${bar} ${pct}%`;

}

/* ================= INIT DB ================= */

async function initDb() {

  await run(`
  CREATE TABLE IF NOT EXISTS sales (
    guild_id TEXT,
    user_id TEXT,
    total_sales INTEGER DEFAULT 0,
    self_gen INTEGER DEFAULT 0,
    set_sales INTEGER DEFAULT 0,
    PRIMARY KEY (guild_id, user_id)
  )`);

  await run(`
  CREATE TABLE IF NOT EXISTS opponent_sales (
    guild_id TEXT,
    user_id TEXT,
    total_sales INTEGER DEFAULT 0,
    self_gen INTEGER DEFAULT 0,
    set_sales INTEGER DEFAULT 0,
    PRIMARY KEY (guild_id, user_id)
  )`);

  await run(`
  CREATE TABLE IF NOT EXISTS daily_appts (
    guild_id TEXT,
    date_key TEXT,
    user_id TEXT,
    count INTEGER DEFAULT 0,
    PRIMARY KEY (guild_id, date_key, user_id)
  )`);

  await run(`
  CREATE TABLE IF NOT EXISTS opponent_appts (
    guild_id TEXT,
    date_key TEXT,
    user_id TEXT,
    count INTEGER DEFAULT 0,
    PRIMARY KEY (guild_id, date_key, user_id)
  )`);

  await run(`
  CREATE TABLE IF NOT EXISTS gym (
    guild_id TEXT,
    user_id TEXT,
    checkins INTEGER DEFAULT 0,
    PRIMARY KEY (guild_id, user_id)
  )`);

  await run(`
  CREATE TABLE IF NOT EXISTS goals (
    guild_id TEXT PRIMARY KEY,
    goal INTEGER DEFAULT 0
  )`);

}
/* ================= DISPLAY HELPERS ================= */

async function displayNameFor(guild, userId) {
  try {
    const member = await guild.members.fetch(userId);
    return member.displayName || member.user.username;
  } catch {
    return `<@${userId}>`;
  }
}

async function sendChunked(channel, text, replyTo = null) {
  const limit = 1900;
  const chunks = [];

  let remaining = text;
  while (remaining.length > limit) {
    let cut = remaining.lastIndexOf("\n", limit);
    if (cut === -1) cut = limit;
    chunks.push(remaining.slice(0, cut));
    remaining = remaining.slice(cut).trimStart();
  }
  if (remaining.length) chunks.push(remaining);

  for (let i = 0; i < chunks.length; i++) {
    if (i === 0 && replyTo) {
      await replyTo.reply(chunks[i]);
    } else {
      await channel.send(chunks[i]);
    }
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

async function ensureOpponentSalesRow(guildId, userId) {
  await run(
    `INSERT OR IGNORE INTO opponent_sales (guild_id, user_id, total_sales, self_gen, set_sales)
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
    `UPDATE sales
     SET total_sales = total_sales + 1,
         set_sales = set_sales + 1
     WHERE guild_id = ? AND user_id = ?`,
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

async function recordOpponentSetSale(guildId, closerId, setterId) {
  await ensureOpponentSalesRow(guildId, closerId);
  await ensureOpponentSalesRow(guildId, setterId);

  await run(
    `UPDATE opponent_sales SET total_sales = total_sales + 1 WHERE guild_id = ? AND user_id = ?`,
    [guildId, closerId]
  );
  await run(
    `UPDATE opponent_sales
     SET total_sales = total_sales + 1,
         set_sales = set_sales + 1
     WHERE guild_id = ? AND user_id = ?`,
    [guildId, setterId]
  );
}

async function recordOpponentSelfGen(guildId, userId) {
  await ensureOpponentSalesRow(guildId, userId);
  await run(
    `UPDATE opponent_sales
     SET total_sales = total_sales + 1,
         self_gen = self_gen + 1,
         set_sales = set_sales + 1
     WHERE guild_id = ? AND user_id = ?`,
    [guildId, userId]
  );
}

async function salesRows(guildId) {
  return await all(
    `SELECT user_id, total_sales, self_gen, set_sales
     FROM sales
     WHERE guild_id = ?
     ORDER BY total_sales DESC, self_gen DESC, set_sales DESC, user_id ASC`,
    [guildId]
  );
}

async function opponentSalesRows(guildId) {
  return await all(
    `SELECT user_id, total_sales, self_gen, set_sales
     FROM opponent_sales
     WHERE guild_id = ?
     ORDER BY total_sales DESC, self_gen DESC, set_sales DESC, user_id ASC`,
    [guildId]
  );
}

async function buildSalesLeaderboard(guild) {
  const rows = await salesRows(guild.id);
  if (!rows.length) return "**Sales Leaderboard**\n(No sales recorded yet.)";

  const lines = [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const name = await displayNameFor(guild, r.user_id);
    const t = r.total_sales || 0;
    lines.push(
      `${i + 1}. ${name} — ${t} sale${t === 1 ? "" : "s"}\n   Self-gen: ${r.self_gen || 0} | Set: ${r.set_sales || 0}`
    );
  }

  return `**Sales Leaderboard**\n${lines.join("\n\n")}`;
}

async function buildOpponentSalesLeaderboard(guild) {
  const rows = await opponentSalesRows(guild.id);
  if (!rows.length) return "**Sales Leaderboard — Opponent**\n(No sales recorded yet.)";

  const lines = [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const name = await displayNameFor(guild, r.user_id);
    const t = r.total_sales || 0;
    lines.push(
      `${i + 1}. ${name} — ${t} sale${t === 1 ? "" : "s"}\n   Self-gen: ${r.self_gen || 0} | Set: ${r.set_sales || 0}`
    );
  }

  return `**Sales Leaderboard — Opponent**\n${lines.join("\n\n")}`;
}

/* ================= GYM HELPERS ================= */

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

async function buildGymLeaderboard(guild) {
  const rows = await all(
    `SELECT user_id, checkins
     FROM gym
     WHERE guild_id = ?
     ORDER BY checkins DESC, user_id ASC`,
    [guild.id]
  );

  if (!rows.length) return "**Gym Leaderboard**\n(No check-ins yet.)";

  const lines = [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const name = await displayNameFor(guild, r.user_id);
    lines.push(`${i + 1}. ${name}: ${r.checkins} check-ins`);
  }

  return `**Gym Leaderboard**\n${lines.join("\n")}`;
}

/* ================= DAILY APPOINTMENT HELPERS ================= */

async function addDailyAppt(guildId, userId, dateKey, delta) {
  await run(
    `INSERT OR IGNORE INTO daily_appts (guild_id, date_key, user_id, count)
     VALUES (?, ?, ?, 0)`,
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

async function addOpponentDailyAppt(guildId, userId, dateKey, delta) {
  await run(
    `INSERT OR IGNORE INTO opponent_appts (guild_id, date_key, user_id, count)
     VALUES (?, ?, ?, 0)`,
    [guildId, dateKey, userId]
  );

  const row = await get(
    `SELECT count FROM opponent_appts WHERE guild_id = ? AND date_key = ? AND user_id = ?`,
    [guildId, dateKey, userId]
  );
  const current = row?.count ?? 0;
  const next = Math.max(0, current + delta);

  await run(
    `UPDATE opponent_appts SET count = ? WHERE guild_id = ? AND date_key = ? AND user_id = ?`,
    [next, guildId, dateKey, userId]
  );

  return next;
}

async function dailyApptsRows(guildId, dateKey) {
  return await all(
    `SELECT user_id, count
     FROM daily_appts
     WHERE guild_id = ? AND date_key = ?
     ORDER BY count DESC, user_id ASC`,
    [guildId, dateKey]
  );
}

async function opponentDailyApptsRows(guildId, dateKey) {
  return await all(
    `SELECT user_id, count
     FROM opponent_appts
     WHERE guild_id = ? AND date_key = ?
     ORDER BY count DESC, user_id ASC`,
    [guildId, dateKey]
  );
}

async function clearDailyAppts(guildId, dateKey) {
  await run(`DELETE FROM daily_appts WHERE guild_id = ? AND date_key = ?`, [
    guildId,
    dateKey,
  ]);
}

async function clearOpponentDailyAppts(guildId, dateKey) {
  await run(`DELETE FROM opponent_appts WHERE guild_id = ? AND date_key = ?`, [
    guildId,
    dateKey,
  ]);
}

async function buildDailyApptsLeaderboard(guild, teamLabel = null) {
  const dateKey = ctDateKey();
  const rows = await dailyApptsRows(guild.id, dateKey);
  const header = teamLabel
    ? `📅 Daily Appointments — ${teamLabel} — ${dateKey} (CT)`
    : `📅 Daily Appointments — ${dateKey} (CT)`;

  if (!rows.length) return `${header}\n(No appointments yet today.)`;

  const lines = [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const name = await displayNameFor(guild, r.user_id);
    lines.push(`${i + 1}. ${name} — ${r.count}`);
  }

  return `${header}\n${lines.join("\n")}`;
}

async function buildOpponentDailyApptsLeaderboard(guild, teamLabel = "Opponent") {
  const dateKey = ctDateKey();
  const rows = await opponentDailyApptsRows(guild.id, dateKey);
  const header = `📅 Daily Appointments — ${teamLabel} — ${dateKey} (CT)`;

  if (!rows.length) return `${header}\n(No appointments yet today.)`;

  const lines = [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const name = await displayNameFor(guild, r.user_id);
    lines.push(`${i + 1}. ${name} — ${r.count}`);
  }

  return `${header}\n${lines.join("\n")}`;
}

/* ================= GOAL HELPERS ================= */

async function getGoal(guildId) {
  const row = await get(`SELECT goal FROM goals WHERE guild_id = ?`, [guildId]);
  return row?.goal ?? 0;
}

async function setGoal(guildId, goal) {
  await run(
    `INSERT INTO goals (guild_id, goal) VALUES (?, ?)
     ON CONFLICT(guild_id) DO UPDATE SET goal = excluded.goal`,
    [guildId, goal]
  );
}

async function todayApptTotal(guildId) {
  const row = await get(
    `SELECT SUM(count) AS total FROM daily_appts WHERE guild_id = ? AND date_key = ?`,
    [guildId, ctDateKey()]
  );
  return row?.total ?? 0;
}

async function opponentTodayApptTotal(guildId) {
  const row = await get(
    `SELECT SUM(count) AS total FROM opponent_appts WHERE guild_id = ? AND date_key = ?`,
    [guildId, ctDateKey()]
  );
  return row?.total ?? 0;
}

/* ================= PART 3 CONTINUES ================= */
/* ================= LIVE TRACKING ================= */

let lastAppointment = null;
let lastSale = null;

/* ================= LIVE MESSAGE IDS ================= */

let liveScoreboardMessageId = null;
let liveSalesMessageId = null;
let compSalesMessageId = null;
let compApptsMessageId = null;

/* ================= LIVE SCOREBOARD ================= */

async function buildLiveScoreboard(guild) {

  const apptsRows = await dailyApptsRows(guild.id, ctDateKey());
  const salesRows = await salesRows(guild.id);

  const goal = await getGoal(guild.id);
  const apptTotal = await todayApptTotal(guild.id);

  let apptLines = [];
  for (let i = 0; i < apptsRows.length; i++) {
    const name = await displayNameFor(guild, apptsRows[i].user_id);
    apptLines.push(`${i + 1}. ${name} — ${apptsRows[i].count}`);
  }

  let salesLines = [];
  for (let i = 0; i < salesRows.length; i++) {
    const name = await displayNameFor(guild, salesRows[i].user_id);
    salesLines.push(`${i + 1}. ${name} — ${salesRows[i].total_sales}`);
  }

  const progress = progressBar(apptTotal, goal);

  return `
🔥 **BLITZ COMMAND CENTER**

**Appointments Leaderboard**
${apptLines.join("\n") || "(none)"}

**Sales Leaderboard**
${salesLines.join("\n") || "(none)"}

**Daily Goal Progress**
Goal: ${goal}
Current: ${apptTotal}

${progress}

Last Appointment: ${lastAppointment || "None"}
Last Sale: ${lastSale || "None"}
Last Updated: ${ctNow()} CT
`;
}

/* ================= LIVE SALES BOARD ================= */

async function buildLiveSalesBoard(guild) {

  const rows = await salesRows(guild.id);

  if (!rows.length) {
    return "**Sales Leaderboard**\n(No sales recorded yet.)";
  }

  let lines = [];

  for (let i = 0; i < rows.length; i++) {

    const name = await displayNameFor(guild, rows[i].user_id);

    lines.push(
`${i + 1}. ${name} — ${rows[i].total_sales} sales
Self-gen: ${rows[i].self_gen} | Set: ${rows[i].set_sales}`
    );
  }

  return `🏆 **SALES LEADERBOARD**

${lines.join("\n\n")}`;
}

/* ================= COMPETITION SALES ================= */

async function buildCompetitionSalesBoard(guild) {

  const our = await salesRows(guild.id);
  const opp = await opponentSalesRows(guild.id);

  let ourLines = [];
  let oppLines = [];

  for (let i = 0; i < our.length; i++) {
    const name = await displayNameFor(guild, our[i].user_id);
    ourLines.push(`${i + 1}. ${name} — ${our[i].total_sales}`);
  }

  for (let i = 0; i < opp.length; i++) {
    const name = await displayNameFor(guild, opp[i].user_id);
    oppLines.push(`${i + 1}. ${name} — ${opp[i].total_sales}`);
  }

  return `
⚔️ **BLITZ BATTLE — SALES**

**Our Team**
${ourLines.join("\n") || "(none)"}

**Opponent**
${oppLines.join("\n") || "(none)"}
`;
}

/* ================= COMPETITION APPOINTMENTS ================= */

async function buildCompetitionApptsBoard(guild) {

  const our = await dailyApptsRows(guild.id, ctDateKey());
  const opp = await opponentDailyApptsRows(guild.id, ctDateKey());

  let ourLines = [];
  let oppLines = [];

  for (let i = 0; i < our.length; i++) {
    const name = await displayNameFor(guild, our[i].user_id);
    ourLines.push(`${i + 1}. ${name} — ${our[i].count}`);
  }

  for (let i = 0; i < opp.length; i++) {
    const name = await displayNameFor(guild, opp[i].user_id);
    oppLines.push(`${i + 1}. ${name} — ${opp[i].count}`);
  }

  return `
⚔️ **BLITZ BATTLE — APPOINTMENTS**

**Our Team**
${ourLines.join("\n") || "(none)"}

**Opponent**
${oppLines.join("\n") || "(none)"}
`;
}

/* ================= MESSAGE UPDATE SYSTEM ================= */

async function updatePinnedMessage(channel, messageId, content) {

  try {

    if (!messageId) {
      const msg = await channel.send(content);
      await msg.pin();
      return msg.id;
    }

    const msg = await channel.messages.fetch(messageId);
    await msg.edit(content);
    return messageId;

  } catch {

    const msg = await channel.send(content);
    await msg.pin();
    return msg.id;

  }

}

/* ================= LIVE UPDATE ENGINE ================= */

async function refreshLiveBoards(guild) {

  const liveChannel = guild.channels.cache.get(CHANNELS.liveScoreboard);
  const salesChannel = guild.channels.cache.get(CHANNELS.sales);
  const compSalesChannel = guild.channels.cache.get(CHANNELS.compSales);
  const compApptsChannel = guild.channels.cache.get(CHANNELS.compAppts);

  if (!liveChannel) return;

  const liveBoard = await buildLiveScoreboard(guild);
  liveScoreboardMessageId = await updatePinnedMessage(
    liveChannel,
    liveScoreboardMessageId,
    liveBoard
  );

  if (salesChannel) {

    const salesBoard = await buildLiveSalesBoard(guild);

    liveSalesMessageId = await updatePinnedMessage(
      salesChannel,
      liveSalesMessageId,
      salesBoard
    );

  }

  if (compSalesChannel) {

    const board = await buildCompetitionSalesBoard(guild);

    compSalesMessageId = await updatePinnedMessage(
      compSalesChannel,
      compSalesMessageId,
      board
    );

  }

  if (compApptsChannel) {

    const board = await buildCompetitionApptsBoard(guild);

    compApptsMessageId = await updatePinnedMessage(
      compApptsChannel,
      compApptsMessageId,
      board
    );

  }

}

/* ================= AUTO REFRESH ================= */

setInterval(async () => {

  client.guilds.cache.forEach(async guild => {

    await refreshLiveBoards(guild);

  });

}, 300000);
/* ================= COMMAND HANDLER ================= */

client.on("messageCreate", async (msg) => {

  if (!msg.guild) return;
  if (msg.author.bot) return;

  const content = msg.content.trim();

  if (!content.startsWith(PREFIX)) return;

  const parts = content.slice(PREFIX.length).trim().split(/\s+/);

  const command = parts.shift()?.toLowerCase();

  const guildId = msg.guild.id;



/* ================= SET GOAL ================= */

if (command === "setgoal") {

  if (!isLeadership(msg.member)) return permissionDenied(msg);

  const goal = Number(parts[0]);

  if (!goal || goal <= 0) return msg.reply("Usage: !setgoal <number>");

  await setGoal(guildId, goal);

  await refreshLiveBoards(msg.guild);

  return msg.reply(`Daily appointment goal set to ${goal}`);

}



/* ================= SALES ================= */

if (command === "setsale") {

  if (!canClose(msg.member)) return permissionDenied(msg);

  const setter = msg.mentions.users.first();

  if (!setter) return msg.reply("Usage: !setsale @setter");

  await recordSetSale(guildId, msg.author.id, setter.id);

  const closerName = msg.member.displayName;

  const setterName = await displayNameFor(msg.guild, setter.id);

  lastSale = `${closerName} closed for ${setterName}`;

  await refreshLiveBoards(msg.guild);

  return msg.channel.send(`💰 Sale Closed\n${closerName} just closed a deal for ${setterName}!`);

}



if (command === "selfgen") {

  if (!canClose(msg.member)) return permissionDenied(msg);

  await recordSelfGen(guildId, msg.author.id);

  const name = msg.member.displayName;

  lastSale = `${name} self generated`;

  await refreshLiveBoards(msg.guild);

  return msg.channel.send(`💰 Self Generated Sale\n${name} recorded a self-gen sale!`);

}



/* ================= SALES LEADERBOARD ================= */

if (command === "sales") {

  const board = await buildSalesLeaderboard(msg.guild);

  return sendChunked(msg.channel, board);

}



/* ================= CLEAR SALES ================= */

if (command === "clearsales") {

  if (!isLeadership(msg.member)) return permissionDenied(msg);

  await run(`DELETE FROM sales WHERE guild_id = ?`, [guildId]);

  await refreshLiveBoards(msg.guild);

  return msg.reply("Sales leaderboard cleared.");

}



/* ================= APPOINTMENTS ================= */

if (command === "setappt") {

  const newCount = await addDailyAppt(guildId, msg.author.id, ctDateKey(), 1);

  const name = msg.member.displayName;

  lastAppointment = `${name} set an appointment`;

  await refreshLiveBoards(msg.guild);

  return msg.channel.send(`📅 Appointment Set\n${name} now has ${newCount} today`);

}



if (command === "removeappt") {

  const mentioned = msg.mentions.users.first();

  let userId = msg.author.id;

  if (mentioned) {

    if (!isLeadership(msg.member)) return permissionDenied(msg);

    userId = mentioned.id;

  }

  const count = await addDailyAppt(guildId, userId, ctDateKey(), -1);

  const name = await displayNameFor(msg.guild, userId);

  await refreshLiveBoards(msg.guild);

  return msg.reply(`Removed 1 appointment from ${name}. Today: ${count}`);

}



if (command === "appts") {

  const board = await buildDailyApptsLeaderboard(msg.guild);

  return sendChunked(msg.channel, board);

}



/* ================= OPPONENT COMMANDS ================= */

if (command === "opsale") {

  if (!canOpponent(msg.member)) return permissionDenied(msg);

  const setter = msg.mentions.users.first();

  if (!setter) return msg.reply("Usage: !opsale @setter");

  await recordOpponentSetSale(guildId, msg.author.id, setter.id);

  await refreshLiveBoards(msg.guild);

  return msg.channel.send("Opponent sale recorded.");

}



if (command === "opselfgen") {

  if (!canOpponent(msg.member)) return permissionDenied(msg);

  await recordOpponentSelfGen(guildId, msg.author.id);

  await refreshLiveBoards(msg.guild);

  return msg.channel.send("Opponent self-gen recorded.");

}



if (command === "opsetappt") {

  if (!canOpponent(msg.member)) return permissionDenied(msg);

  await addOpponentDailyAppt(guildId, msg.author.id, ctDateKey(), 1);

  await refreshLiveBoards(msg.guild);

  return msg.channel.send("Opponent appointment recorded.");

}



/* ================= COMPETITION COMMANDS ================= */

if (command === "compsales") {

  if (!canOpponent(msg.member)) return permissionDenied(msg);

  const board = await buildCompetitionSalesBoard(msg.guild);

  return sendChunked(msg.channel, board);

}



if (command === "compappts") {

  if (!canOpponent(msg.member)) return permissionDenied(msg);

  const board = await buildCompetitionApptsBoard(msg.guild);

  return sendChunked(msg.channel, board);

}



});

/* ================= START BOT ================= */

client.once("ready", async () => {

  console.log(`Bot online as ${client.user.tag}`);

  client.guilds.cache.forEach(async guild => {

    await refreshLiveBoards(guild);

  });

});



(async () => {

  await initDb();

  await client.login(TOKEN);

})();
