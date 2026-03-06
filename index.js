require("dotenv").config();
const { Client, GatewayIntentBits } = require("discord.js");
const sqlite3 = require("sqlite3").verbose();

/* ================= CONFIG ================= */

const PREFIX = "!";
const TIMEZONE = "America/Chicago";

const CHANNELS = {
  LIVE_SCOREBOARD: "1479273397698564179",
  SALES: "1458250404835098795",
  APPOINTMENTS: "1458250231354495150",
  COMP_SALES: "1479295628528844921",
  COMP_APPTS: "1479308152951410768",
  GENERAL_CHAT: "1458248543000068228"
};

const ROLES = {
  ADMIN: "1458245454482640966",
  LEADERSHIP: "1458245230598946940",
  CLOSER: "1458245812827062342",
  OPPONENT: "1479314578642042964"
};

const TOKEN = process.env.DISCORD_TOKEN;
const DB_PATH = process.env.DB_PATH || "./bot.db";

/* ================= DISCORD ================= */

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent
  ]
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

/* ================= TIME ================= */

function ctNow() {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: TIMEZONE,
    hour: "numeric",
    minute: "2-digit"
  }).format(new Date());
}

function ctDateKey() {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).format(new Date());
}

/* ================= PERMISSIONS ================= */

function hasRole(member, roleId) {
  if (!member) return false;
  return member.roles.cache.has(roleId);
}

function isAdmin(member) {
  return hasRole(member, ROLES.ADMIN);
}

function isLeadership(member) {
  return hasRole(member, ROLES.LEADERSHIP) || isAdmin(member);
}

function canClose(member) {
  return hasRole(member, ROLES.CLOSER) || isLeadership(member);
}

function canOpponent(member) {
  return (
    hasRole(member, ROLES.OPPONENT) ||
    isLeadership(member) ||
    isAdmin(member)
  );
}

function deny(msg) {
  return msg.reply("You do not have role permissions to use this command.");
}

/* ================= DB INIT ================= */

async function initDb() {

  await run(`
  CREATE TABLE IF NOT EXISTS sales(
    guild_id TEXT,
    user_id TEXT,
    total_sales INTEGER DEFAULT 0,
    self_gen INTEGER DEFAULT 0,
    set_sales INTEGER DEFAULT 0,
    PRIMARY KEY(guild_id,user_id)
  )`);

  await run(`
  CREATE TABLE IF NOT EXISTS opponent_sales(
    guild_id TEXT,
    user_id TEXT,
    total_sales INTEGER DEFAULT 0,
    self_gen INTEGER DEFAULT 0,
    set_sales INTEGER DEFAULT 0,
    PRIMARY KEY(guild_id,user_id)
  )`);

  await run(`
  CREATE TABLE IF NOT EXISTS daily_appts(
    guild_id TEXT,
    date_key TEXT,
    user_id TEXT,
    count INTEGER DEFAULT 0,
    PRIMARY KEY(guild_id,date_key,user_id)
  )`);

  await run(`
  CREATE TABLE IF NOT EXISTS opponent_appts(
    guild_id TEXT,
    date_key TEXT,
    user_id TEXT,
    count INTEGER DEFAULT 0,
    PRIMARY KEY(guild_id,date_key,user_id)
  )`);

  await run(`
  CREATE TABLE IF NOT EXISTS gym(
    guild_id TEXT,
    user_id TEXT,
    checkins INTEGER DEFAULT 0,
    PRIMARY KEY(guild_id,user_id)
  )`);

  await run(`
  CREATE TABLE IF NOT EXISTS boards(
    name TEXT PRIMARY KEY,
    message_id TEXT
  )`);

  await run(`
  CREATE TABLE IF NOT EXISTS settings(
    key TEXT PRIMARY KEY,
    value TEXT
  )`);
}

/* ================= DISPLAY ================= */

async function displayName(guild, id) {
  try {
    const m = await guild.members.fetch(id);
    return m.displayName;
  } catch {
    return `<@${id}>`;
  }
}

/* ================= SALES HELPERS ================= */

async function ensureSalesRow(guildId, userId) {
  await run(
    `INSERT OR IGNORE INTO sales(guild_id,user_id) VALUES(?,?)`,
    [guildId, userId]
  );
}

async function ensureOpponentRow(guildId, userId) {
  await run(
    `INSERT OR IGNORE INTO opponent_sales(guild_id,user_id) VALUES(?,?)`,
    [guildId, userId]
  );
}

async function getSalesRows(guildId) {
  return await all(
    `SELECT user_id,total_sales,self_gen,set_sales
     FROM sales
     WHERE guild_id=?
     ORDER BY total_sales DESC`,
    [guildId]
  );
}

async function getOpponentSalesRows(guildId) {
  return await all(
    `SELECT user_id,total_sales,self_gen,set_sales
     FROM opponent_sales
     WHERE guild_id=?
     ORDER BY total_sales DESC`,
    [guildId]
  );
}

/* ================= APPOINTMENT HELPERS ================= */

async function addAppt(guildId, userId, dateKey, delta) {

  await run(
    `INSERT OR IGNORE INTO daily_appts(guild_id,date_key,user_id,count)
     VALUES(?,?,?,0)`,
    [guildId, dateKey, userId]
  );

  await run(
    `UPDATE daily_appts
     SET count = count + ?
     WHERE guild_id=? AND date_key=? AND user_id=?`,
    [delta, guildId, dateKey, userId]
  );
}

async function getApptRows(guildId, dateKey) {
  return await all(
    `SELECT user_id,count
     FROM daily_appts
     WHERE guild_id=? AND date_key=?
     ORDER BY count DESC`,
    [guildId, dateKey]
  );
}

async function getOpponentApptRows(guildId, dateKey) {
  return await all(
    `SELECT user_id,count
     FROM opponent_appts
     WHERE guild_id=? AND date_key=?
     ORDER BY count DESC`,
    [guildId, dateKey]
  );
}

/* ================= GOAL ================= */

async function setGoal(value) {
  await run(
    `INSERT OR REPLACE INTO settings(key,value)
     VALUES("daily_goal",?)`,
    [value]
  );
}

async function getGoal() {
  const row = await get(
    `SELECT value FROM settings WHERE key="daily_goal"`
  );
  return row ? Number(row.value) : 50;
}

/* ================= READY ================= */

client.once("ready", async () => {
  console.log(`Bot online: ${client.user.tag}`);
});

(async () => {
  await initDb();
  await client.login(TOKEN);
})();
/* ================= LIVE BOARD ENGINE ================= */

async function getBoardId(name) {
  const row = await get(`SELECT message_id FROM boards WHERE name=?`, [name]);
  return row ? row.message_id : null;
}

async function setBoardId(name, id) {
  await run(
    `INSERT OR REPLACE INTO boards(name,message_id) VALUES(?,?)`,
    [name, id]
  );
}

async function ensureBoard(channelId, name, content) {
  const channel = await client.channels.fetch(channelId);

  let messageId = await getBoardId(name);

  if (messageId) {
    try {
      const msg = await channel.messages.fetch(messageId);
      await msg.edit(content);
      return;
    } catch {}
  }

  const msg = await channel.send(content);
  await msg.pin();
  await setBoardId(name, msg.id);
}

function progressBar(current, goal) {
  const percent = Math.min(100, Math.floor((current / goal) * 100));
  const blocks = Math.floor(percent / 10);
  const bar =
    "█".repeat(blocks) + "░".repeat(10 - blocks);
  return `${bar} ${percent}%`;
}

/* ================= LIVE SALES ================= */

async function renderSalesBoard(guild) {

  const rows = await getSalesRows(guild.id);

  if (!rows.length) return "**Sales Leaderboard**\n(No sales yet)";

  let text = "**Sales Leaderboard**\n";

  for (let i = 0; i < rows.length; i++) {

    const name = await displayName(guild, rows[i].user_id);

    text += `${i + 1}. ${name}: ${rows[i].total_sales} sale${rows[i].total_sales === 1 ? "" : "s"} (Self-gen: ${rows[i].self_gen}, Set: ${rows[i].set_sales})\n`;
  }

  return text;
}

/* ================= LIVE APPOINTMENTS ================= */

async function renderApptBoard(guild) {

  const date = ctDateKey();

  const rows = await getApptRows(guild.id, date);

  let text = `📅 Daily Appointments — ${date} (CT)\n`;

  if (!rows.length) return text + "(No appointments yet)";

  for (let i = 0; i < rows.length; i++) {

    const name = await displayName(guild, rows[i].user_id);

    text += `${i + 1}. ${name} — ${rows[i].count}\n`;
  }

  return text;
}

/* ================= COMMAND CENTER ================= */

async function renderCommandCenter(guild) {

  const sales = await getSalesRows(guild.id);

  const appts = await getApptRows(guild.id, ctDateKey());

  const goal = await getGoal();

  let totalAppts = 0;

  appts.forEach(r => totalAppts += r.count);

  const bar = progressBar(totalAppts, goal);

  let text = `**BLITZ COMMAND CENTER**\n\n`;

  text += `**Appointments Leaderboard**\n`;

  appts.slice(0,5).forEach((r,i)=>{
    text += `${i+1}. <@${r.user_id}> — ${r.count}\n`;
  });

  text += `\n**Sales Leaderboard**\n`;

  sales.slice(0,5).forEach((r,i)=>{
    text += `${i+1}. <@${r.user_id}> — ${r.total_sales}\n`;
  });

  text += `\n**Daily Goal Progress**\n`;

  text += `Goal: ${goal}\n`;
  text += `Current: ${totalAppts}\n`;
  text += `${bar}\n`;

  text += `\nLast Updated: ${ctNow()} CT`;

  return text;
}

/* ================= MASTER UPDATE ================= */

async function refreshLiveBoards(guild) {

  const salesBoard = await renderSalesBoard(guild);
  await ensureBoard(CHANNELS.SALES, "sales_board", salesBoard);

  const apptBoard = await renderApptBoard(guild);
  await ensureBoard(CHANNELS.APPOINTMENTS, "appts_board", apptBoard);

  const center = await renderCommandCenter(guild);
  await ensureBoard(CHANNELS.LIVE_SCOREBOARD, "command_center", center);

}
/* ================= SALES COMMANDS ================= */

client.on("messageCreate", async (msg) => {

  if (!msg.guild) return;
  if (msg.author.bot) return;

  const content = msg.content.trim();
  if (!content.startsWith(PREFIX)) return;

  const args = content.slice(PREFIX.length).split(/\s+/);
  const command = args.shift().toLowerCase();
  const guildId = msg.guild.id;

  try {

    /* ---------- SET SALE ---------- */

    if (command === "setsale") {

      if (!canClose(msg.member)) return deny(msg);

      const setter = msg.mentions.users.first();

      if (!setter) return msg.reply("Usage: !setsale @setter");

      await ensureSalesRow(guildId, msg.author.id);
      await ensureSalesRow(guildId, setter.id);

      await run(
        `UPDATE sales SET total_sales = total_sales + 1 WHERE guild_id=? AND user_id=?`,
        [guildId, msg.author.id]
      );

      await run(
        `UPDATE sales SET total_sales = total_sales + 1, set_sales = set_sales + 1 WHERE guild_id=? AND user_id=?`,
        [guildId, setter.id]
      );

      await refreshLiveBoards(msg.guild);

      return msg.reply(`Sale recorded for <@${msg.author.id}> (setter <@${setter.id}>)`);

    }

    /* ---------- SELF GEN ---------- */

    if (command === "selfgen") {

      if (!canClose(msg.member)) return deny(msg);

      await ensureSalesRow(guildId, msg.author.id);

      await run(
        `UPDATE sales
         SET total_sales = total_sales + 1,
         self_gen = self_gen + 1,
         set_sales = set_sales + 1
         WHERE guild_id=? AND user_id=?`,
        [guildId, msg.author.id]
      );

      await refreshLiveBoards(msg.guild);

      return msg.reply(`Self-gen recorded for <@${msg.author.id}>`);

    }

    /* ---------- SALES LEADERBOARD ---------- */

    if (command === "sales") {

      const board = await renderSalesBoard(msg.guild);

      return msg.reply(board);

    }

    /* ---------- SET APPOINTMENT ---------- */

    if (command === "setappt") {

      const date = ctDateKey();

      await addAppt(guildId, msg.author.id, date, 1);

      await refreshLiveBoards(msg.guild);

      return msg.reply(`Appointment recorded for <@${msg.author.id}>`);

    }

    /* ---------- REMOVE APPOINTMENT ---------- */

    if (command === "removeappt") {

      const date = ctDateKey();

      const target = msg.mentions.users.first() || msg.author;

      if (target.id !== msg.author.id && !isLeadership(msg.member)) {
        return deny(msg);
      }

      await addAppt(guildId, target.id, date, -1);

      await refreshLiveBoards(msg.guild);

      return msg.reply(`Removed appointment from <@${target.id}>`);

    }

    /* ---------- APPOINTMENT LEADERBOARD ---------- */

    if (command === "appts") {

      const board = await renderApptBoard(msg.guild);

      return msg.reply(board);

    }

    /* ---------- SET GOAL ---------- */

    if (command === "setgoal") {

      if (!isLeadership(msg.member)) return deny(msg);

      const num = Number(args[0]);

      if (!num) return msg.reply("Usage: !setgoal <number>");

      await setGoal(num);

      await refreshLiveBoards(msg.guild);

      return msg.reply(`Daily goal set to ${num}`);

    }

  } catch (err) {

    console.error(err);
    msg.reply("Error running command.");

  }

});

/* ================= AUTO REFRESH ================= */

client.once("ready", async () => {

  console.log("Live systems active");

  const guild = client.guilds.cache.first();

  if (!guild) return;

  await refreshLiveBoards(guild);

  setInterval(async () => {

    await refreshLiveBoards(guild);

  }, 300000); // 5 minutes

});
