require("dotenv").config();
const { Client, GatewayIntentBits } = require("discord.js");
const sqlite3 = require("sqlite3").verbose();

/* ================= CONFIG ================= */

const PREFIX = "!";

const ROLE_IDS = {
  leadership: "1458245230598946940",
  admin: "1458245454482640966",
  closer: "1458245812827062342",
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

/* ================= UTIL ================= */

function permissionDenied(msg) {
  msg.reply("You do not have role permissions to use this command");
}

function hasRole(member, roleId) {
  return member.roles.cache.has(roleId);
}

function displayName(member) {
  return member?.displayName || member?.user?.username;
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

function progressBar(current, goal) {
  const width = 10;
  const pct = goal === 0 ? 0 : current / goal;
  const filled = Math.round(pct * width);
  return "█".repeat(filled) + "░".repeat(width - filled);
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
  )
  `);

  await run(`
  CREATE TABLE IF NOT EXISTS opponent_sales (
    guild_id TEXT,
    user_id TEXT,
    total_sales INTEGER DEFAULT 0,
    self_gen INTEGER DEFAULT 0,
    set_sales INTEGER DEFAULT 0,
    PRIMARY KEY (guild_id, user_id)
  )
  `);

  await run(`
  CREATE TABLE IF NOT EXISTS gym (
    guild_id TEXT,
    user_id TEXT,
    checkins INTEGER DEFAULT 0,
    PRIMARY KEY (guild_id, user_id)
  )
  `);

  await run(`
  CREATE TABLE IF NOT EXISTS daily_appts (
    guild_id TEXT,
    date_key TEXT,
    user_id TEXT,
    count INTEGER DEFAULT 0,
    PRIMARY KEY (guild_id, date_key, user_id)
  )
  `);

  await run(`
  CREATE TABLE IF NOT EXISTS opponent_appts (
    guild_id TEXT,
    date_key TEXT,
    user_id TEXT,
    count INTEGER DEFAULT 0,
    PRIMARY KEY (guild_id, date_key, user_id)
  )
  `);

  await run(`
  CREATE TABLE IF NOT EXISTS goals (
    guild_id TEXT PRIMARY KEY,
    goal INTEGER DEFAULT 0
  )
  `);

}

/* ================= SCOREBOARD BUILDERS ================= */

async function buildAppointments(guildId) {

  const rows = await all(
    `SELECT user_id,count FROM daily_appts WHERE guild_id=? AND date_key=? ORDER BY count DESC`,
    [guildId, ctDateKey()]
  );

  if (!rows.length) return "No appointments yet.";

  let out = "";

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const member = await client.guilds.cache.first().members.fetch(r.user_id).catch(()=>null);
    const name = member ? displayName(member) : r.user_id;
    out += `${i+1}. ${name} — ${r.count}\n`;
  }

  return out;
}

async function buildSales(guildId) {

  const rows = await all(
    `SELECT user_id,total_sales FROM sales WHERE guild_id=? ORDER BY total_sales DESC`,
    [guildId]
  );

  if (!rows.length) return "No sales yet.";

  let out = "";

  for (let i=0;i<rows.length;i++) {

    const r = rows[i];
    const member = await client.guilds.cache.first().members.fetch(r.user_id).catch(()=>null);
    const name = member ? displayName(member) : r.user_id;

    out += `${i+1}. ${name} — ${r.total_sales}\n`;
  }

  return out;
}

async function buildScoreboard(guildId) {

  const appts = await buildAppointments(guildId);
  const sales = await buildSales(guildId);

  const goalRow = await get(`SELECT goal FROM goals WHERE guild_id=?`, [guildId]);
  const goal = goalRow?.goal || 0;

  const totalRow = await get(
    `SELECT SUM(count) as total FROM daily_appts WHERE guild_id=? AND date_key=?`,
    [guildId, ctDateKey()]
  );

  const total = totalRow?.total || 0;

  return `
📊 BLITZ COMMAND CENTER

📅 APPOINTMENTS LEADERBOARD
${appts}

🏆 SALES LEADERBOARD
${sales}

🎯 DAILY GOAL PROGRESS
Goal: ${goal}
Current: ${total}
${progressBar(total,goal)}

⚡ MOMENTUM STATUS
Last Updated: ${new Date().toLocaleTimeString()}
`;
}

/* ================= PART 2 CONTINUES ================= */
/* ================= LIVE SCOREBOARD ENGINE ================= */

let scoreboardMessageId = null;

async function updateLiveScoreboard(guild) {

  const channel = guild.channels.cache.get(CHANNELS.liveScoreboard);
  if (!channel) return;

  const content = await buildScoreboard(guild.id);

  try {

    if (scoreboardMessageId) {

      const msg = await channel.messages.fetch(scoreboardMessageId).catch(()=>null);

      if (msg) {
        await msg.edit(content);
        return;
      }

    }

    const newMsg = await channel.send(content);
    await newMsg.pin().catch(()=>{});
    scoreboardMessageId = newMsg.id;

  } catch(e) {
    console.error("Scoreboard update error", e);
  }

}

/* ================= COMMAND HANDLER ================= */

client.on("messageCreate", async (msg) => {

  if (!msg.guild) return;
  if (msg.author.bot) return;
  if (!msg.content.startsWith(PREFIX)) return;

  const args = msg.content.slice(PREFIX.length).trim().split(/\s+/);
  const cmd = args.shift().toLowerCase();
  const guildId = msg.guild.id;

  /* ===== SALES ===== */

  if (cmd === "setsale") {

    if (!(hasRole(msg.member, ROLE_IDS.leadership) || hasRole(msg.member, ROLE_IDS.closer))) {
      return permissionDenied(msg);
    }

    const setter = msg.mentions.users.first();
    if (!setter) return msg.reply("Usage: !setsale @user");

    await run(`INSERT OR IGNORE INTO sales VALUES (?,?,0,0,0)`,[guildId,msg.author.id]);
    await run(`INSERT OR IGNORE INTO sales VALUES (?,?,0,0,0)`,[guildId,setter.id]);

    await run(`UPDATE sales SET total_sales=total_sales+1 WHERE guild_id=? AND user_id=?`,
      [guildId,msg.author.id]);

    await run(`UPDATE sales SET total_sales=total_sales+1,set_sales=set_sales+1 WHERE guild_id=? AND user_id=?`,
      [guildId,setter.id]);

    const general = msg.guild.channels.cache.get(CHANNELS.general);

    if (general) {
      general.send(`🏆 SALE CLOSED\n<@${msg.author.id}> just closed a deal for <@${setter.id}>!`);
    }

    updateLiveScoreboard(msg.guild);

    return;

  }

  if (cmd === "selfgen") {

    if (!(hasRole(msg.member, ROLE_IDS.leadership) || hasRole(msg.member, ROLE_IDS.closer))) {
      return permissionDenied(msg);
    }

    await run(`INSERT OR IGNORE INTO sales VALUES (?,?,0,0,0)`,[guildId,msg.author.id]);

    await run(`UPDATE sales
      SET total_sales=total_sales+1,self_gen=self_gen+1,set_sales=set_sales+1
      WHERE guild_id=? AND user_id=?`,
      [guildId,msg.author.id]);

    const general = msg.guild.channels.cache.get(CHANNELS.general);

    if (general) {
      general.send(`🏆 SALE CLOSED\n<@${msg.author.id}> closed a self-generated deal!`);
    }

    updateLiveScoreboard(msg.guild);

    return;

  }

  if (cmd === "sales") {

    const rows = await all(`SELECT user_id,total_sales,self_gen,set_sales
      FROM sales WHERE guild_id=? ORDER BY total_sales DESC`,[guildId]);

    if (!rows.length) return msg.reply("No sales yet.");

    let text = "**Sales Leaderboard**\n";

    for (let i=0;i<rows.length;i++) {

      const r = rows[i];
      const member = await msg.guild.members.fetch(r.user_id).catch(()=>null);
      const name = member ? displayName(member) : r.user_id;

      text += `${i+1}. ${name}: ${r.total_sales} sales (Self-gen: ${r.self_gen}, Set: ${r.set_sales})\n`;

    }

    return msg.reply(text.slice(0,1900));

  }

  /* ===== APPOINTMENTS ===== */

  if (cmd === "setappt") {

    const date = ctDateKey();

    await run(`INSERT OR IGNORE INTO daily_appts VALUES (?,?,?,0)`,
      [guildId,date,msg.author.id]);

    await run(`UPDATE daily_appts SET count=count+1
      WHERE guild_id=? AND date_key=? AND user_id=?`,
      [guildId,date,msg.author.id]);

    const channel = msg.guild.channels.cache.get(CHANNELS.appointments);

    const row = await get(
      `SELECT count FROM daily_appts WHERE guild_id=? AND date_key=? AND user_id=?`,
      [guildId,date,msg.author.id]
    );

    const member = msg.member;
    const name = displayName(member);

    if (channel) {

      channel.send(
`📅 NEW APPOINTMENT
${name} just set an appointment!
${name} now has ${row.count} today.`
      );

    }

    updateLiveScoreboard(msg.guild);

    return;

  }

  if (cmd === "appts") {

    const rows = await all(
      `SELECT user_id,count FROM daily_appts WHERE guild_id=? AND date_key=? ORDER BY count DESC`,
      [guildId,ctDateKey()]
    );

    if (!rows.length) return msg.reply("No appointments yet.");

    let text = "📅 Daily Appointments\n";

    for (let i=0;i<rows.length;i++) {

      const r = rows[i];
      const member = await msg.guild.members.fetch(r.user_id).catch(()=>null);
      const name = member ? displayName(member) : r.user_id;

      text += `${i+1}. ${name} — ${r.count}\n`;

    }

    return msg.reply(text.slice(0,1900));

  }

  /* ===== GOAL ===== */

  if (cmd === "setgoal") {

    if (!hasRole(msg.member, ROLE_IDS.leadership)) {
      return permissionDenied(msg);
    }

    const goal = parseInt(args[0]);
    if (!goal) return msg.reply("Usage: !setgoal #");

    await run(`INSERT OR REPLACE INTO goals VALUES (?,?)`,[guildId,goal]);

    msg.reply(`🎯 Daily goal set to ${goal}`);

    updateLiveScoreboard(msg.guild);

    return;

  }

});

/* ================= READY ================= */

client.once("ready", async () => {

  console.log(`✅ Logged in as ${client.user.tag}`);

  const guild = client.guilds.cache.first();

  if (guild) {
    updateLiveScoreboard(guild);
  }

  setInterval(async () => {

    const guild = client.guilds.cache.first();
    if (guild) {
      updateLiveScoreboard(guild);
    }

  }, 5 * 60 * 1000);

});

/* ================= START ================= */

(async () => {
  await initDb();
  await client.login(TOKEN);
})();
