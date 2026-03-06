require("dotenv").config();
const { Client, GatewayIntentBits } = require("discord.js");
const sqlite3 = require("sqlite3").verbose();

/* ================= CONFIG ================= */

const PREFIX = "!";

const TIMEZONE = "America/Chicago";

/* CHANNEL IDS */

const CHANNELS = {

LIVE_SCOREBOARD: "1479273397698564179",

SALES: "1458250404835098795",

APPOINTMENTS: "1458250231354495150",

COMP_SALES: "1479295628528844921",

COMP_APPTS: "1479308152951410768",

GENERAL_CHAT: "1458248543000068228"

};

/* ROLE IDS */

const ROLES = {

LEADERSHIP: "1458245230598946940",

ADMIN: "1458245454482640966",

REP: "1458245642026750178",

CLOSER: "1458245812827062342",

BOT: "1458969879406444751",

RECRUIT: "1479313032961064970",

OPPONENT: "1479314578642042964"

};

const TOKEN = process.env.DISCORD_TOKEN;

if (!TOKEN) {

console.error("Missing DISCORD_TOKEN");

process.exit(1);

}

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

/* ================= TIME HELPERS ================= */

function ctDateKey(date = new Date()) {

return new Intl.DateTimeFormat("en-CA", {

timeZone: TIMEZONE,

year: "numeric",

month: "2-digit",

day: "2-digit"

}).format(date);

}

function ctTimeLabel(date = new Date()) {

return new Intl.DateTimeFormat("en-US", {

timeZone: TIMEZONE,

hour: "numeric",

minute: "2-digit"

}).format(date);

}

/* ================= PERMISSIONS ================= */

function hasRole(member, roleId) {

return !!member?.roles?.cache?.has(roleId);

}

function isLeadership(member) {

return hasRole(member, ROLES.LEADERSHIP);

}

function isAdmin(member) {

return hasRole(member, ROLES.ADMIN);

}

function canClose(member) {

return isLeadership(member) || hasRole(member, ROLES.CLOSER);

}

function canUseOpponent(member) {

return (

isLeadership(member) ||

isAdmin(member) ||

hasRole(member, ROLES.OPPONENT)

);

}

function canUseCompetition(member) {

return (

isLeadership(member) ||

hasRole(member, ROLES.OPPONENT)

);

}

function deny(msg) {

return msg.reply("You do not have role permissions to use this command");

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

/* ================= INIT DB ================= */

async function initDb() {

/* SALES */

await run(`

CREATE TABLE IF NOT EXISTS sales (

guild_id TEXT,

user_id TEXT,

total_sales INTEGER DEFAULT 0,

self_gen INTEGER DEFAULT 0,

set_sales INTEGER DEFAULT 0,

PRIMARY KEY(guild_id,user_id)

)

`);

/* OPPONENT SALES */

await run(`

CREATE TABLE IF NOT EXISTS opponent_sales (

guild_id TEXT,

user_id TEXT,

total_sales INTEGER DEFAULT 0,

self_gen INTEGER DEFAULT 0,

set_sales INTEGER DEFAULT 0,

PRIMARY KEY(guild_id,user_id)

)

`);

/* DAILY APPOINTMENTS */

await run(`

CREATE TABLE IF NOT EXISTS daily_appts (

guild_id TEXT,

date_key TEXT,

user_id TEXT,

count INTEGER DEFAULT 0,

PRIMARY KEY(guild_id,date_key,user_id)

)

`);

/* OPPONENT APPOINTMENTS */

await run(`

CREATE TABLE IF NOT EXISTS opponent_appts (

guild_id TEXT,

date_key TEXT,

user_id TEXT,

count INTEGER DEFAULT 0,

PRIMARY KEY(guild_id,date_key,user_id)

)

`);

/* GYM */

await run(`

CREATE TABLE IF NOT EXISTS gym (

guild_id TEXT,

user_id TEXT,

checkins INTEGER DEFAULT 0,

PRIMARY KEY(guild_id,user_id)

)

`);

/* LIVE BOARDS */

await run(`

CREATE TABLE IF NOT EXISTS live_boards (

guild_id TEXT,

name TEXT,

message_id TEXT,

PRIMARY KEY(guild_id,name)

)

`);

/* SETTINGS */

await run(`

CREATE TABLE IF NOT EXISTS bot_settings (

guild_id TEXT,

key TEXT,

value TEXT,

PRIMARY KEY(guild_id,key)

)

`);

}

/* ================= SALES HELPERS ================= */

async function ensureSalesRow(guildId, userId) {

await run(

`INSERT OR IGNORE INTO sales (guild_id,user_id,total_sales,self_gen,set_sales)

VALUES (?, ?, 0,0,0)`,

[guildId, userId]

);

}

async function ensureOpponentSalesRow(guildId, userId) {

await run(

`INSERT OR IGNORE INTO opponent_sales (guild_id,user_id,total_sales,self_gen,set_sales)

VALUES (?, ?, 0,0,0)`,

[guildId, userId]

);

}
/* ================= SALES RECORDING ================= */

async function recordSetSale(guildId, closerId, setterId) {

await ensureSalesRow(guildId, closerId);

await ensureSalesRow(guildId, setterId);

await run(

`UPDATE sales SET total_sales = total_sales + 1 WHERE guild_id=? AND user_id=?`,

[guildId, closerId]

);

await run(

`UPDATE sales

SET total_sales = total_sales + 1,

set_sales = set_sales + 1

WHERE guild_id=? AND user_id=?`,

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

WHERE guild_id=? AND user_id=?`,

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

/* ================= GYM ================= */

async function ensureGymRow(guildId, userId) {

await run(

`INSERT OR IGNORE INTO gym (guild_id,user_id,checkins)

VALUES (?, ?, 0)`,

[guildId, userId]

);

}

async function getGymCount(guildId, userId) {

const row = await get(

`SELECT checkins FROM gym WHERE guild_id=? AND user_id=?`,

[guildId, userId]

);

return row?.checkins ?? 0;

}

async function addGymDelta(guildId, userId, delta) {

await ensureGymRow(guildId, userId);

const current = await getGymCount(guildId, userId);

const next = Math.max(0, current + delta);

await run(

`UPDATE gym SET checkins=? WHERE guild_id=? AND user_id=?`,

[next, guildId, userId]

);

return next;

}

async function getGymRows(guildId) {

return await all(

`SELECT user_id,checkins FROM gym

WHERE guild_id=?

ORDER BY checkins DESC`,

[guildId]

);

}

/* ================= APPOINTMENTS ================= */

async function addDailyAppt(guildId, userId, dateKey, delta) {

await run(

`INSERT OR IGNORE INTO daily_appts (guild_id,date_key,user_id,count)

VALUES (?, ?, ?, 0)`,

[guildId, dateKey, userId]

);

const row = await get(

`SELECT count FROM daily_appts

WHERE guild_id=? AND date_key=? AND user_id=?`,

[guildId, dateKey, userId]

);

const current = row?.count ?? 0;

const next = Math.max(0, current + delta);

await run(

`UPDATE daily_appts SET count=?

WHERE guild_id=? AND date_key=? AND user_id=?`,

[next, guildId, dateKey, userId]

);

return next;

}

async function getDailyApptRows(guildId, dateKey) {

return await all(

`SELECT user_id,count

FROM daily_appts

WHERE guild_id=? AND date_key=?

ORDER BY count DESC`,

[guildId, dateKey]

);

}

async function clearDailyAppts(guildId, dateKey) {

await run(

`DELETE FROM daily_appts

WHERE guild_id=? AND date_key=?`,

[guildId, dateKey]

);

}

/* ================= COMMAND HANDLER ================= */

client.on("messageCreate", async (msg) => {

if (!msg.guild) return;

if (msg.author.bot) return;

const content = msg.content.trim();

if (!content.startsWith(PREFIX)) return;

const parts = content.slice(PREFIX.length).split(/\s+/);

const command = parts.shift().toLowerCase();

const guildId = msg.guild.id;

/* SALES */

if (command === "setsale") {

if (!canClose(msg.member)) return deny(msg);

const setter = msg.mentions.users.first();

if (!setter) return msg.reply("Usage: !setsale @user");

await recordSetSale(guildId, msg.author.id, setter.id);

return msg.reply("Sale recorded.");

}

if (command === "selfgen") {

if (!canClose(msg.member)) return deny(msg);

await recordSelfGen(guildId, msg.author.id);

return msg.reply("Self-gen recorded.");

}

/* GYM */

if (command === "gym") {

const next = await addGymDelta(guildId, msg.author.id, 1);

return msg.reply(`Gym check-in logged. Total: ${next}`);

}

if (command === "gymrank") {

const rows = await getGymRows(guildId);

let text = "**Gym Leaderboard**\n";

for (let i=0;i<rows.length;i++) {

const name = await displayNameFor(msg.guild, rows[i].user_id);

text += `${i+1}. ${name}: ${rows[i].checkins}\n`;

}

return msg.reply(text);

}

/* APPOINTMENTS */

if (command === "setappt") {

const dateKey = ctDateKey();

const next = await addDailyAppt(guildId, msg.author.id, dateKey, 1);

return msg.reply(`Appointment added. Total today: ${next}`);

}

if (command === "appts") {

const dateKey = ctDateKey();

const rows = await getDailyApptRows(guildId, dateKey);

let text = `Appointments ${dateKey}\n`;

for (let i=0;i<rows.length;i++) {

const name = await displayNameFor(msg.guild, rows[i].user_id);

text += `${i+1}. ${name} — ${rows[i].count}\n`;

}

return msg.reply(text);

}

});
/* ================= LIVE BOARD STORAGE ================= */

async function getBoardId(guildId, name) {

const row = await get(

`SELECT message_id FROM live_boards WHERE guild_id=? AND name=?`,

[guildId, name]

);

return row?.message_id || null;

}

async function setBoardId(guildId, name, id) {

await run(

`INSERT OR REPLACE INTO live_boards (guild_id,name,message_id)

VALUES (?, ?, ?)`,

[guildId, name, id]

);

}

async function ensureBoard(channel, guildId, name, content) {

let msgId = await getBoardId(guildId, name);

if (msgId) {

try {

const msg = await channel.messages.fetch(msgId);

await msg.edit(content);

return;

} catch {}

}

const msg = await channel.send(content);

await msg.pin();

await setBoardId(guildId, name, msg.id);

}

/* ================= PROGRESS BAR ================= */

function progressBar(current, goal) {

const width = 10;

if (!goal) return "░░░░░░░░░░ 0%";

const pct = Math.min(100, Math.floor((current/goal)*100));

const filled = Math.floor(pct/10);

return `${"█".repeat(filled)}${"░".repeat(width-filled)} ${pct}%`;

}

/* ================= SALES RENDER ================= */

async function renderSalesBoard(guild) {

const rows = await getSalesRows(guild.id);

let text = "**Sales Leaderboard**\n";

if (!rows.length) return text+"(No sales recorded)";

for (let i=0;i<rows.length;i++) {

const name = await displayNameFor(guild, rows[i].user_id);

text += `${i+1}. ${name}: ${rows[i].total_sales} sales\n`;

}

return text;

}

/* ================= APPTS RENDER ================= */

async function renderApptsBoard(guild) {

const dateKey = ctDateKey();

const rows = await getDailyApptRows(guild.id,dateKey);

let text = `Appointments ${dateKey}\n`;

if (!rows.length) return text+"(None yet)";

for (let i=0;i<rows.length;i++) {

const name = await displayNameFor(guild, rows[i].user_id);

text += `${i+1}. ${name} — ${rows[i].count}\n`;

}

return text;

}

/* ================= COMMAND CENTER ================= */

async function renderCommandCenter(guild) {

const sales = await getSalesRows(guild.id);

const appts = await getDailyApptRows(guild.id,ctDateKey());

let totalAppts = 0;

appts.forEach(r=>totalAppts+=r.count);

const goal = 50;

const bar = progressBar(totalAppts,goal);

let text = `BLITZ COMMAND CENTER\n\n`;

text += `APPOINTMENTS\n`;

for (let i=0;i<appts.length;i++) {

const name = await displayNameFor(guild,appts[i].user_id);

text+=`${i+1}. ${name} — ${appts[i].count}\n`;

}

text+=`\nSALES\n`;

for (let i=0;i<sales.length;i++) {

const name = await displayNameFor(guild,sales[i].user_id);

text+=`${i+1}. ${name} — ${sales[i].total_sales}\n`;

}

text+=`\nDAILY GOAL\n`;

text+=`${bar}\n`;

text+=`\nLast Updated ${ctTimeLabel()} CT`;

return text;

}

/* ================= LIVE REFRESH ================= */

async function refreshLiveSystems(guild){

const salesChannel = guild.channels.cache.get(CHANNELS.SALES);

const apptsChannel = guild.channels.cache.get(CHANNELS.APPOINTMENTS);

const scoreboardChannel = guild.channels.cache.get(CHANNELS.LIVE_SCOREBOARD);

if (salesChannel){

const board = await renderSalesBoard(guild);

await ensureBoard(salesChannel,guild.id,"sales_board",board);

}

if (apptsChannel){

const board = await renderApptsBoard(guild);

await ensureBoard(apptsChannel,guild.id,"appts_board",board);

}

if (scoreboardChannel){

const board = await renderCommandCenter(guild);

await ensureBoard(scoreboardChannel,guild.id,"command_center",board);

}

}

/* ================= BOT READY ================= */

client.once("ready", async () => {

console.log(`Logged in as ${client.user.tag}`);

for (const guild of client.guilds.cache.values()){

await refreshLiveSystems(guild);

}

/* refresh every 5 minutes */

setInterval(async ()=>{

for (const guild of client.guilds.cache.values()){

await refreshLiveSystems(guild);

}

},300000);

});

/* ================= START BOT ================= */

(async()=>{

await initDb();

await client.login(TOKEN);

})();
