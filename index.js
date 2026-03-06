require("dotenv").config();
const { Client, GatewayIntentBits } = require("discord.js");
const sqlite3 = require("sqlite3").verbose();

// ===== CONFIG =====
const PREFIX = "!";
const TIMEZONE = "America/Chicago";
const LEADERSHIP_ROLES = ["Leadership"];
const CLOSER_ROLES = ["Closer"];
const OPPONENT_ROLE_ID = "1479314578642042964";

const TOKEN = process.env.DISCORD_TOKEN;
if (!TOKEN) {
  console.error("❌ Missing DISCORD_TOKEN environment variable.");
  process.exit(1);
}

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

// ===== TIME HELPERS =====
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

function canUseOpponent(member) {
  if (!member) return false;
  if (member.permissions?.has?.("Administrator")) return true;
  if (isLeadership(member)) return true;

  return member.roles.cache.some(r => r.id === OPPONENT_ROLE_ID);
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

  await run(`
    CREATE TABLE IF NOT EXISTS sales (
      guild_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      total_sales INTEGER DEFAULT 0,
      self_gen INTEGER DEFAULT 0,
      set_sales INTEGER DEFAULT 0,
      PRIMARY KEY (guild_id,user_id)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS daily_appts (
      guild_id TEXT,
      date_key TEXT,
      user_id TEXT,
      count INTEGER DEFAULT 0,
      PRIMARY KEY (guild_id,date_key,user_id)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS gym (
      guild_id TEXT,
      user_id TEXT,
      checkins INTEGER DEFAULT 0,
      PRIMARY KEY (guild_id,user_id)
    )
  `);

  // ===== OPPONENT TABLES =====

  await run(`
    CREATE TABLE IF NOT EXISTS op_sales (
      guild_id TEXT,
      user_id TEXT,
      total_sales INTEGER DEFAULT 0,
      self_gen INTEGER DEFAULT 0,
      set_sales INTEGER DEFAULT 0,
      PRIMARY KEY (guild_id,user_id)
    )
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS op_daily_appts (
      guild_id TEXT,
      date_key TEXT,
      user_id TEXT,
      count INTEGER DEFAULT 0,
      PRIMARY KEY (guild_id,date_key,user_id)
    )
  `);
}

// ===== SALES HELPERS =====

async function ensureSalesRow(guildId,userId){
  await run(`INSERT OR IGNORE INTO sales VALUES(?,?,?,?,?)`,
  [guildId,userId,0,0,0])
}

async function recordSetSale(guildId,closerId,setterId){

  await ensureSalesRow(guildId,closerId)
  await ensureSalesRow(guildId,setterId)

  await run(`UPDATE sales SET total_sales = total_sales + 1 WHERE guild_id=? AND user_id=?`,
  [guildId,closerId])

  await run(`UPDATE sales SET total_sales = total_sales + 1 WHERE guild_id=? AND user_id=?`,
  [guildId,setterId])

  await run(`UPDATE sales SET set_sales = set_sales + 1 WHERE guild_id=? AND user_id=?`,
  [guildId,setterId])
}

async function recordSelfGen(guildId,userId){

  await ensureSalesRow(guildId,userId)

  await run(`
  UPDATE sales
  SET total_sales=total_sales+1,
      self_gen=self_gen+1,
      set_sales=set_sales+1
  WHERE guild_id=? AND user_id=?`,
  [guildId,userId])
}

// ===== OPPONENT SALES HELPERS =====

async function ensureOpSalesRow(guildId,userId){
  await run(`INSERT OR IGNORE INTO op_sales VALUES(?,?,?,?,?)`,
  [guildId,userId,0,0,0])
}

async function recordOpSetSale(guildId,closerId,setterId){

  await ensureOpSalesRow(guildId,closerId)
  await ensureOpSalesRow(guildId,setterId)

  await run(`UPDATE op_sales SET total_sales=total_sales+1 WHERE guild_id=? AND user_id=?`,
  [guildId,closerId])

  await run(`UPDATE op_sales SET total_sales=total_sales+1 WHERE guild_id=? AND user_id=?`,
  [guildId,setterId])

  await run(`UPDATE op_sales SET set_sales=set_sales+1 WHERE guild_id=? AND user_id=?`,
  [guildId,setterId])
}

async function recordOpSelfGen(guildId,userId){

  await ensureOpSalesRow(guildId,userId)

  await run(`
  UPDATE op_sales
  SET total_sales=total_sales+1,
      self_gen=self_gen+1,
      set_sales=set_sales+1
  WHERE guild_id=? AND user_id=?`,
  [guildId,userId])
}

// ===== APPT HELPERS =====

async function addDailyAppt(guildId,userId,dateKey,delta){

  await run(`
  INSERT OR IGNORE INTO daily_appts VALUES(?,?,?,0)`,
  [guildId,dateKey,userId])

  const row = await get(`
  SELECT count FROM daily_appts
  WHERE guild_id=? AND date_key=? AND user_id=?`,
  [guildId,dateKey,userId])

  const next = Math.max(0,(row?.count||0)+delta)

  await run(`
  UPDATE daily_appts
  SET count=?
  WHERE guild_id=? AND date_key=? AND user_id=?`,
  [next,guildId,dateKey,userId])

  return next
}

// ===== OPPONENT APPT HELPERS =====

async function addOpDailyAppt(guildId,userId,dateKey,delta){

  await run(`
  INSERT OR IGNORE INTO op_daily_appts VALUES(?,?,?,0)`,
  [guildId,dateKey,userId])

  const row = await get(`
  SELECT count FROM op_daily_appts
  WHERE guild_id=? AND date_key=? AND user_id=?`,
  [guildId,dateKey,userId])

  const next = Math.max(0,(row?.count||0)+delta)

  await run(`
  UPDATE op_daily_appts
  SET count=?
  WHERE guild_id=? AND date_key=? AND user_id=?`,
  [next,guildId,dateKey,userId])

  return next
}

// ===== MESSAGE HANDLER =====

client.on("messageCreate",async(msg)=>{

try{

if(!msg.guild) return
if(msg.author.bot) return

const content = msg.content.trim()
if(!content.startsWith(PREFIX)) return

const parts = content.slice(PREFIX.length).split(/\s+/)
const command = parts.shift().toLowerCase()
const guildId = msg.guild.id

// ===== OUR SALES =====

if(command==="setsale"){

if(!canSetSale(msg.member))
return msg.reply("❌ Only Leadership/Closer can use `!setsale`.")

const target = msg.mentions.users.first()
if(!target) return msg.reply("Usage: !setsale @user")

await recordSetSale(guildId,msg.author.id,target.id)

return msg.reply("✅ Sale recorded")
}

if(command==="selfgen"){

if(!canSetSale(msg.member))
return msg.reply("❌ Only Leadership/Closer can use `!selfgen`.")

await recordSelfGen(guildId,msg.author.id)

return msg.reply("✅ Selfgen recorded")
}

// ===== OPPONENT SALES =====

if(command==="opsale"){

if(!canUseOpponent(msg.member)) return

const target = msg.mentions.users.first()
if(!target) return msg.reply("Usage: !opsale @user")

await recordOpSetSale(guildId,msg.author.id,target.id)

return msg.reply("🔥 Opponent sale recorded")
}

if(command==="opselfgen"){

if(!canUseOpponent(msg.member)) return

await recordOpSelfGen(guildId,msg.author.id)

return msg.reply("🔥 Opponent selfgen recorded")
}

if(command==="opsales"){

if(!canUseOpponent(msg.member)) return

const rows = await all(`
SELECT user_id,total_sales,self_gen,set_sales
FROM op_sales
WHERE guild_id=?
ORDER BY total_sales DESC`,
[guildId])

if(!rows.length) return msg.reply("Opponent Sales Leaderboard\n(No sales yet)")

const lines=[]

for(let i=0;i<rows.length;i++){

const r=rows[i]
const name=await displayNameFor(msg.guild,r.user_id)

lines.push(`${i+1}. ${name}: ${r.total_sales} (Self:${r.self_gen} Set:${r.set_sales})`)
}

return msg.reply(`Opponent Sales Leaderboard\n${lines.join("\n")}`)
}

// ===== OUR APPTS =====

if(command==="setappt"){

const dateKey=ctDateKey()
const newCount=await addDailyAppt(guildId,msg.author.id,dateKey,+1)

return msg.reply(`Appointment added. Today: ${newCount}`)
}

// ===== OPPONENT APPTS =====

if(command==="opsetappt"){

if(!canUseOpponent(msg.member)) return

const dateKey=ctDateKey()

const newCount=await addOpDailyAppt(guildId,msg.author.id,dateKey,+1)

return msg.reply(`Opponent appointment added. Today: ${newCount}`)
}

if(command==="opappts"){

if(!canUseOpponent(msg.member)) return

const dateKey=ctDateKey()

const rows = await all(`
SELECT user_id,count
FROM op_daily_appts
WHERE guild_id=? AND date_key=?
ORDER BY count DESC`,
[guildId,dateKey])

if(!rows.length) return msg.reply("Opponent Appointments\n(No appointments today)")

const lines=[]

for(let i=0;i<rows.length;i++){

const r=rows[i]
const name=await displayNameFor(msg.guild,r.user_id)

lines.push(`${i+1}. ${name} — ${r.count}`)
}

return msg.reply(`Opponent Appointments ${dateKey}\n${lines.join("\n")}`)
}

}catch(err){

console.error(err)
msg.reply("⚠️ Something went wrong")

}

})

client.once("ready",async()=>{
console.log(`✅ Logged in as ${client.user.tag}`)
})

;(async()=>{
await initDb()
await client.login(TOKEN)
})()
