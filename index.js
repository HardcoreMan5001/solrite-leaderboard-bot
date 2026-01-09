// solrite-leaderboard-bot (message commands)
// Commands:
// !setsale @user  -> if @user is you => self-gen +1, else => set +1 for sender and target
// !leaderboard    -> sales leaderboard
// !clearsales      -> wipe all sales rows (Leadership/Admin only)
//
// !gym            -> gym check-in +1
// !gymrank        -> gym leaderboard
// !cleargym       -> wipe all gym rows (Leadership/Admin only)

require("dotenv").config();
const { Client, GatewayIntentBits } = require("discord.js");
const sqlite3 = require("sqlite3").verbose();
const path = require("path");

// ====== CONFIG ======
const PREFIX = "!";
const LEADERSHIP_ROLES = ["Leadership", "Admin"]; // adjust names if your roles differ
// ====================

// ----- Discord client -----
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers,
  ],
});

// ----- SQLite setup -----
const dbPath = path.join(__dirname, "data.sqlite");
const db = new sqlite3.Database(dbPath);

// Promisified helpers
function run(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) return reject(err);
      resolve(this);
    });
  });
}

function all(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) return reject(err);
      resolve(rows);
    });
  });
}

async function initDb() {
  await run(`
    CREATE TABLE IF NOT EXISTS sales (
      guild_id TEXT NOT NULL,
      user_id  TEXT NOT NULL,
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
}

function isLeadership(member) {
  if (!member || !member.roles) return false;
  return member.roles.cache.some((r) => LEADERSHIP_ROLES.includes(r.name));
}

// Safe display name
async function getDisplayName(guild, userId) {
  try {
    const member =
      guild.members.cache.get(userId) ||
      (await guild.members.fetch(userId).catch(() => null));
    if (!member) return `Unknown User (${userId})`;
    return member.displayName || member.user.username;
  } catch {
    return `Unknown User (${userId})`;
  }
}

// ===== Sales helpers =====
async function ensureSalesRow(guildId, userId) {
  await run(
    `INSERT OR IGNORE INTO sales (guild_id, user_id, self_gen, set_sales)
     VALUES (?, ?, 0, 0)`,
    [guildId, userId]
  );
}

async function addSelfGen(guildId, userId) {
  await ensureSalesRow(guildId, userId);
  await run(
    `UPDATE sales SET self_gen = self_gen + 1 WHERE guild_id = ? AND user_id = ?`,
    [guildId, userId]
  );
}

async function addSetSale(guildId, setterId, targetId) {
  await ensureSalesRow(guildId, setterId);
  await ensureSalesRow(guildId, targetId);

  await run(
    `UPDATE sales SET set_sales = set_sales + 1 WHERE guild_id = ? AND user_id = ?`,
    [guildId, setterId]
  );
  await run(
    `UPDATE sales SET set_sales = set_sales + 1 WHERE guild_id = ? AND user_id = ?`,
    [guildId, targetId]
  );
}

async function getSalesLeaderboard(guild) {
  const rows = await all(
    `SELECT user_id, self_gen, set_sales,
            (self_gen + set_sales) AS total
     FROM sales
     WHERE guild_id = ?
     ORDER BY total DESC, set_sales DESC, self_gen DESC`,
    [guild.id]
  );

  const result = [];
  for (const r of rows) {
    const name = await getDisplayName(guild, r.user_id);
    result.push({
      name,
      total: r.total,
      selfGen: r.self_gen,
      setSales: r.set_sales,
    });
  }
  return result;
}

// ===== Gym helpers =====
async function ensureGymRow(guildId, userId) {
  await run(
    `INSERT OR IGNORE INTO gym (guild_id, user_id, checkins)
     VALUES (?, ?, 0)`,
    [guildId, userId]
  );
}

async function addGymCheckin(guildId, userId) {
  await ensureGymRow(guildId, userId);
  await run(
    `UPDATE gym SET checkins = checkins + 1 WHERE guild_id = ? AND user_id = ?`,
    [guildId, userId]
  );
}

async function getGymCount(guildId, userId) {
  const rows = await all(
    `SELECT checkins FROM gym WHERE guild_id = ? AND user_id = ?`,
    [guildId, userId]
  );
  return rows.length ? rows[0].checkins : 0;
}

async function getGymLeaderboard(guild) {
  const rows = await all(
    `SELECT user_id, checkins
     FROM gym
     WHERE guild_id = ?
     ORDER BY checkins DESC`,
    [guild.id]
  );

  const result = [];
  for (const r of rows) {
    const name = await getDisplayName(guild, r.user_id);
    result.push({ name, checkins: r.checkins });
  }
  return result;
}

// ----- Ready -----
client.once("ready", async () => {
  await initDb();
  console.log(`✅ Logged in as ${client.user.tag}`);
});

// ----- Message handler -----
client.on("messageCreate", async (msg) => {
  try {
    if (!msg.guild) return; // ignore DMs
    if (msg.author.bot) return;
    if (!msg.content.startsWith(PREFIX)) return;

    const raw = msg.content.slice(PREFIX.length).trim();
    if (!raw) return;

    const parts = raw.split(/\s+/);
    const command = parts[0].toLowerCase();

    // ---------------- SALES ----------------
    if (command === "setsale") {
      // leadership/admin only
      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership/Admin can use `!setsale`.");
      }

      const target = msg.mentions.users.first();
      if (!target) return msg.reply("Usage: `!setsale @user`");

      const setter = msg.author;
      const isSelfGen = target.id === setter.id;

      if (isSelfGen) {
        await addSelfGen(msg.guild.id, setter.id);
        const setterName = await getDisplayName(msg.guild, setter.id);
        return msg.reply(`✅ Sale recorded for ${setterName}. (Self-gen)`);
      } else {
        await addSetSale(msg.guild.id, setter.id, target.id);
        const setterName = await getDisplayName(msg.guild, setter.id);
        const targetName = await getDisplayName(msg.guild, target.id);
        return msg.reply(`✅ Sale recorded. Credited: ${setterName} + ${targetName}`);
      }
    }

    if (command === "leaderboard") {
      const board = await getSalesLeaderboard(msg.guild);

      if (!board.length) {
        return msg.reply("Sales Leaderboard\n(no entries yet)");
      }

      let text = "Sales Leaderboard\n";
      board.slice(0, 25).forEach((u, i) => {
        text += `${i + 1}. ${u.name}: ${u.total} sales (Self-gen: ${u.selfGen}, Set: ${u.setSales})\n`;
      });

      return msg.reply(text.trim());
    }

    if (command === "clearsales") {
      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership/Admin can use `!clearsales`.");
      }
      await run(`DELETE FROM sales WHERE guild_id = ?`, [msg.guild.id]);
      return msg.reply("🧹 Sales leaderboard cleared.");
    }

    // ---------------- GYM ----------------
    if (command === "gym") {
      await addGymCheckin(msg.guild.id, msg.author.id);
      const total = await getGymCount(msg.guild.id, msg.author.id);
      const name = await getDisplayName(msg.guild, msg.author.id);
      return msg.reply(`🏋️ Gym check-in logged for ${name}. Total: ${total}`);
    }

    if (command === "gymrank") {
      const board = await getGymLeaderboard(msg.guild);

      if (!board.length) {
        return msg.reply("Gym Leaderboard\n(no entries yet)");
      }

      let text = "Gym Leaderboard\n";
      board.slice(0, 25).forEach((u, i) => {
        text += `${i + 1}. ${u.name}: ${u.checkins} check-ins\n`;
      });

      return msg.reply(text.trim());
    }

    if (command === "cleargym") {
      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership/Admin can use `!cleargym`.");
      }
      await run(`DELETE FROM gym WHERE guild_id = ?`, [msg.guild.id]);
      return msg.reply("🧹 Gym leaderboard cleared.");
    }

    // Optional: quick help
    if (command === "help") {
      return msg.reply(
        [
          "Commands:",
          "`!setsale @user` (Leadership/Admin only) — logs a sale (self-gen if you tag yourself)",
          "`!leaderboard` — shows sales leaderboard",
          "`!clearsales` (Leadership/Admin only) — clears sales",
          "`!gym` — logs a gym check-in",
          "`!gymrank` — shows gym leaderboard",
          "`!cleargym` (Leadership/Admin only) — clears gym",
        ].join("\n")
      );
    }
  } catch (err) {
    console.error("Command error:", err);
    // Keep user-facing error simple
    try {
      await msg.reply("⚠️ Something went wrong running that command.");
    } catch {}
  }
});

// ----- Login -----
if (!process.env.DISCORD_TOKEN) {
  console.error("❌ Missing DISCORD_TOKEN in environment variables.");
  process.exit(1);
}

client.login(process.env.DISCORD_TOKEN);
