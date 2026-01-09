const express = require("express");

const app = express();
app.get("/", (req, res) => res.send("OK"));
app.get("/health", (req, res) => res.send("OK"));
app.listen(process.env.PORT || 3000, () => console.log("Web server ready"));

const { Client, GatewayIntentBits } = require("discord.js");
const sqlite3 = require("sqlite3").verbose();

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers,
  ],
});

const db = new sqlite3.Database("./leaderboards.sqlite");
// Create table if it doesn't exist
db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS counters (
      guild_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      key TEXT NOT NULL,
      value INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (guild_id, user_id, key)
    )
  `);
});

// ===== Leaderboard Helpers =====
function getLeaderboardSales(guildId) {
  return new Promise((resolve, reject) => {
    db.all(
      `
      SELECT user_id,
        SUM(CASE WHEN key IN ('sales', 'sales_total') THEN value ELSE 0 END) AS total,
        SUM(CASE WHEN key = 'sales_selfgen' THEN value ELSE 0 END) AS selfgen,
        SUM(CASE WHEN key = 'sales_set' THEN value ELSE 0 END) AS setcount
      FROM counters
      WHERE guild_id = ?
        AND key IN ('sales', 'sales_total', 'sales_selfgen', 'sales_set')
      GROUP BY user_id
      HAVING total > 0
      ORDER BY total DESC, selfgen DESC, setcount DESC
      `,
      [guildId],
      (err, rows) => (err ? reject(err) : resolve(rows || []))
    );
  });
}

function getLeaderboardGym(guildId) {
  return new Promise((resolve, reject) => {
    db.all(
      `
      SELECT user_id, value AS total
      FROM counters
      WHERE guild_id = ? AND key = 'gym'
      ORDER BY total DESC
      `,
      [guildId],
      (err, rows) => (err ? reject(err) : resolve(rows || []))
    );
  });
}

async function displayNameFor(msg, userId) {
  try {
    const member = await msg.guild.members.fetch(userId);
    return member?.displayName || member?.user?.username || `User ${userId}`;
  } catch {
    return `User ${userId}`;
  }
}
// ===============================

function hasAnyRole(member, roleNames) {
  if (!member) return false;
  const names = new Set(roleNames.map((r) => r.toLowerCase()));
  return member.roles.cache.some((role) => names.has(role.name.toLowerCase()));
}

function isLeadership(member) {
  return hasAnyRole(member, LEADERSHIP_ROLE_NAMES);
}

function canSetSale(member) {
  return isLeadership(member) || hasAnyRole(member, CLOSER_ROLE_NAMES);
}

function incrementCounter(guildId, userId, key, amount = 1) {
  return new Promise((resolve, reject) => {
    db.run(
      `
      INSERT INTO counters (guild_id, user_id, key, value)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(guild_id, user_id, key)
      DO UPDATE SET value = value + ?
      `,
      [guildId, userId, key, amount, amount],
      (err) => {
        if (err) return reject(err);
        db.get(
          `SELECT value FROM counters WHERE guild_id = ? AND user_id = ? AND key = ?`,
          [guildId, userId, key],
          (err2, row) => {
            if (err2) return reject(err2);
            resolve(row?.value ?? 0);
          }
        );
      }
    );
  });
}

function clearKeyForGuild(guildId, key) {
  return new Promise((resolve, reject) => {
    db.run(`DELETE FROM counters WHERE guild_id = ? AND key = ?`, [guildId, key], (err) =>
      err ? reject(err) : resolve()
    );
  });
}

client.on("ready", () => {
  console.log(`Logged in as ${client.user.tag}`);
});

client.on("messageCreate", async (msg) => {
  try {
    if (!msg.guild) return;
    if (msg.author.bot) return;
    if (!msg.content.startsWith(PREFIX)) return;

    const [commandRaw] = msg.content.slice(PREFIX.length).trim().split(/\s+/);
    const command = (commandRaw || "").toLowerCase();

    // !gym
    if (command === "gym") {
      const newTotal = await incrementCounter(msg.guild.id, msg.author.id, "gym", 1);
      return msg.reply(`🏋️ Gym check-in logged for <@${msg.author.id}>. Total: **${newTotal}**`);
    }

    // !setsale @user
if (command === "setsale") {
  if (!canSetSale(msg.member)) {
    return msg.reply("❌ You don’t have permission to use `!setsale`.");
  }

  const mentioned = msg.mentions.users.first();
  if (!mentioned) return msg.reply("Usage: `!setsale @user`");

  const guildId = msg.guild.id;
  const authorId = msg.author.id;
  const mentionedId = mentioned.id;

  // SELF-GEN: mentioning yourself counts as ONE sale total
  if (mentionedId === authorId) {
    await incrementCounter(guildId, authorId, "sales_total", 1);
    await incrementCounter(guildId, authorId, "sales_selfgen", 1);
    return msg.reply(`✅ Sale recorded for ${msg.member.displayName}. (Self-gen)`);
  }

  // SET: two different people, both get ONE sale + ONE set
  await incrementCounter(guildId, authorId, "sales_total", 1);
  await incrementCounter(guildId, mentionedId, "sales_total", 1);

  await incrementCounter(guildId, authorId, "sales_set", 1);
  await incrementCounter(guildId, mentionedId, "sales_set", 1);

  const mentionedMember =
    msg.guild.members.cache.get(mentionedId) ||
    (await msg.guild.members.fetch(mentionedId).catch(() => null));
  const mentionedName = mentionedMember?.displayName || mentioned.username;

  return msg.reply(`✅ Sale recorded. Credited: ${msg.member.displayName} + ${mentionedName}`);
}

    // !clearsales
    if (command === "clearsales") {
      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership can use `!clearsales`.");
      }

      await clearKeyForGuild(msg.guild.id, "sales_total");
      await clearKeyForGuild(msg.guild.id, "sales_selfgen");
      await clearKeyForGuild(msg.guild.id, "sales_set");

      return msg.reply("🧹 Sales leaderboard cleared.");
    }

    // !cleargym
    if (command === "cleargym") {
      if (!isLeadership(msg.member)) {
        return msg.reply("❌ Only Leadership can use `!cleargym`.");
      }
      await clearKeyForGuild(msg.guild.id, "gym");
      return msg.reply("🧹 Gym leaderboard cleared.");
    }
    // !leaderboard (sales)
    if (command === "leaderboard") {
      const rows = await getLeaderboardSales(msg.guild.id);

      if (!rows || rows.length === 0) {
        return msg.reply("No sales have been recorded yet.");
      }

      let reply = "**Sales Leaderboard**\n";
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const name = await displayNameFor(msg, r.user_id);
        reply += `${i + 1}. ${name}: ${r.total} sales (Self-gen: ${r.selfgen}, Set: ${r.setcount})\n`;
      }

      return msg.reply(reply);
    }
    // !gymrank
    if (command === "gymrank") {
      const rows = await getLeaderboardGym(msg.guild.id);

      if (!rows || rows.length === 0) {
        return msg.reply("No gym check-ins have been recorded yet.");
      }

      let reply = "**Gym Leaderboard**\n";
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const name = await displayNameFor(msg, r.user_id);
        reply += `${i + 1}. ${name}: ${r.total} check-ins\n`;
      }

      return msg.reply(reply);
    }
  } catch (err) {
    console.error(err);
	return msg.reply("⚠️ Something went wrong running that command.");
  }
});

client.login(process.env.DISCORD_TOKEN);

