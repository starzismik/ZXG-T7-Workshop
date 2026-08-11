const express = require("express");
const mysql = require("mysql2/promise");

const PORT = Number.parseInt(process.env.PORT || "3000", 10);
const DATABASE_URL = process.env.DATABASE_URL;
const PRESENCE_TTL_SECONDS = 90;

if (!DATABASE_URL) {
  console.error("DATABASE_URL is required.");
  process.exit(1);
}

const pool = mysql.createPool({
  uri: DATABASE_URL,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  enableKeepAlive: true,
  keepAliveInitialDelay: 0
});

const app = express();
app.disable("x-powered-by");
app.use(express.json({ limit: "16kb" }));

function asyncRoute(handler) {
  return (req, res, next) => {
    Promise.resolve(handler(req, res, next)).catch(next);
  };
}

function readPresenceId(req) {
  const value = req.body?.client_id ?? req.body?.id ?? req.body?.userId ?? req.body?.clientId;
  return typeof value === "string" ? value.trim() : "";
}

async function initializeDatabase() {
  await pool.execute(`
    CREATE TABLE IF NOT EXISTS app_presence (
      presence_id VARCHAR(191) NOT NULL PRIMARY KEY,
      last_seen_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
      INDEX idx_app_presence_last_seen (last_seen_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `);
}

app.get("/health", asyncRoute(async (_req, res) => {
  await pool.query("SELECT 1");
  res.status(200).json({ status: "ok" });
}));

app.post("/presence/heartbeat", asyncRoute(async (req, res) => {
  const presenceId = readPresenceId(req);

  if (!presenceId || presenceId.length > 191) {
    return res.status(400).json({
      error: "A valid id, userId or clientId is required (maximum 191 characters)."
    });
  }

  await pool.execute(
    `INSERT INTO app_presence (presence_id, last_seen_at)
     VALUES (?, CURRENT_TIMESTAMP(3))
     ON DUPLICATE KEY UPDATE last_seen_at = CURRENT_TIMESTAMP(3)`,
    [presenceId]
  );

  return res.status(200).json({ online: true, id: presenceId });
}));

app.post("/presence/disconnect", asyncRoute(async (req, res) => {
  const presenceId = readPresenceId(req);

  if (!presenceId || presenceId.length > 191) {
    return res.status(400).json({
      error: "A valid id, userId or clientId is required (maximum 191 characters)."
    });
  }

  const [result] = await pool.execute(
    "DELETE FROM app_presence WHERE presence_id = ?",
    [presenceId]
  );

  return res.status(200).json({
    online: false,
    id: presenceId,
    removed: result.affectedRows > 0
  });
}));

app.get("/presence/count", asyncRoute(async (_req, res) => {
  await pool.execute(
    "DELETE FROM app_presence WHERE last_seen_at < DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL ? SECOND)",
    [PRESENCE_TTL_SECONDS]
  );

  const [rows] = await pool.execute(
    "SELECT COUNT(*) AS online FROM app_presence WHERE last_seen_at >= DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL ? SECOND)",
    [PRESENCE_TTL_SECONDS]
  );

  return res.status(200).json({
    count: Number(rows[0].online),
    online: Number(rows[0].online),
    ttlSeconds: PRESENCE_TTL_SECONDS
  });
}));

app.use((err, _req, res, _next) => {
  console.error(err);
  res.status(500).json({ error: "Internal server error." });
});

async function start() {
  await initializeDatabase();

  const server = app.listen(PORT, "0.0.0.0", () => {
    console.log(`ZXG presence API listening on port ${PORT}.`);
  });

  async function shutdown(signal) {
    console.log(`${signal} received, shutting down.`);
    server.close(async () => {
      await pool.end();
      process.exit(0);
    });
  }

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
}

start().catch(async (error) => {
  console.error("Unable to start the API:", error);
  await pool.end().catch(() => {});
  process.exit(1);
});
