// db/pool.js
const mysql = require("mysql2/promise");

let pool = null;

function getPool() {
  if (!pool) {
    pool = mysql.createPool({
      host: process.env.DB_HOST,
      port: Number(process.env.DB_PORT || 3306),
      user: process.env.DB_USER,
      password: process.env.DB_PASS,
      database: process.env.DB_NAME,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
      enableKeepAlive: true,
      keepAliveInitialDelay: 0,
      localInfile: true
    });
  }
  return pool;
}

async function pingDb() {
  const p = getPool();
  const [rows] = await p.query("SELECT 1 AS ok");
  return rows?.[0]?.ok === 1;
}

module.exports = { getPool, pingDb };