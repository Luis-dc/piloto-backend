// controllers/healthController.js
const { pingDb } = require("../db/pool");

async function healthCheck(req, res) {
  try {
    const dbOk = await pingDb();
    return res.status(200).json({
      ok: true,
      service: "smarttrack-backend",
      db: dbOk ? "up" : "down"
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      service: "smarttrack-backend",
      error: error.message
    });
  }
}

module.exports = { healthCheck };