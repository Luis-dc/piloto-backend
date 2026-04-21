const { getPool } = require("../db/pool");

function parseJson(value) {
  if (value == null) return {};
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return {};
  }
}

function toJsonValue(value) {
  if (value === undefined || value === null) return null;
  return typeof value === "string" ? value : JSON.stringify(value);
}

async function getState(channel, userChannelId, conversationId) {
  const pool = getPool();

  const sql = `
    SELECT
      state_id,
      channel,
      user_channel_id,
      conversation_id,
      state,
      data_json,
      created_at,
      updated_at
    FROM conversation_state
    WHERE channel = ?
      AND user_channel_id = ?
      AND conversation_id = ?
    LIMIT 1
  `;

  const [rows] = await pool.query(sql, [channel, userChannelId, conversationId]);
  if (!rows[0]) return null;

  return {
    ...rows[0],
    data_json: parseJson(rows[0].data_json)
  };
}

async function upsertState(data) {
  const pool = getPool();

  const sql = `
    INSERT INTO conversation_state (
      channel,
      user_channel_id,
      conversation_id,
      state,
      data_json
    ) VALUES (?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      state = VALUES(state),
      data_json = VALUES(data_json),
      updated_at = CURRENT_TIMESTAMP
  `;

  const params = [
    data.channel,
    data.user_channel_id,
    data.conversation_id,
    data.state,
    toJsonValue(data.data_json || {})
  ];

  await pool.query(sql, params);

  return getState(data.channel, data.user_channel_id, data.conversation_id);
}

async function clearState(channel, userChannelId, conversationId) {
  const pool = getPool();

  const sql = `
    DELETE FROM conversation_state
    WHERE channel = ?
      AND user_channel_id = ?
      AND conversation_id = ?
  `;

  await pool.query(sql, [channel, userChannelId, conversationId]);
}

module.exports = {
  getState,
  upsertState,
  clearState
};