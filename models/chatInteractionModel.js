const { getPool } = require("../db/pool");

function toJsonValue(value) {
  if (value === undefined || value === null) return null;
  return typeof value === "string" ? value : JSON.stringify(value);
}

async function createInteraction(data) {
  const pool = getPool();

  const sql = `
    INSERT INTO chat_interaction (
      channel,
      user_channel_id,
      user_name,
      conversation_id,
      action,
      input_value,
      normalized_input,
      result,
      output_text,
      state_before,
      state_after,
      pdv_id,
      epin_id,
      metadata_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;

  const params = [
    data.channel,
    data.user_channel_id,
    data.user_name || null,
    data.conversation_id || null,
    data.action,
    data.input_value || null,
    data.normalized_input || null,
    data.result,
    data.output_text || null,
    data.state_before || null,
    data.state_after || null,
    data.pdv_id || null,
    data.epin_id || null,
    toJsonValue(data.metadata_json)
  ];

  const [result] = await pool.query(sql, params);
  return { interactionId: result.insertId };
}

module.exports = {
  createInteraction
};