const { getPool } = require("../db/pool");

function toJsonValue(value) {
  if (value === undefined || value === null) return null;
  return typeof value === "string" ? value : JSON.stringify(value);
}

async function createInteresado(data) {
  const pool = getPool();

  const sql = `
    INSERT INTO interesado (
      channel,
      created_by_user_channel_id,
      created_by_name,
      created_by_web_user_id,
      input_type,
      input_value,
      pdv_id,
      epin_id,
      id_dms,
      epin_reportado,
      telefono,
      nombre_pdv,
      propietario,
      direccion,
      departamento,
      municipio,
      lat,
      lon,
      data_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;

  const params = [
    data.channel,
    data.created_by_user_channel_id,
    data.created_by_name || null,
    data.created_by_web_user_id || null,
    data.input_type || "ID_DMS",
    data.input_value,
    data.pdv_id || null,
    data.epin_id || null,
    data.id_dms || null,
    data.epin_reportado || null,
    data.telefono || null,
    data.nombre_pdv || null,
    data.propietario || null,
    data.direccion || null,
    data.departamento || null,
    data.municipio || null,
    data.lat ?? null,
    data.lon ?? null,
    toJsonValue(data.data_json || {})
  ];

  const [result] = await pool.query(sql, params);
  return { interesadoId: result.insertId };
}

function buildUserScope(user, where = [], params = []) {
  if (user.role === "SUPERVISOR") {
    where.push("wu.region = ?");
    params.push(user.region || "");
  }
}

async function getUltimoPeriodoConInteresados() {
  const pool = getPool();

  const sql = `
    SELECT
      MAX(created_at) AS last_created_at
    FROM interesado
  `;

  const [rows] = await pool.query(sql);
  return rows[0]?.last_created_at || null;
}

async function getAniosDisponibles() {
  const pool = getPool();

  const sql = `
    SELECT DISTINCT YEAR(created_at) AS year
    FROM interesado
    WHERE created_at IS NOT NULL
    ORDER BY year DESC
  `;

  const [rows] = await pool.query(sql);
  return rows.map((row) => Number(row.year)).filter(Boolean);
}

async function getResumenPorEr({ user, year, month }) {
  const pool = getPool();

  const where = [];
  const params = [];

  where.push("wu.role = 'ER'");
  buildUserScope(user, where, params);

  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  const sql = `
    SELECT
      wu.web_user_id,
      wu.name,
      wu.email,
      wu.region,
      COUNT(i.interesado_id) AS total_interesados
    FROM web_user wu
    LEFT JOIN interesado i
      ON i.created_by_web_user_id = wu.web_user_id
      AND YEAR(i.created_at) = ?
      AND MONTH(i.created_at) = ?
    ${whereSql}
    GROUP BY wu.web_user_id, wu.name, wu.email, wu.region
    ORDER BY wu.name ASC
  `;

  const [rows] = await pool.query(sql, [year, month, ...params]);
  return rows;
}

async function findForExport(user, filters = {}) {
  const pool = getPool();

  const where = [];
  const params = [];

  buildUserScope(user, where, params);

  if (filters.createdByWebUserId) {
    where.push("i.created_by_web_user_id = ?");
    params.push(filters.createdByWebUserId);
  }

  if (filters.year) {
    where.push("YEAR(i.created_at) = ?");
    params.push(filters.year);
  }

  if (filters.month) {
    where.push("MONTH(i.created_at) = ?");
    params.push(filters.month);
  }

  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  const sql = `
    SELECT
      i.interesado_id,
      i.channel,
      i.created_by_name,
      i.created_by_web_user_id,
      i.id_dms,
      i.epin_reportado,
      i.telefono,
      i.nombre_pdv,
      i.propietario,
      i.direccion,
      i.departamento,
      i.municipio,
      i.lat,
      i.lon,
      i.created_at,
      i.exported_at,
      i.export_note,
      wu.name AS er_name,
      wu.email AS er_email,
      wu.region
    FROM interesado i
    LEFT JOIN web_user wu
      ON wu.web_user_id = i.created_by_web_user_id
    ${whereSql}
    ORDER BY i.created_at DESC, i.interesado_id DESC
  `;

  const [rows] = await pool.query(sql, params);
  return rows;
}

async function markExported(interesadoIds = [], exportedByWebUserId, exportNote = null) {
  if (!interesadoIds.length) return;

  const pool = getPool();
  const placeholders = interesadoIds.map(() => "?").join(",");

  const sql = `
    UPDATE interesado
    SET
      exported_at = CURRENT_TIMESTAMP,
      exported_by_web_user_id = ?,
      export_note = ?
    WHERE interesado_id IN (${placeholders})
  `;

  await pool.query(sql, [exportedByWebUserId, exportNote, ...interesadoIds]);
}

module.exports = {
  createInteresado,
  getUltimoPeriodoConInteresados,
  getAniosDisponibles,
  getResumenPorEr,
  findForExport,
  markExported
};