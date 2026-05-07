const { getPool } = require("../db/pool");

function toJsonValue(value) {
  if (value === undefined || value === null) return null;
  return typeof value === "string" ? value : JSON.stringify(value);
}

function buildUserScope(user, where = [], params = []) {
  if (user.role === "SUPERVISOR") {
    where.push("wu.region = ?");
    params.push(user.region || "");
  }
}

async function createCasoPuntual(data) {
  const pool = getPool();

  const sql = `
    INSERT INTO caso_puntual (
      channel,
      created_by_user_channel_id,
      created_by_name,
      created_by_web_user_id,

      pdv_id,
      epin_id,

      id_dms,
      epin_reportado,
      otros_epin,

      nombre_pdv,
      propietario,
      direccion,
      departamento,
      municipio,
      circuito,
      distribuidor,
      categoria,

      estado_pdv,
      estado_epin,
      mi_tienda,

      lat,
      lon,

      tipo_caso_id,
      tipo_caso_codigo,
      tipo_caso_nombre,

      area_responsable_texto,
      areas_responsables_json,

      descripcion,
      contacto_referencia,
      telefono_referencia,

      data_json
    ) VALUES (
      ?, ?, ?, ?,
      ?, ?,
      ?, ?, ?,
      ?, ?, ?, ?, ?, ?, ?, ?,
      ?, ?, ?,
      ?, ?,
      ?, ?, ?,
      ?, ?,
      ?, ?, ?,
      ?
    )
  `;

  const params = [
    data.channel,
    data.created_by_user_channel_id,
    data.created_by_name || null,
    data.created_by_web_user_id ?? null,

    data.pdv_id || null,
    data.epin_id || null,

    data.id_dms,
    data.epin_reportado || null,
    data.otros_epin || null,

    data.nombre_pdv || null,
    data.propietario || null,
    data.direccion || null,
    data.departamento || null,
    data.municipio || null,
    data.circuito || null,
    data.distribuidor || null,
    data.categoria || null,

    data.estado_pdv || null,
    data.estado_epin || null,
    data.mi_tienda ?? null,

    data.lat ?? null,
    data.lon ?? null,

    data.tipo_caso_id,
    data.tipo_caso_codigo,
    data.tipo_caso_nombre,

    data.area_responsable_texto,
    toJsonValue(data.areas_responsables_json || []),

    data.descripcion || null,
    data.contacto_referencia,
    data.telefono_referencia,

    toJsonValue(data.data_json || {})
  ];

  const [result] = await pool.query(sql, params);

  return {
    casoPuntualId: result.insertId
  };
}

async function getUltimoPeriodoConCasos() {
  const pool = getPool();

  const sql = `
    SELECT
      MAX(created_at) AS last_created_at
    FROM caso_puntual
  `;

  const [rows] = await pool.query(sql);
  return rows[0]?.last_created_at || null;
}

async function getAniosDisponibles() {
  const pool = getPool();

  const sql = `
    SELECT DISTINCT YEAR(created_at) AS year
    FROM caso_puntual
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
      COUNT(cp.caso_puntual_id) AS total_casos_puntuales
    FROM web_user wu
    LEFT JOIN caso_puntual cp
      ON cp.created_by_web_user_id = wu.web_user_id
      AND YEAR(cp.created_at) = ?
      AND MONTH(cp.created_at) = ?
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
    where.push("cp.created_by_web_user_id = ?");
    params.push(filters.createdByWebUserId);
  }

  if (filters.year) {
    where.push("YEAR(cp.created_at) = ?");
    params.push(filters.year);
  }

  if (filters.month) {
    where.push("MONTH(cp.created_at) = ?");
    params.push(filters.month);
  }

  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  const sql = `
    SELECT
      cp.caso_puntual_id,
      cp.channel,
      cp.created_by_name,
      cp.created_by_web_user_id,

      cp.id_dms,
      cp.epin_reportado,
      cp.otros_epin,

      cp.nombre_pdv,
      cp.propietario,
      cp.direccion,
      cp.departamento,
      cp.municipio,
      cp.circuito,
      cp.distribuidor,
      cp.categoria,

      cp.estado_pdv,
      cp.estado_epin,
      cp.mi_tienda,

      cp.lat,
      cp.lon,

      cp.tipo_caso_codigo,
      cp.tipo_caso_nombre,
      cp.area_responsable_texto,

      cp.descripcion,
      cp.contacto_referencia,
      cp.telefono_referencia,

      cp.created_at,
      cp.exported_at,
      cp.export_note,

      wu.name AS er_name,
      wu.email AS er_email,
      wu.region
    FROM caso_puntual cp
    LEFT JOIN web_user wu
      ON wu.web_user_id = cp.created_by_web_user_id
    ${whereSql}
    ORDER BY cp.created_at DESC, cp.caso_puntual_id DESC
  `;

  const [rows] = await pool.query(sql, params);
  return rows;
}

async function markExported(casoPuntualIds = [], exportedByWebUserId, exportNote = null) {
  if (!casoPuntualIds.length) return;

  const pool = getPool();
  const placeholders = casoPuntualIds.map(() => "?").join(",");

  const sql = `
    UPDATE caso_puntual
    SET
      exported_at = CURRENT_TIMESTAMP,
      exported_by_web_user_id = ?,
      export_note = ?
    WHERE caso_puntual_id IN (${placeholders})
  `;

  await pool.query(sql, [exportedByWebUserId, exportNote, ...casoPuntualIds]);
}

module.exports = {
  createCasoPuntual,
  getUltimoPeriodoConCasos,
  getAniosDisponibles,
  getResumenPorEr,
  findForExport,
  markExported
};