const { getPool } = require("../db/pool");

async function findOtrosEpinsByPdvId(pool, pdvId, epinPrincipal) {
  if (!pdvId) return null;

  const sql = `
    SELECT
      e.epin
    FROM epin e
    WHERE e.pdv_id = ?
      AND e.activo = 1
      AND e.epin <> ?
    ORDER BY
      CASE e.estado_epin
        WHEN 'ACTIVO' THEN 1
        WHEN 'BLOQUEADO' THEN 2
        WHEN 'INACTIVO' THEN 3
        WHEN 'BAJA' THEN 4
        ELSE 5
      END,
      e.epin ASC
  `;

  const [rows] = await pool.query(sql, [pdvId, epinPrincipal]);

  const otrosEpin = rows
    .map((item) => item.epin)
    .join(", ");

  return otrosEpin || null;
}

async function findByEpin(epin) {
  const pool = getPool();

  const sql = `
    SELECT
      e.epin_id,
      e.epin,
      e.pdv_id,
      e.estado_epin,
      e.last_seen_batch_id AS batch_id,

      p.id_dms,
      p.nombre_pdv,
      p.categoria,
      p.propietario,
      p.circuito,
      p.distribuidor,
      p.departamento,
      p.municipio,
      p.direccion,
      p.lat,
      p.lon,
      p.estado_pdv,
      p.mi_tienda
    FROM epin e
    LEFT JOIN pdv p
      ON p.pdv_id = e.pdv_id
    WHERE e.epin = ?
      AND e.activo = 1
    LIMIT 1
  `;

  const [rows] = await pool.query(sql, [String(epin).trim()]);
  const record = rows[0];

  if (!record) return null;

  const otrosEpin = await findOtrosEpinsByPdvId(
    pool,
    record.pdv_id,
    record.epin
  );

  return {
    ...record,
    otros_epin: otrosEpin
  };
}

async function findBasicByEpinId(epinId) {
  const pool = getPool();

  const sql = `
    SELECT
      e.epin_id,
      e.epin,
      e.pdv_id,
      e.estado_epin,
      e.last_seen_batch_id AS batch_id,

      p.id_dms,
      p.nombre_pdv,
      p.categoria,
      p.propietario,
      p.circuito,
      p.distribuidor,
      p.departamento,
      p.municipio,
      p.direccion,
      p.lat,
      p.lon,
      p.estado_pdv,
      p.mi_tienda
    FROM epin e
    LEFT JOIN pdv p
      ON p.pdv_id = e.pdv_id
    WHERE e.epin_id = ?
      AND e.activo = 1
    LIMIT 1
  `;

  const [rows] = await pool.query(sql, [epinId]);
  const record = rows[0];

  if (!record) return null;

  const otrosEpin = await findOtrosEpinsByPdvId(
    pool,
    record.pdv_id,
    record.epin
  );

  return {
    ...record,
    otros_epin: otrosEpin
  };
}

module.exports = {
  findByEpin,
  findBasicByEpinId
};