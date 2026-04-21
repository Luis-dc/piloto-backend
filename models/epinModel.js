const { getPool } = require("../db/pool");

async function findByEpin(epin) {
  const pool = getPool();

  const sql = `
    SELECT
      e.epin_id,
      e.epin,
      e.pdv_id,
      e.estado_epin,
      e.activo,
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
  return rows[0] || null;
}

async function findBasicByEpinId(epinId) {
  const pool = getPool();

  const sql = `
    SELECT
      e.epin_id,
      e.epin,
      e.pdv_id,
      e.estado_epin,
      e.activo,
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
      p.categoria,
      p.mi_tienda
    FROM epin e
    LEFT JOIN pdv p
      ON p.pdv_id = e.pdv_id
    WHERE e.epin_id = ?
    LIMIT 1
  `;

  const [rows] = await pool.query(sql, [epinId]);
  return rows[0] || null;
}

module.exports = {
  findByEpin,
  findBasicByEpinId
};