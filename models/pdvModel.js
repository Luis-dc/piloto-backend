const { getPool } = require("../db/pool");

async function findByIdDms(idDms) {
  const pool = getPool();

  const sql = `
    SELECT
      p.pdv_id,
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
      p.mi_tienda,
      (
        SELECT e1.epin_id
        FROM epin e1
        WHERE e1.pdv_id = p.pdv_id AND e1.activo = 1
        ORDER BY e1.updated_at DESC, e1.epin_id DESC
        LIMIT 1
      ) AS epin_id,
      (
        SELECT e2.epin
        FROM epin e2
        WHERE e2.pdv_id = p.pdv_id AND e2.activo = 1
        ORDER BY e2.updated_at DESC, e2.epin_id DESC
        LIMIT 1
      ) AS epin
    FROM pdv p
    WHERE p.id_dms = ?
      AND p.activo = 1
    LIMIT 1
  `;

  const [rows] = await pool.query(sql, [String(idDms).trim()]);
  return rows[0] || null;
}

async function findBasicByPdvId(pdvId) {
  const pool = getPool();

  const sql = `
    SELECT
      p.pdv_id,
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
      p.mi_tienda,
      (
        SELECT e1.epin_id
        FROM epin e1
        WHERE e1.pdv_id = p.pdv_id AND e1.activo = 1
        ORDER BY e1.updated_at DESC, e1.epin_id DESC
        LIMIT 1
      ) AS epin_id,
      (
        SELECT e2.epin
        FROM epin e2
        WHERE e2.pdv_id = p.pdv_id AND e2.activo = 1
        ORDER BY e2.updated_at DESC, e2.epin_id DESC
        LIMIT 1
      ) AS epin
    FROM pdv p
    WHERE p.pdv_id = ?
      AND p.activo = 1
    LIMIT 1
  `;

  const [rows] = await pool.query(sql, [pdvId]);
  return rows[0] || null;
}

module.exports = {
  findByIdDms,
  findBasicByPdvId
};