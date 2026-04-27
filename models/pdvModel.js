const { getPool } = require("../db/pool");

function buildPdvWithEpins(pdv, epins) {
  const epinPrincipal = epins[0] || null;

  const otrosEpin = epins
    .slice(1)
    .map((item) => item.epin)
    .join(", ");

  return {
    ...pdv,

    epin_id: epinPrincipal?.epin_id || null,
    epin: epinPrincipal?.epin || null,
    estado_epin: epinPrincipal?.estado_epin || null,

    otros_epin: otrosEpin || null
  };
}

async function findEpinsByPdvId(pool, pdvId) {
  const sql = `
    SELECT
      e.epin_id,
      e.epin,
      e.estado_epin
    FROM epin e
    WHERE e.pdv_id = ?
      AND e.activo = 1
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

  const [rows] = await pool.query(sql, [pdvId]);
  return rows;
}

async function findByIdDms(idDms) {
  const pool = getPool();

  const pdvSql = `
    SELECT
      p.pdv_id,
      p.id_dms,
      p.nombre_pdv,
      p.categoria,
      p.estado_pdv,
      p.propietario,
      p.circuito,
      p.distribuidor,
      p.departamento,
      p.municipio,
      p.direccion,
      p.lat,
      p.lon,
      p.mi_tienda
    FROM pdv p
    WHERE p.id_dms = ?
      AND p.activo = 1
    LIMIT 1
  `;

  const [pdvRows] = await pool.query(pdvSql, [String(idDms).trim()]);
  const pdv = pdvRows[0];

  if (!pdv) return null;

  const epins = await findEpinsByPdvId(pool, pdv.pdv_id);

  return buildPdvWithEpins(pdv, epins);
}

async function findBasicByPdvId(pdvId) {
  const pool = getPool();

  const pdvSql = `
    SELECT
      p.pdv_id,
      p.id_dms,
      p.nombre_pdv,
      p.categoria,
      p.estado_pdv,
      p.propietario,
      p.circuito,
      p.distribuidor,
      p.departamento,
      p.municipio,
      p.direccion,
      p.lat,
      p.lon,
      p.mi_tienda
    FROM pdv p
    WHERE p.pdv_id = ?
      AND p.activo = 1
    LIMIT 1
  `;

  const [pdvRows] = await pool.query(pdvSql, [pdvId]);
  const pdv = pdvRows[0];

  if (!pdv) return null;

  const epins = await findEpinsByPdvId(pool, pdv.pdv_id);

  return buildPdvWithEpins(pdv, epins);
}

module.exports = {
  findByIdDms,
  findBasicByPdvId
};