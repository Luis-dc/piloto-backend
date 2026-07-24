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
      s.estado_epin,
      e.es_epin_actual,
      e.origen_ultimo_corte
    FROM epin_snapshot s
    CROSS JOIN (
      SELECT batch_id
      FROM import_batch
      WHERE status = 'done'
      ORDER BY as_of_date DESC, batch_id DESC
      LIMIT 1
    ) latest_batch
    JOIN epin e
      ON e.epin = s.epin
    WHERE s.batch_id = latest_batch.batch_id
      AND s.pdv_id = ?
      AND s.existe_en_2cnv = 1
      AND e.activo = 1
      AND e.es_epin_actual = 1
    ORDER BY
      CASE s.estado_epin
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
