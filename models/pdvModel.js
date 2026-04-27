const { getPool } = require("../db/pool");

async function getLatestDoneBatch(pool) {
  const [[row]] = await pool.query(`
    SELECT batch_id, as_of_date
    FROM import_batch
    WHERE status = 'done'
    ORDER BY as_of_date DESC, batch_id DESC
    LIMIT 1
  `);

  return row || null;
}

async function findByIdDms(idDms) {
  const pool = getPool();
  const latestBatch = await getLatestDoneBatch(pool);

  if (!latestBatch) return null;

  const sql = `
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
      p.mi_tienda,
      s.epin_id,
      s.epin,
      s.estado_epin
    FROM pdv p
    LEFT JOIN epin_snapshot s
      ON s.pdv_id = p.pdv_id
     AND s.batch_id = ?
    WHERE p.id_dms = ?
      AND p.activo = 1
    ORDER BY
      CASE s.estado_epin
        WHEN 'ACTIVO' THEN 1
        WHEN 'BLOQUEADO' THEN 2
        WHEN 'INACTIVO' THEN 3
        ELSE 4
      END,
      s.epin ASC
    LIMIT 1
  `;

  const [rows] = await pool.query(sql, [latestBatch.batch_id, String(idDms).trim()]);
  return rows[0] || null;
}

async function findBasicByPdvId(pdvId) {
  const pool = getPool();
  const latestBatch = await getLatestDoneBatch(pool);

  if (!latestBatch) return null;

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
      p.mi_tienda,
      s.epin_id,
      s.epin,
      s.estado_epin
    FROM pdv p
    LEFT JOIN epin_snapshot s
      ON s.pdv_id = p.pdv_id
     AND s.batch_id = ?
    WHERE p.pdv_id = ?
      AND p.activo = 1
    ORDER BY
      CASE s.estado_epin
        WHEN 'ACTIVO' THEN 1
        WHEN 'BLOQUEADO' THEN 2
        WHEN 'INACTIVO' THEN 3
        ELSE 4
      END,
      s.epin ASC
    LIMIT 1
  `;

  const [rows] = await pool.query(sql, [latestBatch.batch_id, pdvId]);
  return rows[0] || null;
}

module.exports = {
  findByIdDms,
  findBasicByPdvId
};