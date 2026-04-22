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

async function findByEpin(epin) {
  const pool = getPool();
  const latestBatch = await getLatestDoneBatch(pool);

  if (!latestBatch) return null;

  const sql = `
    SELECT
      e.epin_id,
      s.epin,
      s.pdv_id,
      s.estado_epin,
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
      p.mi_tienda,
      s.batch_id
    FROM epin_snapshot s
    LEFT JOIN epin e
      ON e.epin_id = s.epin_id
    LEFT JOIN pdv p
      ON p.pdv_id = s.pdv_id
    WHERE s.batch_id = ?
      AND s.epin = ?
    LIMIT 1
  `;

  const [rows] = await pool.query(sql, [latestBatch.batch_id, String(epin).trim()]);
  return rows[0] || null;
}

async function findBasicByEpinId(epinId) {
  const pool = getPool();
  const latestBatch = await getLatestDoneBatch(pool);

  if (!latestBatch) return null;

  const sql = `
    SELECT
      e.epin_id,
      s.epin,
      s.pdv_id,
      s.estado_epin,
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
      s.batch_id
    FROM epin_snapshot s
    INNER JOIN epin e
      ON e.epin_id = s.epin_id
    LEFT JOIN pdv p
      ON p.pdv_id = s.pdv_id
    WHERE s.batch_id = ?
      AND s.epin_id = ?
    LIMIT 1
  `;

  const [rows] = await pool.query(sql, [latestBatch.batch_id, epinId]);
  return rows[0] || null;
}

module.exports = {
  findByEpin,
  findBasicByEpinId
};