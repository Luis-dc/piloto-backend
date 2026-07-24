const { getPool } = require("../db/pool");

async function findOtrosEpinsByPdvId(pool, pdvId, epinPrincipal) {
  if (!pdvId) return null;

  const sql = `
    SELECT
      e.epin
    FROM epin e
    CROSS JOIN (
      SELECT batch_id
      FROM import_batch
      WHERE status = 'done'
      ORDER BY as_of_date DESC, batch_id DESC
      LIMIT 1
    ) latest_batch
    JOIN epin_snapshot s
      ON s.batch_id = latest_batch.batch_id
     AND s.epin = e.epin
     AND s.existe_en_2cnv = 1
    WHERE s.pdv_id = ?
      AND e.activo = 1
      AND e.es_epin_actual = 1
      AND e.epin <> ?
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
      s.pdv_id,
      s.estado_epin,
      e.es_epin_actual,
      e.origen_ultimo_corte,
      s.batch_id,

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
    CROSS JOIN (
      SELECT batch_id
      FROM import_batch
      WHERE status = 'done'
      ORDER BY as_of_date DESC, batch_id DESC
      LIMIT 1
    ) latest_batch
    JOIN epin_snapshot s
      ON s.batch_id = latest_batch.batch_id
     AND s.epin = e.epin
     AND s.existe_en_2cnv = 1
    LEFT JOIN pdv p
      ON p.pdv_id = s.pdv_id
    WHERE e.epin = ?
      AND e.activo = 1
      AND e.es_epin_actual = 1
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
      s.pdv_id,
      s.estado_epin,
      e.es_epin_actual,
      e.origen_ultimo_corte,
      s.batch_id,

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
    CROSS JOIN (
      SELECT batch_id
      FROM import_batch
      WHERE status = 'done'
      ORDER BY as_of_date DESC, batch_id DESC
      LIMIT 1
    ) latest_batch
    JOIN epin_snapshot s
      ON s.batch_id = latest_batch.batch_id
     AND s.epin = e.epin
     AND s.existe_en_2cnv = 1
    LEFT JOIN pdv p
      ON p.pdv_id = s.pdv_id
    WHERE e.epin_id = ?
      AND e.activo = 1
      AND e.es_epin_actual = 1
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
