const { getPool } = require("../db/pool");

/**
 * Obtiene información básica de un batch.
 */
async function getBatchInfo(connection, batchId) {
  const [rows] = await connection.query(
    `
    SELECT
      batch_id,
      as_of_date,
      status
    FROM import_batch
    WHERE batch_id = ?
    LIMIT 1
    `,
    [batchId]
  );

  return rows[0] || null;
}

/**
 * Busca automáticamente el último corte 2CNV válido anterior.
 *
 * Solo considera batches cuyo 2CNV fue realmente subido (UPLOAD).
 * Los batches que reutilizan un 2CNV no participan en la comparación.
 */
async function findPreviousValidCnvBatch(
  connection,
  currentBatchId,
  currentDate
) {
  const [rows] = await connection.query(
    `
    SELECT
      batch_id,
      as_of_date
    FROM import_batch
    WHERE status = 'done'
      AND cnv_source_type = 'UPLOAD'
      AND batch_id <> ?
      AND as_of_date < ?
    ORDER BY as_of_date DESC, batch_id DESC
    LIMIT 1
    `,
    [currentBatchId, currentDate]
  );

  return rows[0] || null;
}

/**
 * Cuenta los EPIN activos del corte.
 *
 * ACTIVO = aparece en el 2CNV del corte.
 */
async function countActiveEpins(connection, batchId) {
  const [[row]] = await connection.query(
    `
    SELECT COUNT(DISTINCT epin) AS total
    FROM stg_2cnv
    WHERE batch_id = ?
      AND epin IS NOT NULL
      AND epin <> ''
    `,
    [batchId]
  );

  return Number(row?.total || 0);
}

/**
 * Registra EPIN BLOQUEADOS.
 *
 * Regla:
 * estaba en el 2CNV anterior
 * y ya no aparece en el 2CNV actual.
 */
async function insertBlockedEvents(
  connection,
  batchId,
  previousBatchId
) {
  const [result] = await connection.query(
    `
    INSERT INTO epin_event (
      epin_id,
      batch_id,
      previous_batch_id,
      event_type
    )
    SELECT
      e.epin_id,
      ?,
      ?,
      'BLOQUEADO'
    FROM (
      SELECT DISTINCT epin
      FROM stg_2cnv
      WHERE batch_id = ?
        AND epin IS NOT NULL
        AND epin <> ''
    ) previous_cnv

    INNER JOIN epin e
      ON e.epin = previous_cnv.epin

    LEFT JOIN (
      SELECT DISTINCT epin
      FROM stg_2cnv
      WHERE batch_id = ?
        AND epin IS NOT NULL
        AND epin <> ''
    ) current_cnv
      ON current_cnv.epin = previous_cnv.epin

    WHERE current_cnv.epin IS NULL
    `,
    [
      batchId,
      previousBatchId,
      previousBatchId,
      batchId
    ]
  );

  return result.affectedRows || 0;
}

/**
 * Registra EPIN NUEVOS.
 *
 * Regla:
 * aparece en el corte actual
 * y nunca había aparecido anteriormente en ningún 2CNV.
 */
async function insertNewEvents(
  connection,
  batchId,
  previousBatchId,
  currentDate
) {
  const [result] = await connection.query(
    `
    INSERT INTO epin_event (
      epin_id,
      batch_id,
      previous_batch_id,
      event_type
    )
    SELECT
      e.epin_id,
      ?,
      ?,
      'NUEVO'
    FROM (
      SELECT DISTINCT epin
      FROM stg_2cnv
      WHERE batch_id = ?
        AND epin IS NOT NULL
        AND epin <> ''
    ) current_cnv

    INNER JOIN epin e
      ON e.epin = current_cnv.epin

    WHERE NOT EXISTS (
      SELECT 1
      FROM stg_2cnv old_cnv

      INNER JOIN import_batch old_batch
        ON old_batch.batch_id = old_cnv.batch_id

      WHERE old_cnv.epin = current_cnv.epin
        AND old_batch.status = 'done'
        AND old_batch.as_of_date < ?
    )
    `,
    [
      batchId,
      previousBatchId,
      batchId,
      currentDate
    ]
  );

  return result.affectedRows || 0;
}

/**
 * Registra EPIN REACTIVADOS.
 *
 * Regla:
 * aparece en el corte actual,
 * no estaba en el corte inmediatamente anterior,
 * pero sí había aparecido anteriormente en algún 2CNV.
 */
async function insertReactivatedEvents(
  connection,
  batchId,
  previousBatchId,
  currentDate
) {
  const [result] = await connection.query(
    `
    INSERT INTO epin_event (
      epin_id,
      batch_id,
      previous_batch_id,
      event_type
    )
    SELECT
      e.epin_id,
      ?,
      ?,
      'REACTIVADO'
    FROM (
      SELECT DISTINCT epin
      FROM stg_2cnv
      WHERE batch_id = ?
        AND epin IS NOT NULL
        AND epin <> ''
    ) current_cnv

    INNER JOIN epin e
      ON e.epin = current_cnv.epin

    LEFT JOIN (
      SELECT DISTINCT epin
      FROM stg_2cnv
      WHERE batch_id = ?
        AND epin IS NOT NULL
        AND epin <> ''
    ) previous_cnv
      ON previous_cnv.epin = current_cnv.epin

    WHERE previous_cnv.epin IS NULL

      AND EXISTS (
        SELECT 1
        FROM stg_2cnv old_cnv

        INNER JOIN import_batch old_batch
          ON old_batch.batch_id = old_cnv.batch_id

        WHERE old_cnv.epin = current_cnv.epin
          AND old_batch.status = 'done'
          AND old_batch.as_of_date < ?
      )
    `,
    [
      batchId,
      previousBatchId,
      batchId,
      previousBatchId,
      currentDate
    ]
  );

  return result.affectedRows || 0;
}

/**
 * Guarda o actualiza el resumen precalculado del corte.
 */
async function saveBatchSummary(
  connection,
  {
    batchId,
    previousBatchId,
    totalActivos,
    totalBloqueados,
    totalReactivados,
    totalNuevos
  }
) {
  await connection.query(
    `
    INSERT INTO epin_batch_summary (
      batch_id,
      previous_batch_id,
      total_activos,
      total_bloqueados,
      total_reactivados,
      total_nuevos
    )
    VALUES (?, ?, ?, ?, ?, ?)

    ON DUPLICATE KEY UPDATE
      previous_batch_id = VALUES(previous_batch_id),
      total_activos = VALUES(total_activos),
      total_bloqueados = VALUES(total_bloqueados),
      total_reactivados = VALUES(total_reactivados),
      total_nuevos = VALUES(total_nuevos)
    `,
    [
      batchId,
      previousBatchId,
      totalActivos,
      totalBloqueados,
      totalReactivados,
      totalNuevos
    ]
  );
}

/**
 * Procesa el histórico de un corte 2CNV.
 *
 * Casos:
 *
 * processEpinHistory({ batchId: 8 })
 * -> busca automáticamente el corte anterior.
 *
 * processEpinHistory({
 *   batchId: 8,
 *   previousBatchId: 7
 * })
 * -> usa explícitamente Batch 7.
 *
 * processEpinHistory({
 *   batchId: 1,
 *   previousBatchId: null
 * })
 * -> trata Batch 1 como CORTE BASE.
 */
async function processEpinHistory({
  batchId,
  previousBatchId
}) {
  const pool = getPool();
  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    // =====================================================
    // 1) OBTENER CORTE ACTUAL
    // =====================================================

    const currentBatch = await getBatchInfo(
      connection,
      batchId
    );

    if (!currentBatch) {
      throw new Error(
        `No existe el batch ${batchId}`
      );
    }

    // =====================================================
    // 2) RESOLVER CORTE ANTERIOR
    // =====================================================

    let resolvedPreviousBatchId = previousBatchId;

    // undefined = buscar automáticamente.
    if (resolvedPreviousBatchId === undefined) {
      const previousCnvBatch =
        await findPreviousValidCnvBatch(
          connection,
          batchId,
          currentBatch.as_of_date
        );

      resolvedPreviousBatchId =
        previousCnvBatch?.batch_id || null;
    }

    let previousBatch = null;

    // null = CORTE BASE.
    if (resolvedPreviousBatchId !== null) {
      previousBatch = await getBatchInfo(
        connection,
        resolvedPreviousBatchId
      );

      if (!previousBatch) {
        throw new Error(
          `No existe el batch anterior ${resolvedPreviousBatchId}`
        );
      }
    }

    // =====================================================
    // 3) LIMPIAR EVENTOS DEL CORTE
    // =====================================================
    // Esto permite reprocesar el mismo batch sin duplicar
    // BLOQUEADOS / REACTIVADOS / NUEVOS.

    await connection.query(
      `
      DELETE FROM epin_event
      WHERE batch_id = ?
      `,
      [batchId]
    );

    // =====================================================
    // 4) CONTAR ACTIVOS
    // =====================================================

    const totalActivos = await countActiveEpins(
      connection,
      batchId
    );

    // =====================================================
    // 5) CORTE BASE
    // =====================================================

    if (!previousBatch) {
      await saveBatchSummary(
        connection,
        {
          batchId,
          previousBatchId: null,
          totalActivos,
          totalBloqueados: 0,
          totalReactivados: 0,
          totalNuevos: 0
        }
      );

      await connection.commit();

      return {
        batchId,
        previousBatchId: null,
        base: true,
        activos: totalActivos,
        bloqueados: 0,
        reactivados: 0,
        nuevos: 0
      };
    }

    // =====================================================
    // 6) BLOQUEADOS
    // =====================================================

    const totalBloqueados =
      await insertBlockedEvents(
        connection,
        batchId,
        resolvedPreviousBatchId
      );

    // =====================================================
    // 7) NUEVOS
    // =====================================================

    const totalNuevos =
      await insertNewEvents(
        connection,
        batchId,
        resolvedPreviousBatchId,
        currentBatch.as_of_date
      );

    // =====================================================
    // 8) REACTIVADOS
    // =====================================================

    const totalReactivados =
      await insertReactivatedEvents(
        connection,
        batchId,
        resolvedPreviousBatchId,
        currentBatch.as_of_date
      );

    // =====================================================
    // 9) GUARDAR RESUMEN
    // =====================================================

    await saveBatchSummary(
      connection,
      {
        batchId,
        previousBatchId:
          resolvedPreviousBatchId,
        totalActivos,
        totalBloqueados,
        totalReactivados,
        totalNuevos
      }
    );

    // =====================================================
    // 10) CONFIRMAR TRANSACCIÓN
    // =====================================================

    await connection.commit();

    return {
      batchId,
      previousBatchId:
        resolvedPreviousBatchId,
      base: false,
      activos: totalActivos,
      bloqueados: totalBloqueados,
      reactivados: totalReactivados,
      nuevos: totalNuevos
    };
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
}

module.exports = {
  processEpinHistory
};