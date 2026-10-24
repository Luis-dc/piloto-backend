const { getPool } = require("../db/pool");

/**
 * Evalua automaticamente la ultima predicción OFICIAL pendiente
 * contra un nuevo corte 2CNV.
 *
 * Regla de clasificación:
 * - ROJO = predicción positiva de bloqueo
 * - VERDE/AMARILLO = predicción negativa
 * - EPIN ausente en el corte de evaluación = bloqueo real
 */
async function evaluatePreviousOfficialRun(evaluationBatchId) {
  const pool = getPool();
  const connection = await pool.getConnection();

  try {
    // =====================================================
    // 1. VALIDAR CORTE DE EVALUACION
    // =====================================================

    const [batchRows] = await connection.query(
      `
      SELECT
        batch_id,
        as_of_date,
        status,
        cnv_source_type
      FROM import_batch
      WHERE batch_id = ?
      LIMIT 1
      `,
      [evaluationBatchId]
    );

    if (!batchRows.length) {
      return {
        evaluated: false,
        reason: "evaluation_batch_not_found"
      };
    }

    const evaluationBatch = batchRows[0];

    if (evaluationBatch.status !== "done") {
      return {
        evaluated: false,
        reason: "evaluation_batch_not_done"
      };
    }

    if (evaluationBatch.cnv_source_type !== "UPLOAD") {
      return {
        evaluated: false,
        reason: "evaluation_batch_not_real_cnv"
      };
    }

    // =====================================================
    // 2. BUSCAR ULTIMO RUN OFICIAL COMPLETADO ANTERIOR
    //    QUE TODAVIA NO HAYA SIDO EVALUADO
    // =====================================================

    const [runRows] = await connection.query(
      `
      SELECT
        mr.model_run_id,
        mr.batch_id AS prediction_batch_id,
        mr.model_version_id,
        ib.as_of_date AS prediction_date
      FROM model_run mr
      INNER JOIN import_batch ib
        ON ib.batch_id = mr.batch_id
      WHERE mr.run_type = 'OFICIAL'
        AND mr.status = 'COMPLETADO'
        AND ib.as_of_date < ?
        AND EXISTS (
          SELECT 1
          FROM prediction p
          WHERE p.model_run_id = mr.model_run_id
          LIMIT 1
        )
        AND NOT EXISTS (
          SELECT 1
          FROM prediction p
          INNER JOIN prediction_evaluation pe
            ON pe.prediction_id = p.prediction_id
          WHERE p.model_run_id = mr.model_run_id
          LIMIT 1
        )
      ORDER BY
        ib.as_of_date DESC,
        mr.model_run_id DESC
      LIMIT 1
      `,
      [evaluationBatch.as_of_date]
    );

    if (!runRows.length) {
      return {
        evaluated: false,
        reason: "no_pending_official_run"
      };
    }

    const run = runRows[0];

    // =====================================================
    // 3. DETECTAR SI EL NUEVO CORTE ES DUPLICADO
    // =====================================================

    const [[comparison]] = await connection.query(
      `
      SELECT

        (
          SELECT COUNT(*)
          FROM epin_snapshot s
          WHERE s.batch_id = ?
            AND s.existe_en_2cnv = 1
        ) AS prediction_total,

        (
          SELECT COUNT(*)
          FROM epin_snapshot s
          WHERE s.batch_id = ?
            AND s.existe_en_2cnv = 1
        ) AS evaluation_total,

        (
          SELECT COUNT(*)
          FROM epin_snapshot base
          LEFT JOIN epin_snapshot actual
            ON actual.batch_id = ?
           AND actual.epin = base.epin
           AND actual.existe_en_2cnv = 1
          WHERE base.batch_id = ?
            AND base.existe_en_2cnv = 1
            AND actual.epin IS NULL
        ) AS ausentes,

        (
          SELECT COUNT(*)
          FROM epin_snapshot actual
          LEFT JOIN epin_snapshot base
            ON base.batch_id = ?
           AND base.epin = actual.epin
           AND base.existe_en_2cnv = 1
          WHERE actual.batch_id = ?
            AND actual.existe_en_2cnv = 1
            AND base.epin IS NULL
        ) AS nuevos,

        (
          SELECT COUNT(*)
          FROM epin_snapshot base
          INNER JOIN epin_snapshot actual
            ON actual.batch_id = ?
           AND actual.epin = base.epin
          WHERE base.batch_id = ?
            AND base.existe_en_2cnv = 1
            AND actual.existe_en_2cnv = 1
            AND (
              NOT (base.estado_epin <=> actual.estado_epin)
              OR NOT (base.saldo_epin <=> actual.saldo_epin)
            )
        ) AS cambios

      `,
      [
        run.prediction_batch_id,
        evaluationBatchId,

        evaluationBatchId,
        run.prediction_batch_id,

        run.prediction_batch_id,
        evaluationBatchId,

        evaluationBatchId,
        run.prediction_batch_id
      ]
    );

    const duplicateCut =
      Number(comparison.ausentes || 0) === 0 &&
      Number(comparison.nuevos || 0) === 0 &&
      Number(comparison.cambios || 0) === 0;

    if (duplicateCut) {
      return {
        evaluated: false,
        reason: "duplicate_cnv_cut",
        modelRunId: run.model_run_id,
        predictionBatchId: run.prediction_batch_id,
        evaluationBatchId
      };
    }

    // =====================================================
    // 4. INSERTAR EVALUACION
    // =====================================================

    await connection.beginTransaction();

    const [insertResult] = await connection.query(
      `
      INSERT INTO prediction_evaluation (
        prediction_id,
        evaluation_batch_id,
        actual_blocked,
        confusion_class
      )
      SELECT
        p.prediction_id,
        ?,

        CASE
          WHEN current_epin.epin IS NULL
            THEN 1
          ELSE 0
        END AS actual_blocked,

        CASE
          WHEN p.risk_level = 'ROJO'
               AND current_epin.epin IS NULL
            THEN 'TP'

          WHEN p.risk_level = 'ROJO'
               AND current_epin.epin IS NOT NULL
            THEN 'FP'

          WHEN p.risk_level <> 'ROJO'
               AND current_epin.epin IS NULL
            THEN 'FN'

          ELSE 'TN'
        END AS confusion_class

      FROM prediction p

      LEFT JOIN epin_snapshot current_epin
        ON current_epin.batch_id = ?
       AND current_epin.epin = p.epin
       AND current_epin.existe_en_2cnv = 1

      WHERE p.model_run_id = ?
      `,
      [
        evaluationBatchId,
        evaluationBatchId,
        run.model_run_id
      ]
    );

    // =====================================================
    // 5. CALCULAR RESULTADOS
    // =====================================================

    const [[metrics]] = await connection.query(
      `
      SELECT
        COUNT(*) AS total,

        SUM(confusion_class = 'TP') AS tp,
        SUM(confusion_class = 'FP') AS fp,
        SUM(confusion_class = 'TN') AS tn,
        SUM(confusion_class = 'FN') AS fn,

        ROUND(
          (
            SUM(confusion_class = 'TP') +
            SUM(confusion_class = 'TN')
          ) / COUNT(*) * 100,
          4
        ) AS accuracy_pct,

        ROUND(
          SUM(confusion_class = 'TP') /
          NULLIF(
            SUM(confusion_class IN ('TP','FP')),
            0
          ) * 100,
          4
        ) AS precision_pct,

        ROUND(
          SUM(confusion_class = 'TP') /
          NULLIF(
            SUM(confusion_class IN ('TP','FN')),
            0
          ) * 100,
          4
        ) AS recall_pct,

        ROUND(
          (2 * SUM(confusion_class = 'TP')) /
          NULLIF(
            (2 * SUM(confusion_class = 'TP')) +
            SUM(confusion_class = 'FP') +
            SUM(confusion_class = 'FN'),
            0
          ) * 100,
          4
        ) AS f1_pct

      FROM prediction_evaluation pe
      INNER JOIN prediction p
        ON p.prediction_id = pe.prediction_id
      WHERE p.model_run_id = ?
        AND pe.evaluation_batch_id = ?
      `,
      [
        run.model_run_id,
        evaluationBatchId
      ]
    );

    await connection.commit();

    return {
      evaluated: true,
      reason: "evaluated",
      modelRunId: run.model_run_id,
      predictionBatchId: run.prediction_batch_id,
      evaluationBatchId,
      insertedEvaluations: insertResult.affectedRows,
      metrics
    };

  } catch (error) {
    try {
      await connection.rollback();
    } catch (_) {
      // No hacer nada: se preserva el error original.
    }

    throw error;

  } finally {
    connection.release();
  }
}

module.exports = {
  evaluatePreviousOfficialRun
};