const { getPool } = require("../db/pool");


// =====================================================
// LISTAR VERSIONES
// =====================================================

async function listModelVersions() {
  const pool = getPool();

  const [rows] = await pool.query(`
    SELECT
      model_version_id,
      version_name,
      algorithm,
      trained_at,
      training_start_date,
      training_end_date,
      accuracy,
      precision_score,
      recall_score,
      f1_score,
      auprc,
      status,
      artifact_path,
      created_at
    FROM model_version
    ORDER BY model_version_id DESC
  `);

  return rows;
}


// =====================================================
// ACTIVAR MODELO CANDIDATO
// =====================================================

async function activateCandidateModel(
  modelVersionId
) {
  const pool = getPool();
  const connection =
    await pool.getConnection();

  try {
    await connection.beginTransaction();

    const [candidateRows] =
      await connection.query(
        `
          SELECT
            model_version_id,
            version_name,
            status
          FROM model_version
          WHERE model_version_id = ?
          FOR UPDATE
        `,
        [modelVersionId]
      );

    const candidate =
      candidateRows[0];

    if (!candidate) {
      const error =
        new Error(
          "La versión de modelo no existe."
        );

      error.status = 404;
      throw error;
    }

    if (candidate.status !== "CANDIDATO") {
      const error =
        new Error(
          "Solo un modelo CANDIDATO puede ser activado."
        );

      error.status = 400;
      throw error;
    }

    // Bloqueamos también cualquier modelo activo
    await connection.query(`
      SELECT model_version_id
      FROM model_version
      WHERE status = 'ACTIVO'
      FOR UPDATE
    `);

    // El modelo actualmente activo pasa a histórico
    await connection.query(`
      UPDATE model_version
      SET status = 'HISTORICO'
      WHERE status = 'ACTIVO'
    `);

    // El candidato seleccionado pasa a activo
    const [result] =
      await connection.query(
        `
          UPDATE model_version
          SET status = 'ACTIVO'
          WHERE model_version_id = ?
            AND status = 'CANDIDATO'
        `,
        [modelVersionId]
      );

    if (result.affectedRows !== 1) {
      throw new Error(
        "No fue posible activar el modelo candidato."
      );
    }

    await connection.commit();

    return {
      activated: true,
      modelVersionId:
        candidate.model_version_id,
      versionName:
        candidate.version_name,
      status: "ACTIVO"
    };

  } catch (error) {
    await connection.rollback();
    throw error;

  } finally {
    connection.release();
  }
}


// =====================================================
// DESCARTAR MODELO CANDIDATO
// =====================================================

async function discardCandidateModel(
  modelVersionId
) {
  const pool = getPool();

  const [result] =
    await pool.query(
      `
        UPDATE model_version
        SET status = 'DESCARTADO'
        WHERE model_version_id = ?
          AND status = 'CANDIDATO'
      `,
      [modelVersionId]
    );

  if (result.affectedRows !== 1) {
    const error =
      new Error(
        "El modelo no existe o no se encuentra en estado CANDIDATO."
      );

    error.status = 400;
    throw error;
  }

  return {
    discarded: true,
    modelVersionId:
      Number(modelVersionId),
    status: "DESCARTADO"
  };
}


module.exports = {
  listModelVersions,
  activateCandidateModel,
  discardCandidateModel
};