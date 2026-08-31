const { getPool } = require("../db/pool");

/**
 * Obtiene todos los cortes 2CNV reales y válidos
 * en orden cronológico.
 *
 * Solo participan batches:
 * - status = done
 * - cnv_source_type = UPLOAD
 *
 * Los batches REUSED se ignoran.
 */
async function getRealCnvCuts() {
  const pool = getPool();

  const [rows] = await pool.query(
    `
    SELECT
      batch_id,
      as_of_date
    FROM import_batch
    WHERE status = 'done'
      AND cnv_source_type = 'UPLOAD'
    ORDER BY
      as_of_date ASC,
      batch_id ASC
    `
  );

  return rows.map((row) => ({
    batchId: Number(row.batch_id),
    asOfDate: row.as_of_date
  }));
}

const {
    processEpinHistory
  } = require("./epinHistoryService");
  
  
  async function reprocessAllEpinHistory() {
    const cuts = await getRealCnvCuts();
  
    if (!cuts.length) {
      return {
        processed: 0,
        results: []
      };
    }
  
    const results = [];
  
    for (let i = 0; i < cuts.length; i++) {
      const currentCut = cuts[i];
  
      const previousBatchId =
        i === 0
          ? null
          : cuts[i - 1].batchId;
  
      const result = await processEpinHistory({
        batchId: currentCut.batchId,
        previousBatchId
      });
  
      results.push(result);
    }
  
    return {
      processed: results.length,
      results
    };
}

async function reprocessEpinHistoryFromBatch(startBatchId) {
    const cuts = await getRealCnvCuts();
  
    const startIndex = cuts.findIndex(
      (cut) => cut.batchId === Number(startBatchId)
    );
  
    if (startIndex === -1) {
      throw new Error(
        `El batch ${startBatchId} no es un corte 2CNV real válido`
      );
    }
  
    const results = [];
  
    for (let i = startIndex; i < cuts.length; i++) {
      const currentCut = cuts[i];
  
      const previousBatchId =
        i === 0
          ? null
          : cuts[i - 1].batchId;
  
      const result = await processEpinHistory({
        batchId: currentCut.batchId,
        previousBatchId
      });
  
      results.push(result);
    }
  
    return {
      startBatchId: Number(startBatchId),
      processed: results.length,
      results
    };
}

module.exports = {
  getRealCnvCuts,
  reprocessAllEpinHistory,
  reprocessEpinHistoryFromBatch
};