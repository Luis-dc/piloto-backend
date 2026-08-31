const { getPool } = require("../db/pool");


// =========================================
// TENDENCIAS EPIN
// =========================================

async function getEpinTrendSeries(period = "12") {
  const pool = getPool();

  const normalizedPeriod =
    String(period || "12").toLowerCase();

  const allowedPeriods = [
    "6",
    "12",
    "all"
  ];

  if (!allowedPeriods.includes(normalizedPeriod)) {
    throw new Error(
      `Periodo no válido: ${period}`
    );
  }


  const [rows] = await pool.query(
    `
    SELECT
      s.batch_id,
      s.previous_batch_id,
      ib.as_of_date,

      s.total_activos,
      s.total_bloqueados,
      s.total_reactivados,
      s.total_nuevos

    FROM epin_batch_summary s

    INNER JOIN import_batch ib
      ON ib.batch_id = s.batch_id

    WHERE ib.status = 'done'

    ORDER BY
      ib.as_of_date ASC,
      s.batch_id ASC
    `
  );


  let selectedRows = rows;

  if (normalizedPeriod !== "all") {
    const limit =
      Number(normalizedPeriod);

    selectedRows =
      rows.slice(-limit);
  }


  const items = selectedRows.map(
    (row) => ({
      batchId:
        Number(row.batch_id),

      previousBatchId:
        row.previous_batch_id === null
          ? null
          : Number(
              row.previous_batch_id
            ),

      asOfDate:
        row.as_of_date,

      activos:
        Number(
          row.total_activos || 0
        ),

      bloqueados:
        Number(
          row.total_bloqueados || 0
        ),

      reactivados:
        Number(
          row.total_reactivados || 0
        ),

      nuevos:
        Number(
          row.total_nuevos || 0
        )
    })
  );


  return {
    period: normalizedPeriod,
    totalCuts: items.length,
    items
  };
}


module.exports = {
  getEpinTrendSeries
};