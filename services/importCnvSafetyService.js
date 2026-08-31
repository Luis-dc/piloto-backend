const fs = require("fs");
const path = require("path");
const readline = require("readline");

const { getPool } = require("../db/pool");

const CNV_DROP_WARNING_THRESHOLD = 10;

/**
 * Obtiene el último corte REAL de 2CNV.
 *
 * Solo toma batches:
 * - terminados correctamente
 * - cuyo 2CNV fue realmente UPLOAD
 *
 * Los REUSED no cuentan como nuevo corte.
 */
async function getLatestValidCnvCut() {
    const pool = getPool();
  
    const [batches] = await pool.query(
      `
      SELECT
        batch_id,
        as_of_date
      FROM import_batch
      WHERE status = 'done'
        AND cnv_source_type = 'UPLOAD'
      ORDER BY
        as_of_date DESC,
        batch_id DESC
      LIMIT 1
      `
    );
  
    if (!batches.length) {
      return null;
    }
  
    const latestBatch = batches[0];
  
    const [[countRow]] = await pool.query(
      `
      SELECT
        COUNT(DISTINCT epin) AS total_epins
      FROM stg_2cnv
      WHERE batch_id = ?
        AND epin IS NOT NULL
        AND epin <> ''
      `,
      [latestBatch.batch_id]
    );
  
    return {
      batchId: Number(latestBatch.batch_id),
      asOfDate: latestBatch.as_of_date,
      totalEpins: Number(countRow?.total_epins || 0)
    };
}

/**
 * Cuenta EPIN únicos dentro del CSV limpio.
 *
 * No hace comparación EPIN por EPIN contra MySQL.
 * Solo obtiene el volumen del archivo preparado.
 */
async function countUniqueEpinsFromCleanCsv(
  csvPath
) {
  const uniqueEpins = new Set();

  const stream = fs.createReadStream(
    csvPath,
    {
      encoding: "utf8"
    }
  );

  const reader = readline.createInterface({
    input: stream,
    crlfDelay: Infinity
  });

  let firstLine = true;

  for await (const rawLine of reader) {
    const line = String(rawLine || "").trim();

    if (!line) {
      continue;
    }

    /*
     * El CSV limpio de 2CNV tiene:
     *
     * EPIN,SALDO
     *
     * Solo necesitamos la primera columna.
     */
    let epin = line
      .split(",")[0]
      .trim()
      .replace(/^"|"$/g, "");

    /*
     * Quitar BOM si existiera.
     */
    epin = epin.replace(/^\uFEFF/, "");

    if (firstLine) {
      firstLine = false;

      if (epin.toUpperCase() === "EPIN") {
        continue;
      }
    }

    if (/^[0-9]{1,32}$/.test(epin)) {
      uniqueEpins.add(epin);
    }
  }

  return uniqueEpins.size;
}

/**
 * Analiza si el volumen del nuevo 2CNV presenta
 * una caída considerable respecto al último
 * corte real.
 *
 * IMPORTANTE:
 * Esto NO rechaza el archivo.
 *
 * Solo genera una advertencia cuando la
 * disminución es >= 10%.
 */
async function analyzeCnvVolumeSafety({
  preparationDir,
  cleanedFileName
}) {
  if (
    !preparationDir ||
    !cleanedFileName
  ) {
    return null;
  }

  const csvPath = path.join(
    preparationDir,
    cleanedFileName
  );

  if (!fs.existsSync(csvPath)) {
    throw new Error(
      "No se encontró el archivo 2CNV limpio para realizar la validación de volumen"
    );
  }

  const currentTotalEpins =
    await countUniqueEpinsFromCleanCsv(
      csvPath
    );

  const previousCut =
    await getLatestValidCnvCut();

  /*
   * Si todavía no existe histórico,
   * el archivo se considera corte base.
   */
  if (!previousCut) {
    return {
      applied: false,
      warning: false,
      reason: "NO_PREVIOUS_VALID_CNV",
      thresholdPct:
        CNV_DROP_WARNING_THRESHOLD,
      currentTotalEpins,
      previousCut: null,
      dropCount: 0,
      dropPct: 0
    };
  }

  const dropCount =
    Math.max(
      previousCut.totalEpins -
        currentTotalEpins,
      0
    );

  const dropPct =
    previousCut.totalEpins > 0
      ? (
          dropCount /
          previousCut.totalEpins
        ) * 100
      : 0;

  const warning =
    dropPct >=
    CNV_DROP_WARNING_THRESHOLD;

  return {
    applied: true,

    warning,

    thresholdPct:
      CNV_DROP_WARNING_THRESHOLD,

    currentTotalEpins,

    previousCut,

    dropCount,

    dropPct:
      Number(dropPct.toFixed(2)),

    code: warning
      ? "CNV_VOLUME_DROP_WARNING"
      : "CNV_VOLUME_OK"
  };
}

module.exports = {
  analyzeCnvVolumeSafety
};