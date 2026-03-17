// services/importService.js
const fs = require("fs");
const { parse } = require("csv-parse");
const { getPool } = require("../db/pool");
const logger = require("../utils/logger");
const path = require("path");

// ---------- helpers ----------
function parseFechaCorteFromFilename(filename) {
  const name = String(filename || "");

  const m =
    name.match(/(\d{2})_(\d{2})(\d{4})/) ||
    name.match(/(\d{2})_(\d{2})_(\d{4})/) ||
    name.match(/(\d{2})-(\d{2})-(\d{4})/);

  if (!m) {
    throw new Error(
      `No pude extraer FECHA_CORTE del nombre: "${filename}". Formato esperado: *_DD_MMYYYY.*`
    );
  }

  const dd = Number(m[1]);
  const mm = Number(m[2]);
  const yyyy = Number(m[3]);

  if (yyyy < 2000 || yyyy > 2100 || mm < 1 || mm > 12 || dd < 1 || dd > 31) {
    throw new Error(`FECHA_CORTE inválida en nombre: "${filename}"`);
  }

  return `${yyyy}-${String(mm).padStart(2, "0")}-${String(dd).padStart(2, "0")}`;
}

function validateAndGetFechaCorte(bdoOriginalName, cnvOriginalName) {
  const bdoDate = parseFechaCorteFromFilename(bdoOriginalName);
  const cnvDate = parseFechaCorteFromFilename(cnvOriginalName);
  if (bdoDate !== cnvDate) {
    throw new Error(`FECHA_CORTE no coincide: BDO(${bdoDate}) vs 2CNV(${cnvDate}).`);
  }
  return bdoDate;
}

let cachedImportBatchCols = null;
async function getColumnsSet(tableName) {
  const pool = getPool();
  const [rows] = await pool.query(`SHOW COLUMNS FROM ${tableName}`);
  return new Set(rows.map((r) => r.Field));
}

async function getImportBatchColumns() {
  if (cachedImportBatchCols) return cachedImportBatchCols;
  cachedImportBatchCols = await getColumnsSet("import_batch");
  return cachedImportBatchCols;
}

async function updateImportBatch(batchId, patch) {
  const pool = getPool();
  const cols = await getImportBatchColumns();

  const entries = Object.entries(patch).filter(([k]) => cols.has(k));
  if (!entries.length) return;

  const sets = entries.map(([k]) => `${k}=?`).join(", ");
  const values = entries.map(([, v]) => v);
  values.push(batchId);

  await pool.query(`UPDATE import_batch SET ${sets} WHERE batch_id=?`, values);
}

async function createOrGetBatch({ asOfDate, bdoName, cnvName, userLabel }) {
  const pool = getPool();
  const cols = await getImportBatchColumns();

  const [existing] = await pool.query(
    "SELECT batch_id, status FROM import_batch WHERE as_of_date = ? ORDER BY batch_id DESC LIMIT 1",
    [asOfDate]
  );

  if (existing.length) {
    // Reutilizamos el batch pero lo dejamos listo para reproceso
    const batchId = existing[0].batch_id;

    await updateImportBatch(batchId, {
      status: "processing",
      error_message: null,
      // si existen estas columnas en tu tabla, se resetean:
      total_rows: 0,
      inserted_rows: 0,
      updated_rows: 0,
      error_rows: 0,
      file_name: `CORTE_${asOfDate}`,
      source: "manual_upload",
      source_path: `bdo=${bdoName};cnv=${cnvName}`,
      uploaded_by: userLabel || "unknown"
    });

    return { batchId, asOfDate, reused: true };
  }

  const payload = {
    file_name: `CORTE_${asOfDate}`,
    source: "manual_upload",
    source_path: `bdo=${bdoName};cnv=${cnvName}`,
    uploaded_by: userLabel || "unknown",
    as_of_date: asOfDate,
    status: "processing"
  };

  const insertCols = Object.keys(payload).filter((c) => cols.has(c));
  const placeholders = insertCols.map(() => "?").join(", ");
  const values = insertCols.map((c) => payload[c]);

  const sql = `INSERT INTO import_batch (${insertCols.join(", ")}) VALUES (${placeholders})`;
  const [result] = await pool.query(sql, values);

  return { batchId: result.insertId, asOfDate, reused: false };
}

function normalizeHeader(h) {
  return String(h || "")
    .trim()
    .replace(/^"+|"+$/g, "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/\s+/g, "_");
}

// Define el “contrato” de headers que tú dijiste que ya limpiaste
function mapBdoHeader(header) {
  const h = normalizeHeader(header);

  const map = {
    ID: "id_dms",
    DEPARTAMENTO: "departamento",
    MUNICIPIO: "municipio",
    CIRCUITO: "circuito",
    EPIN: "epin",
    ES_EPIN: "es_epin",
    ES_EPIN_: "es_epin",
    ES_EPIN__1: "es_epin",
    ES_EPIN__2: "es_epin",
    ES_EPIN__3: "es_epin",
    ES_EPIN__4: "es_epin",
    ES_EPIN__5: "es_epin",
    ES_EPIN__6: "es_epin",
    ES_EPIN__7: "es_epin",
    ES_EPIN__8: "es_epin",
    ES_EPIN__9: "es_epin",
    ES_EPIN__10: "es_epin",
    "ES_EPIN": "es_epin",
    "ES_EPIN_": "es_epin",
    "ES_EPIN__": "es_epin",
    "ES_EPIN__0": "es_epin",
    "ES_EPIN__00": "es_epin",
    "ES_EPIN__000": "es_epin",
    "ES_EPIN__0000": "es_epin",
    "ES_EPIN__00000": "es_epin",
    "ES_EPIN__000000": "es_epin",
    "ES_EPIN__0000000": "es_epin",
    "ES_EPIN__00000000": "es_epin",
    "ES_EPIN__000000000": "es_epin",
    "ES_EPIN__0000000000": "es_epin",
    ESTADO: "estado_pdv",
    NOMBRE: "nombre_pdv",
    DIRECCION: "direccion",
    CATEGORIA: "categoria",
    X: "lat",
    Y: "lon",
    PROPIETARIO: "propietario",
    DISTRIBUIDOR: "distribuidor"
  };

  // caso especial: "ES EPIN"
  if (h === "ES_EPIN" || h === "ES_EPIN_" || h === "ES_EPIN__") return "es_epin";
  if (h === "ES_EPIN" || h === "ES_EPIN") return "es_epin";
  if (h === "ES_EPIN" || h === "ES_EPIN") return "es_epin";

  // si viene como "ES_EPIN" ya lo captura
  return map[h] || null;
}

function map2CnvHeader(header) {
  const h = normalizeHeader(header);
  const map = {
    DISTRIBUIDORA: "distribuidora",
    ESTADO: "estado",
    EPIN: "epin",
    TIPO: "tipo",
    SALDO: "saldo"
  };
  return map[h] || null;
}

function toNumOrNull(v) {
  const s = String(v ?? "").trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function isValidEpin(epin) {
  // solo dígitos, entre 1 y 32 
  return typeof epin === "string" && /^[0-9]{1,32}$/.test(epin);
}

function toStrOrNull(v) {
  const s = String(v ?? "").trim();
  return s ? s : null;
}

function isYes(v) {
  const s = String(v ?? "").trim().toUpperCase();
  return s === "SI" || s === "S" || s === "1" || s === "TRUE";
}

function chunkInsertSql(table, cols, rows) {
  const placeholdersRow = `(${cols.map(() => "?").join(",")})`;
  const placeholders = rows.map(() => placeholdersRow).join(",");
  const sql = `INSERT INTO ${table} (${cols.join(",")}) VALUES ${placeholders}`;
  const values = [];
  for (const r of rows) values.push(...r);
  return { sql, values };
}

async function loadCsvToStaging({ table, batchId, filePath, headerMapper, batchSize }) {
  const pool = getPool();

  // Descubre columnas reales de staging (por si difiere)
  const stagingCols = await getColumnsSet(table);

  // Armamos columnas objetivo: batch_id + las que existan
  const desired = { batch_id: true }; // siempre
  const rowsBuffer = [];
  let readRows = 0;
  let insertedRows = 0;
  let skippedRows = 0;

  // parser streaming
const parser = parse({
  bom: true,
  relax_quotes: true,
  relax_column_count: true,
  trim: true,
  skip_empty_lines: true,
  columns: (header) => header.map((h, i) => headerMapper(h) || `__skip_${i}`)
});

  const stream = fs.createReadStream(filePath);

  const insertBatch = async () => {
    if (!rowsBuffer.length) return;

    // columnas de insert se determinan por primera fila del buffer
    const cols = Object.keys(rowsBuffer[0]).filter((c) => stagingCols.has(c));
    // aseguramos batch_id primero
    const finalCols = ["batch_id", ...cols.filter((c) => c !== "batch_id")];

    const rows = rowsBuffer.map((obj) => finalCols.map((c) => obj[c] ?? null));
    const { sql, values } = chunkInsertSql(table, finalCols, rows);
    await pool.query(sql, values);

    insertedRows += rowsBuffer.length;
    rowsBuffer.length = 0;
  };

    return new Promise((resolve, reject) => {
    stream
        .pipe(parser)
        .on("data", async (record) => {
        readRows++;

        try {
            if (table === "stg_bdo") {
            const id_dms = toStrOrNull(record.id_dms);
            if (!id_dms) {
                skippedRows++;
                return;
            }

            // validar EPIN antes de meterlo al row
            const epin = toStrOrNull(record.epin);
            if (epin && !isValidEpin(epin)) {
                skippedRows++;
                return;
            }
            const lat = toNumOrNull(record.lat);
            const lon = toNumOrNull(record.lon);

            // validar rangos geográficos
            const safeLat = lat !== null && lat >= -90 && lat <= 90 ? lat : null;
            const safeLon = lon !== null && lon >= -180 && lon <= 180 ? lon : null;

            const row = {
              batch_id: batchId,
              id_dms,
              departamento: toStrOrNull(record.departamento),
              municipio: toStrOrNull(record.municipio),
              circuito: toStrOrNull(record.circuito),
              epin,
              es_epin: toStrOrNull(record.es_epin),
              estado_pdv: toStrOrNull(record.estado_pdv),
              nombre_pdv: toStrOrNull(record.nombre_pdv),
              direccion: toStrOrNull(record.direccion),
              categoria: toStrOrNull(record.categoria),
              lat: safeLat,
              lon: safeLon,
              propietario: toStrOrNull(record.propietario),
              distribuidor: toStrOrNull(record.distribuidor)
            };

            rowsBuffer.push(row);
            } else {
            // stg_2cnv
            const epin = toStrOrNull(record.epin);

            //  EPIN obligatorio y válido
            if (!epin || !isValidEpin(epin)) {
                skippedRows++;
                return;
            }

            const row = {
                batch_id: batchId,
                distribuidora: toStrOrNull(record.distribuidora),
                estado: toStrOrNull(record.estado),
                epin,
                tipo: toStrOrNull(record.tipo),
                saldo: toNumOrNull(record.saldo)
            };

            rowsBuffer.push(row);
            }

            if (rowsBuffer.length >= batchSize) {
            stream.pause();
            insertBatch()
                .then(() => stream.resume())
                .catch((e) => reject(e));
            }
        } catch (e) {
            reject(e);
        }
        })
        .on("end", async () => {
        try {
            await insertBatch();
            resolve({ readRows, insertedRows, skippedRows });
        } catch (e) {
            reject(e);
        }
        })
        .on("error", (err) => reject(err));
    });
}

async function runUpsertsAndSnapshots(batchId) {
  const pool = getPool();

  // Conteos previos (aprox insert vs update)
  const [[pdvDistinctRow]] = await pool.query(
    "SELECT COUNT(DISTINCT id_dms) AS cnt FROM stg_bdo WHERE batch_id=?",
    [batchId]
  );
  const pdvDistinct = pdvDistinctRow.cnt || 0;

  const [[pdvExistingRow]] = await pool.query(
    "SELECT COUNT(*) AS cnt FROM pdv WHERE id_dms IN (SELECT DISTINCT id_dms FROM stg_bdo WHERE batch_id=?)",
    [batchId]
  );
  const pdvExisting = pdvExistingRow.cnt || 0;

  const [[epinDistinctRow]] = await pool.query(
    "SELECT COUNT(DISTINCT epin) AS cnt FROM stg_bdo WHERE batch_id=? AND epin IS NOT NULL AND epin<>'' AND (UPPER(TRIM(es_epin))='SI' OR TRIM(es_epin)='1')",
    [batchId]
  );
  const epinDistinct = epinDistinctRow.cnt || 0;

  const [[epinExistingRow]] = await pool.query(
    "SELECT COUNT(*) AS cnt FROM epin WHERE epin IN (SELECT DISTINCT epin FROM stg_bdo WHERE batch_id=? AND epin IS NOT NULL AND epin<>'' AND (UPPER(TRIM(es_epin))='SI' OR TRIM(es_epin)='1'))",
    [batchId]
  );
  const epinExisting = epinExistingRow.cnt || 0;

  // 1) UPSERT PDV
  const upsertPdvSql = `
    INSERT INTO pdv (
      id_dms, nombre_pdv, categoria, propietario,
      circuito, distribuidor,
      departamento, municipio, direccion,
      lat, lon, estado_pdv,
      last_seen_batch_id, activo
    )
    SELECT
      x.id_dms,
      x.nombre_pdv,
      x.categoria,
      x.propietario,
      x.circuito,
      x.distribuidor,
      x.departamento,
      x.municipio,
      x.direccion,
      x.lat,
      x.lon,
      x.estado_pdv,
      x.batch_id,
      1
    FROM (
      SELECT
        batch_id,
        id_dms,
        MAX(nombre_pdv) AS nombre_pdv,
        MAX(categoria) AS categoria,
        MAX(propietario) AS propietario,
        MAX(circuito) AS circuito,
        MAX(distribuidor) AS distribuidor,
        MAX(departamento) AS departamento,
        MAX(municipio) AS municipio,
        MAX(direccion) AS direccion,
        MAX(lat) AS lat,
        MAX(lon) AS lon,
        MAX(estado_pdv) AS estado_pdv
      FROM stg_bdo
      WHERE batch_id = ? AND id_dms IS NOT NULL AND id_dms <> ''
      GROUP BY batch_id, id_dms
    ) x
    ON DUPLICATE KEY UPDATE
      nombre_pdv = VALUES(nombre_pdv),
      categoria = VALUES(categoria),
      propietario = VALUES(propietario),
      circuito = VALUES(circuito),
      distribuidor = VALUES(distribuidor),
      departamento = VALUES(departamento),
      municipio = VALUES(municipio),
      direccion = VALUES(direccion),
      lat = VALUES(lat),
      lon = VALUES(lon),
      estado_pdv = VALUES(estado_pdv),
      last_seen_batch_id = VALUES(last_seen_batch_id),
      activo = 1
  `;
  const [pdvRes] = await pool.query(upsertPdvSql, [batchId]);

  // 2) UPSERT EPIN
  const upsertEpinSql = `
    INSERT INTO epin (epin, pdv_id, estado_epin, activo, last_seen_batch_id)
    SELECT
      src.epin,
      src.pdv_id,
      src.estado_epin,
      1,
      src.batch_id
    FROM (
      -- A) EPINs que vienen en BDO
      SELECT
        b.batch_id,
        b.epin,
        p.pdv_id,
        CASE
          WHEN c.estado IN ('Active','ACTIVO') THEN 'ACTIVO'
          WHEN c.estado IN ('Suspended','SUSPENDED','BLOQUEADO','Blocked') THEN 'BLOQUEADO'
          WHEN c.estado IN ('Inactive','INACTIVO') THEN 'INACTIVO'
          ELSE 'ACTIVO'
        END AS estado_epin
      FROM stg_bdo b
      JOIN pdv p ON p.id_dms = b.id_dms
      LEFT JOIN (
        SELECT
          batch_id,
          epin,
          MAX(estado) AS estado
        FROM stg_2cnv
        WHERE batch_id = ?
          AND epin IS NOT NULL
          AND epin <> ''
        GROUP BY batch_id, epin
      ) c ON c.batch_id = b.batch_id AND c.epin = b.epin
      WHERE b.batch_id = ?
        AND b.epin IS NOT NULL
        AND b.epin <> ''
        AND (UPPER(TRIM(b.es_epin)) = 'SI' OR TRIM(b.es_epin) = '1')

      UNION

      -- B) EPINs que vienen solo en 2CNV (sin ID_DMS todavía)
      SELECT
        c.batch_id,
        c.epin,
        NULL AS pdv_id,
        CASE
          WHEN c.estado IN ('Active','ACTIVO') THEN 'ACTIVO'
          WHEN c.estado IN ('Suspended','SUSPENDED','BLOQUEADO','Blocked') THEN 'BLOQUEADO'
          WHEN c.estado IN ('Inactive','INACTIVO') THEN 'INACTIVO'
          ELSE 'ACTIVO'
        END AS estado_epin
      FROM (
        SELECT
          batch_id,
          epin,
          MAX(estado) AS estado
        FROM stg_2cnv
        WHERE batch_id = ?
          AND epin IS NOT NULL
          AND epin <> ''
        GROUP BY batch_id, epin
      ) c
      LEFT JOIN (
        SELECT DISTINCT epin
        FROM stg_bdo
        WHERE batch_id = ?
          AND epin IS NOT NULL
          AND epin <> ''
          AND (UPPER(TRIM(es_epin)) = 'SI' OR TRIM(es_epin) = '1')
      ) b ON b.epin = c.epin
      WHERE b.epin IS NULL
    ) src
    ON DUPLICATE KEY UPDATE
      pdv_id = COALESCE(VALUES(pdv_id), epin.pdv_id),
      estado_epin = VALUES(estado_epin),
      activo = 1,
      last_seen_batch_id = VALUES(last_seen_batch_id)
  `;
  const [epinRes] = await pool.query(upsertEpinSql, [batchId, batchId, batchId, batchId]);

  // 3) SNAPSHOT: para consistencia, limpiamos y regeneramos el batch
  await pool.query("DELETE FROM epin_snapshot WHERE batch_id=?", [batchId]);

  const insertSnapshotSql = `
    INSERT INTO epin_snapshot (
      batch_id, id_dms, epin, pdv_id, epin_id,
      estado_pdv, estado_epin, saldo_epin, features_json
    )
    SELECT
      src.batch_id,
      src.id_dms,
      src.epin,
      src.pdv_id,
      e.epin_id,
      src.estado_pdv,
      e.estado_epin,
      src.saldo_epin,
      src.features_json
    FROM (
      -- A) EPINs que sí vienen en BDO
      SELECT
        x.batch_id,
        x.id_dms,
        x.epin,
        p.pdv_id,
        x.estado_pdv,
        c.saldo AS saldo_epin,
        JSON_OBJECT(
          'distribuidora', c.distribuidora,
          'tipo', c.tipo,
          'tiene_id_dms', true,
          'origen', 'BDO'
        ) AS features_json
      FROM (
        SELECT
          batch_id,
          epin,
          MAX(id_dms) AS id_dms,
          MAX(estado_pdv) AS estado_pdv
        FROM stg_bdo
        WHERE batch_id = ?
          AND epin IS NOT NULL
          AND epin <> ''
          AND (UPPER(TRIM(es_epin)) = 'SI' OR TRIM(es_epin) = '1')
        GROUP BY batch_id, epin
      ) x
      JOIN pdv p ON p.id_dms = x.id_dms
      LEFT JOIN (
        SELECT
          batch_id,
          epin,
          MAX(saldo) AS saldo,
          MAX(distribuidora) AS distribuidora,
          MAX(tipo) AS tipo
        FROM stg_2cnv
        WHERE batch_id = ?
          AND epin IS NOT NULL
          AND epin <> ''
        GROUP BY batch_id, epin
      ) c ON c.batch_id = x.batch_id AND c.epin = x.epin

      UNION

      -- B) EPINs que vienen solo en 2CNV
      SELECT
        c.batch_id,
        NULL AS id_dms,
        c.epin,
        NULL AS pdv_id,
        NULL AS estado_pdv,
        c.saldo AS saldo_epin,
        JSON_OBJECT(
          'distribuidora', c.distribuidora,
          'tipo', c.tipo,
          'tiene_id_dms', false,
          'origen', '2CNV_ONLY'
        ) AS features_json
      FROM (
        SELECT
          batch_id,
          epin,
          MAX(saldo) AS saldo,
          MAX(distribuidora) AS distribuidora,
          MAX(tipo) AS tipo
        FROM stg_2cnv
        WHERE batch_id = ?
          AND epin IS NOT NULL
          AND epin <> ''
        GROUP BY batch_id, epin
      ) c
      LEFT JOIN (
        SELECT DISTINCT epin
        FROM stg_bdo
        WHERE batch_id = ?
          AND epin IS NOT NULL
          AND epin <> ''
          AND (UPPER(TRIM(es_epin)) = 'SI' OR TRIM(es_epin) = '1')
      ) b ON b.epin = c.epin
      WHERE b.epin IS NULL
    ) src
    JOIN epin e ON e.epin = src.epin
    ON DUPLICATE KEY UPDATE
      id_dms = VALUES(id_dms),
      pdv_id = VALUES(pdv_id),
      estado_pdv = VALUES(estado_pdv),
      estado_epin = VALUES(estado_epin),
      saldo_epin = VALUES(saldo_epin),
      features_json = VALUES(features_json)
  `;
  const [snapRes] = await pool.query(insertSnapshotSql, [batchId, batchId, batchId, batchId]);

  return {
    pdv: {
      distinctInFile: pdvDistinct,
      existedBefore: pdvExisting,
      insertedApprox: Math.max(0, pdvDistinct - pdvExisting),
      updatedApprox: Math.max(0, pdvExisting),
      affectedRows: pdvRes.affectedRows
    },
    epin: {
      distinctInFile: epinDistinct,
      existedBefore: epinExisting,
      insertedApprox: Math.max(0, epinDistinct - epinExisting),
      updatedApprox: Math.max(0, epinExisting),
      affectedRows: epinRes.affectedRows
    },
    snapshot: {
      insertedRows: snapRes.affectedRows
    }
  };
}

async function resetStaging(batchId) {
  const pool = getPool();
  await pool.query("TRUNCATE TABLE stg_bdo");
  await pool.query("TRUNCATE TABLE stg_2cnv");
}

async function runImportPipeline({
  batchId,
  asOfDate,
  bdoPath,
  cnvPath,
  bdoOriginalName,
  cnvOriginalName,
  userLabel
}) {
  const start = Date.now();

  try {
    await updateImportBatch(batchId, {
      status: "processing",
      error_message: null,
      file_name: `CORTE_${asOfDate}`,
      source: "manual_upload",
      source_path: `bdo=${bdoOriginalName};cnv=${cnvOriginalName}`,
      uploaded_by: userLabel || "unknown"
    });

    logger.info("Import pipeline start", { batchId, asOfDate });

    // limpia staging para reintentos (idempotente)
    await resetStaging();

    // carga a staging (batchSize ajustado por tamaño de filas)
    const bdoLoad = await loadDataToStaging({
      table: "stg_bdo",
      batchId,
      filePath: bdoPath
    });

    const cnvLoad = await loadDataToStaging({
      table: "stg_2cnv",
      batchId,
      filePath: cnvPath
    });

    // upserts + snapshots
    const upserts = await runUpsertsAndSnapshots(batchId);

    // conteo total filas staging (para reportar)
    const pool = getPool();
    const [[bdoCnt]] = await pool.query(
      "SELECT COUNT(*) AS cnt FROM stg_bdo WHERE batch_id=?",
      [batchId]
    );
    const [[cnvCnt]] = await pool.query(
      "SELECT COUNT(*) AS cnt FROM stg_2cnv WHERE batch_id=?",
      [batchId]
    );

    await updateImportBatch(batchId, {
      status: "done",
      total_rows: (bdoCnt.cnt || 0) + (cnvCnt.cnt || 0),
      error_rows: 0
    });

    const report = {
      batchId,
      asOfDate,
      files: {
        bdo: { originalName: bdoOriginalName, insertedStaging: bdoLoad.insertedRows },
        cnv: { originalName: cnvOriginalName, insertedStaging: cnvLoad.insertedRows }
      },
      upserts,
      durationMs: Date.now() - start
    };

    logger.info("Import pipeline done", { batchId, ms: report.durationMs });
    return report;
  } catch (err) {
    const msg = String(err?.message || err).slice(0, 800);
    await updateImportBatch(batchId, { status: "failed", error_message: msg });

    logger.error("Import pipeline failed", { batchId, error: msg });
    throw err;
  }
}

async function getBatchStatus(batchId) {
  const pool = getPool();
  const [rows] = await pool.query(
    "SELECT batch_id, status, error_message, as_of_date, created_at FROM import_batch WHERE batch_id=? LIMIT 1",
    [batchId]
  );
  return rows[0] || null;
}

async function loadDataToStaging({ table, batchId, filePath }) {
  const pool = getPool();
  const normalizedPath = path.resolve(filePath).replace(/\\/g, "/");

  if (table === "stg_bdo") {
    const sql = `
      LOAD DATA LOCAL INFILE '${normalizedPath}'
      INTO TABLE stg_bdo
      CHARACTER SET utf8mb4
      FIELDS TERMINATED BY ','
      OPTIONALLY ENCLOSED BY '"'
      LINES TERMINATED BY '\n'
      IGNORE 1 LINES
      (@ID, @DEPARTAMENTO, @MUNICIPIO, @CIRCUITO, @EPIN, @ES_EPIN, @ESTADO, @NOMBRE, @DIRECCION, @CATEGORIA, @X, @Y, @PROPIETARIO, @DISTRIBUIDOR)
      SET
        batch_id = ${Number(batchId)},
        id_dms = NULLIF(TRIM(@ID), ''),
        departamento = NULLIF(TRIM(@DEPARTAMENTO), ''),
        municipio = NULLIF(TRIM(@MUNICIPIO), ''),
        circuito = NULLIF(TRIM(@CIRCUITO), ''),
        epin = CASE
          WHEN TRIM(@EPIN) REGEXP '^[0-9]{1,32}$' THEN TRIM(@EPIN)
          ELSE NULL
        END,
        es_epin = NULLIF(TRIM(@ES_EPIN), ''),
        estado_pdv = NULLIF(TRIM(@ESTADO), ''),
        nombre_pdv = NULLIF(TRIM(@NOMBRE), ''),
        direccion = NULLIF(TRIM(@DIRECCION), ''),
        categoria = NULLIF(TRIM(@CATEGORIA), ''),
        lat = CASE
          WHEN NULLIF(TRIM(@X), '') IS NULL THEN NULL
          WHEN CAST(TRIM(@X) AS DECIMAL(10,6)) BETWEEN -90 AND 90 THEN CAST(TRIM(@X) AS DECIMAL(10,6))
          ELSE NULL
        END,
        lon = CASE
          WHEN NULLIF(TRIM(@Y), '') IS NULL THEN NULL
          WHEN CAST(TRIM(@Y) AS DECIMAL(10,6)) BETWEEN -180 AND 180 THEN CAST(TRIM(@Y) AS DECIMAL(10,6))
          ELSE NULL
        END,
        propietario = NULLIF(TRIM(@PROPIETARIO), ''),
        distribuidor = NULLIF(TRIM(@DISTRIBUIDOR), '')
    `;

    const [result] = await pool.query({
      sql,
      infileStreamFactory: (requestedPath) => {
        if (requestedPath !== normalizedPath) {
          throw new Error(`Ruta inesperada solicitada por LOCAL INFILE: ${requestedPath}`);
        }
        return fs.createReadStream(normalizedPath);
      }
    });

    return { insertedRows: result.affectedRows || 0 };
  }

  if (table === "stg_2cnv") {
    const sql = `
      LOAD DATA LOCAL INFILE '${normalizedPath}'
      INTO TABLE stg_2cnv
      CHARACTER SET utf8mb4
      FIELDS TERMINATED BY ','
      OPTIONALLY ENCLOSED BY '"'
      LINES TERMINATED BY '\n'
      IGNORE 1 LINES
      (@DISTRIBUIDORA, @ESTADO, @EPIN, @TIPO, @SALDO)
      SET
        batch_id = ${Number(batchId)},
        distribuidora = NULLIF(TRIM(@DISTRIBUIDORA), ''),
        estado = NULLIF(TRIM(@ESTADO), ''),
        epin = CASE
          WHEN TRIM(@EPIN) REGEXP '^[0-9]{1,32}$' THEN TRIM(@EPIN)
          ELSE NULL
        END,
        tipo = NULLIF(TRIM(@TIPO), ''),
        saldo = CASE
          WHEN NULLIF(TRIM(@SALDO), '') IS NULL THEN NULL
          ELSE CAST(TRIM(@SALDO) AS DECIMAL(12,2))
        END
    `;

    const [result] = await pool.query({
      sql,
      infileStreamFactory: (requestedPath) => {
        if (requestedPath !== normalizedPath) {
          throw new Error(`Ruta inesperada solicitada por LOCAL INFILE: ${requestedPath}`);
        }
        return fs.createReadStream(normalizedPath);
      }
    });

    return { insertedRows: result.affectedRows || 0 };
  }

  throw new Error(`Tabla staging no soportada: ${table}`);
}

module.exports = {
  validateAndGetFechaCorte,
  createOrGetBatch,
  runImportPipeline,
  getBatchStatus
};