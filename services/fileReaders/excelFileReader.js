const ExcelJS = require("exceljs");
const unzipper = require("unzipper");

const MAX_PREVIEW_ROWS = 20;
const MAX_PREVIEW_COLUMNS = 100;

/**
 * Convierte valores internos de ExcelJS
 * en valores simples que puedan enviarse como JSON.
 */
function normalizeExcelValue(
  value,
  sharedStrings = new Map()
) {
  if (value === null || value === undefined) {
    return "";
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (typeof value !== "object") {
    return value;
  }

  // Referencia a sharedStrings.xml.
  if (
    Object.prototype.hasOwnProperty.call(
      value,
      "sharedString"
    )
  ) {
    const index = Number(value.sharedString);

    if (sharedStrings.has(index)) {
      return normalizeExcelValue(
        sharedStrings.get(index),
        sharedStrings
      );
    }

    return "";
  }

  // Celda con fórmula.
  if (
    Object.prototype.hasOwnProperty.call(
      value,
      "formula"
    )
  ) {
    return normalizeExcelValue(
      value.result,
      sharedStrings
    );
  }

  // Texto enriquecido.
  if (Array.isArray(value.richText)) {
    return value.richText
      .map((item) => item.text || "")
      .join("");
  }

  // Hipervínculo.
  if (
    Object.prototype.hasOwnProperty.call(
      value,
      "text"
    )
  ) {
    return normalizeExcelValue(
      value.text,
      sharedStrings
    );
  }

  // Error de Excel.
  if (
    Object.prototype.hasOwnProperty.call(
      value,
      "error"
    )
  ) {
    return String(value.error);
  }

  return String(value);
}

/**
 * Convierte una fila de ExcelJS en un arreglo normal.
 */
function normalizeExcelRow(
  row,
  sharedStrings = new Map()
) {
  const values = [];

  const columnLimit = Math.min(
    row.cellCount,
    MAX_PREVIEW_COLUMNS
  );

  for (
    let column = 1;
    column <= columnLimit;
    column += 1
  ) {
    values.push(
      normalizeExcelValue(
        row.getCell(column).value,
        sharedStrings
      )
    );
  }

  return values;
}


async function readSharedStrings(filePath) {
  const sharedStrings = new Map();

  const directory = await unzipper.Open.file(filePath);

  const entry = directory.files.find(
    (file) => file.path === "xl/sharedStrings.xml"
  );

  if (!entry) {
    return sharedStrings;
  }

  const xml = (
    await entry.buffer()
  ).toString("utf8");

  function decodeXml(text) {
    return String(text ?? "")
      .replace(/&#(\d+);/g, (_, code) =>
        String.fromCodePoint(Number(code))
      )
      .replace(/&#x([0-9a-fA-F]+);/g, (_, code) =>
        String.fromCodePoint(
          parseInt(code, 16)
        )
      )
      .replace(/&amp;/g, "&")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"')
      .replace(/&apos;/g, "'");
  }

  const stringRegex = /<si\b[^>]*>([\s\S]*?)<\/si>/g;

  let match;
  let index = 0;

  while (
    (match = stringRegex.exec(xml)) !== null
  ) {
    const content = match[1];

    const parts = [];
    const textRegex =
      /<t\b[^>]*>([\s\S]*?)<\/t>/g;

    let textMatch;

    while (
      (textMatch =
        textRegex.exec(content)) !== null
    ) {
      parts.push(
        decodeXml(textMatch[1])
      );
    }

    sharedStrings.set(
      index,
      parts.join("")
    );

    index += 1;
  }

  return sharedStrings;
}
/**
 * Normaliza encabezados para compararlos.
 *
 * Ejemplos:
 * "es_epin"   → "ES EPIN"
 * "Dirección" → "DIRECCION"
 */
function normalizeHeader(value) {
  return String(value ?? "")
    .trim()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .toUpperCase();
}

/**
 * Valida únicamente la primera fila del Excel.
 */
function analyzeHeaderRow(
  headerValues,
  expectedHeaderGroups = []
) {
  const normalizedValues = new Set(
    headerValues
      .map(normalizeHeader)
      .filter(Boolean)
  );

  let matchedHeaders = 0;

  for (const aliases of expectedHeaderGroups) {
    const matched = aliases.some((alias) =>
      normalizedValues.has(
        normalizeHeader(alias)
      )
    );

    if (matched) {
      matchedHeaders += 1;
    }
  }

  return {
    headerRow: 1,
    matchedHeaders,
    expectedHeaders: expectedHeaderGroups.length,
    completeMatch:
      expectedHeaderGroups.length > 0 &&
      matchedHeaders === expectedHeaderGroups.length
  };
}

/**
 * Inspecciona un archivo XLSX mediante streaming.
 *
 * Reglas de plantilla:
 * - Debe contener exactamente una hoja.
 * - Los encabezados deben estar en la fila 1.
 * - Los datos deben comenzar en la fila 2.
 */
async function inspectExcelFile(
  filePath,
  options = {}
) {
  const expectedHeaderGroups =
    options.expectedHeaderGroups || [];

  const sharedStrings =
    await readSharedStrings(filePath);

  const workbookReader =
    new ExcelJS.stream.xlsx.WorkbookReader(
      filePath,
      {
        entries: "emit",
        sharedStrings: "ignore",
        hyperlinks: "ignore",
        styles: "ignore",
        worksheets: "emit"
      }
    );

  const sheets = [];
  const sheetAnalysis = [];

  for await (
    const worksheetReader of workbookReader
  ) {
    let rowCount = 0;
    let maxColumnCount = 0;

    const previewRows = [];
    let firstRowValues = [];

    for await (const row of worksheetReader) {
      rowCount += 1;

      maxColumnCount = Math.max(
        maxColumnCount,
        row.cellCount
      );

      const normalizedRow =
        normalizeExcelRow(
          row,
          sharedStrings
        );

      if (row.number === 1) {
        firstRowValues = normalizedRow;
      }

      if (
        previewRows.length <
        MAX_PREVIEW_ROWS
      ) {
        previewRows.push({
          rowNumber: row.number,
          values: normalizedRow
        });
      }
    }

    const headerAnalysis =
      analyzeHeaderRow(
        firstRowValues,
        expectedHeaderGroups
      );

    const sheetInfo = {
      name: worksheetReader.name,
      rowCount,
      maxColumnCount,
      headerRow: 1,
      matchedHeaders:
        headerAnalysis.matchedHeaders,
      expectedHeaders:
        headerAnalysis.expectedHeaders,
      completeHeaderMatch:
        headerAnalysis.completeMatch
    };

    sheets.push(sheetInfo);

    sheetAnalysis.push({
      name: worksheetReader.name,
      previewRows,
      headerAnalysis
    });
  }

  if (sheets.length === 0) {
    const error = new Error(
      "El archivo Excel no contiene hojas disponibles"
    );

    error.statusCode = 400;
    throw error;
  }

  if (sheets.length !== 1) {
    const error = new Error(
      "El archivo Excel debe contener una sola hoja"
    );

    error.statusCode = 400;
    throw error;
  }

  const selectedSheet = sheets[0];
  const selectedAnalysis = sheetAnalysis[0];

  return {
    format: "xlsx",
    sheets,
    selectedSheet: selectedSheet.name,

    // Ya no existe selección manual.
    requiresSheetSelection: false,

    // La plantilla exige encabezados en la fila 1.
    headerRow: 1,

    matchedHeaders:
      selectedAnalysis
        .headerAnalysis
        .matchedHeaders,

    expectedHeaders:
      selectedAnalysis
        .headerAnalysis
        .expectedHeaders,

    completeHeaderMatch:
      selectedAnalysis
        .headerAnalysis
        .completeMatch,

    /*
     * Ya no se permitirá configurar manualmente
     * otra fila de encabezados.
     */
    requiresHeaderConfiguration: false,

    templateValid:
      selectedAnalysis
        .headerAnalysis
        .completeMatch,

    previewRows:
      selectedAnalysis.previewRows
  };
}

module.exports = {
  inspectExcelFile
};