const fs = require("fs");
const path = require("path");
const { once } = require("events");
const ExcelJS = require("exceljs");
const unzipper = require("unzipper");

const OUTPUT_HEADERS = [
  
  "EPIN",
  "SALDO"
];

const HEADER_ALIASES = {
  EPIN: [
    "EPIN"
  ],
  SALDO: [
    "SALDO"
  ]
};

async function readSharedStrings(filePath) {
  const sharedStrings = new Map();

  const directory = await unzipper.Open.file(
    filePath
  );

  const entry = directory.files.find(
    (file) =>
      file.path === "xl/sharedStrings.xml"
  );

  if (!entry) {
    return sharedStrings;
  }

  const xml = (
    await entry.buffer()
  ).toString("utf8");

  function decodeXml(text) {
    return String(text ?? "")
      .replace(
        /&#(\d+);/g,
        (_, code) =>
          String.fromCodePoint(
            Number(code)
          )
      )
      .replace(
        /&#x([0-9a-fA-F]+);/g,
        (_, code) =>
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

  const stringRegex =
    /<si\b[^>]*>([\s\S]*?)<\/si>/g;

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
 * Convierte los valores internos de ExcelJS
 * en valores simples.
 */
function normalizeExcelValue(
  value,
  sharedStrings = new Map()
) {
  if (
    value === null ||
    value === undefined
  ) {
    return "";
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (typeof value !== "object") {
    return value;
  }

  if (
    Object.prototype.hasOwnProperty.call(
      value,
      "sharedString"
    )
  ) {
    const index = Number(
      value.sharedString
    );

    return sharedStrings.has(index)
      ? sharedStrings.get(index)
      : "";
  }

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

  if (Array.isArray(value.richText)) {
    return value.richText
      .map((item) => item.text || "")
      .join("");
  }

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
 * Normaliza un encabezado para poder compararlo.
 */
function normalizeHeader(
  value,
  sharedStrings = new Map()
) {
  return String(
    normalizeExcelValue(
      value,
      sharedStrings
    )
  )
    .trim()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .toUpperCase();
}

/**
 * Encuentra la posición de cada columna necesaria
 * dentro de la primera fila.
 */
function buildColumnMap(
  headerRow,
  sharedStrings
) {
  const availableHeaders = new Map();

  for (
    let columnIndex = 1;
    columnIndex <= headerRow.cellCount;
    columnIndex += 1
  ) {
    const normalizedHeader = normalizeHeader(
      headerRow.getCell(columnIndex).value,
      sharedStrings
    );

    if (
      normalizedHeader &&
      !availableHeaders.has(normalizedHeader)
    ) {
      availableHeaders.set(
        normalizedHeader,
        columnIndex
      );
    }
  }

  const columnMap = {};
  const missingHeaders = [];

  for (const outputHeader of OUTPUT_HEADERS) {
    const aliases =
      HEADER_ALIASES[outputHeader] || [];

    let matchedColumnIndex = null;

    for (const alias of aliases) {
      const normalizedAlias =
        normalizeHeader(alias);

      if (
        availableHeaders.has(
          normalizedAlias
        )
      ) {
        matchedColumnIndex =
          availableHeaders.get(
            normalizedAlias
          );

        break;
      }
    }

    if (!matchedColumnIndex) {
      missingHeaders.push(outputHeader);
      continue;
    }

    columnMap[outputHeader] =
      matchedColumnIndex;
  }

  if (missingHeaders.length > 0) {
    const error = new Error(
      `El archivo 2CNV no contiene los encabezados obligatorios: ` +
      missingHeaders.join(", ")
    );

    error.statusCode = 400;
    throw error;
  }

  return columnMap;
}

/**
 * Obtiene el texto limpio de una celda.
 */
function getCellText(
  row,
  columnIndex,
  sharedStrings
) {
  return String(
    normalizeExcelValue(
      row.getCell(columnIndex).value,
      sharedStrings
    )
  ).trim();
}

/**
 * Determina si una fila está completamente vacía.
 */
function isEmptyRow(
  row,
  sharedStrings
) {
  for (
    let columnIndex = 1;
    columnIndex <= row.cellCount;
    columnIndex += 1
  ) {
    const value = getCellText(
      row,
      columnIndex,
      sharedStrings
    );

    if (value !== "") {
      return false;
    }
  }

  return true;
}

/**
 * Convierte EPIN en texto y valida
 * que contenga únicamente números.
 */
function normalizeEpin(
  value,
  sharedStrings
) {
  const normalizedValue =
    normalizeExcelValue(
      value,
      sharedStrings
    );

  if (typeof normalizedValue === "number") {
    if (
      !Number.isFinite(normalizedValue) ||
      !Number.isInteger(normalizedValue) ||
      normalizedValue < 0
    ) {
      return null;
    }

    return String(normalizedValue);
  }

  const text = String(
    normalizedValue
  ).trim();

  if (!/^\d+$/.test(text)) {
    return null;
  }

  return text;
}

/**
 * Convierte SALDO en número decimal.
 *
 * Acepta ejemplos como:
 * 100
 * 100.50
 * 100,50
 * 1,234.50
 * 1.234,50
 */
function normalizeBalance(
  value,
  sharedStrings
) {
  const normalizedValue =
    normalizeExcelValue(
      value,
      sharedStrings
    );

  if (typeof normalizedValue === "number") {
    if (!Number.isFinite(normalizedValue)) {
      return null;
    }

    return String(
      Object.is(normalizedValue, -0)
        ? 0
        : normalizedValue
    );
  }

  let text = String(
    normalizedValue
  )
    .trim()
    .replace(/\s+/g, "");

  if (!text) {
    return "";
  }

  const commaPosition =
    text.lastIndexOf(",");

  const dotPosition =
    text.lastIndexOf(".");

  if (
    commaPosition !== -1 &&
    dotPosition !== -1
  ) {
    if (commaPosition > dotPosition) {
      text = text
        .replace(/\./g, "")
        .replace(",", ".");
    } else {
      text = text.replace(/,/g, "");
    }
  } else if (commaPosition !== -1) {
    text = text.replace(",", ".");
  }

  if (
    !/^[+-]?(?:\d+\.?\d*|\.\d+)$/.test(
      text
    )
  ) {
    return null;
  }

  const numberValue = Number(text);

  if (!Number.isFinite(numberValue)) {
    return null;
  }

  return String(
    Object.is(numberValue, -0)
      ? 0
      : numberValue
  );
}
/**
 * Escapa correctamente valores para un CSV.
 */
function escapeCsvValue(value) {
  const text = String(value ?? "");

  if (
    text.includes(",") ||
    text.includes('"') ||
    text.includes("\n") ||
    text.includes("\r")
  ) {
    return `"${text.replace(/"/g, '""')}"`;
  }

  return text;
}

/**
 * Escribe una fila respetando el backpressure
 * del stream.
 */
async function writeCsvRow(
  outputStream,
  values
) {
  const line =
    values
      .map(escapeCsvValue)
      .join(",") + "\n";

  if (!outputStream.write(line)) {
    await once(outputStream, "drain");
  }
}

/**
 * Espera a que el stream se cierre.
 */
async function waitForStreamClose(stream) {
  if (stream.closed) {
    return;
  }

  await once(stream, "close").catch(
    () => {}
  );
}

/**
 * Limpia un archivo Excel 2CNV y genera
 * cnv-clean.csv dentro de la misma preparación.
 */
async function cleanCnvExcelFile(
  inputPath,
  outputPath = path.join(
    path.dirname(inputPath),
    "cnv-clean.csv"
  )
) {
    let inputStats;

    try {
      inputStats = await fs.promises.stat(
        inputPath
      );
    } catch (error) {
      if (error.code === "ENOENT") {
        const notFoundError = new Error(
          `No existe el archivo 2CNV indicado: ${inputPath}`
        );
  
        notFoundError.statusCode = 400;
        throw notFoundError;
      }
  
      throw error;
    }
  
    if (!inputStats.isFile()) {
      const error = new Error(
        `La ruta indicada no corresponde a un archivo: ${inputPath}`
      );
  
      error.statusCode = 400;
      throw error;
    }
  await fs.promises.mkdir(
    path.dirname(outputPath),
    {
      recursive: true
    }
  );

  const statistics = {
    inputRows: 0,
    outputRows: 0,
    excludedRows: 0,
    emptyRowsRemoved: 0,
    invalidEpinRows: 0,
    invalidBalanceRows: 0
  };

  const outputStream =
    fs.createWriteStream(outputPath, {
      encoding: "utf8"
    });

  let completed = false;

  try {
    await writeCsvRow(
      outputStream,
      OUTPUT_HEADERS
    );

    const sharedStrings =
      await readSharedStrings(inputPath);

    const workbookReader =
      new ExcelJS.stream.xlsx.WorkbookReader(
        inputPath,
        {
          entries: "emit",
          sharedStrings: "ignore",
          hyperlinks: "ignore",
          styles: "ignore",
          worksheets: "emit"
        }
      );

    let worksheetCount = 0;
    let headerFound = false;

    for await (
      const worksheetReader of workbookReader
    ) {
      worksheetCount += 1;

      if (worksheetCount > 1) {
        const error = new Error(
          "El archivo 2CNV debe contener una sola hoja"
        );

        error.statusCode = 400;
        throw error;
      }

      let columnMap = null;

      for await (
        const row of worksheetReader
      ) {
        if (row.number === 1) {
          columnMap = buildColumnMap(
            row,
            sharedStrings
          );
          headerFound = true;
          continue;
        }

        statistics.inputRows += 1;

        if (isEmptyRow(row)) {
          statistics.emptyRowsRemoved += 1;
          statistics.excludedRows += 1;
          continue;
        }

        const epin = normalizeEpin(
          row.getCell(
            columnMap.EPIN
          ).value,
          sharedStrings
        );

        const balance = normalizeBalance(
          row.getCell(
            columnMap.SALDO
          ).value,
          sharedStrings
        );

        let invalidRow = false;

        if (!epin) {
          statistics.invalidEpinRows += 1;
          invalidRow = true;
        }

        if (balance === null) {
            statistics.invalidBalanceRows += 1;
            invalidRow = true;
          }

        if (invalidRow) {
          statistics.excludedRows += 1;
          continue;
        }

        const cleanedRow = [
          epin,
          balance
        ];

        await writeCsvRow(
          outputStream,
          cleanedRow
        );

        statistics.outputRows += 1;
      }
    }

    if (worksheetCount === 0) {
      const error = new Error(
        "El archivo 2CNV no contiene hojas disponibles"
      );

      error.statusCode = 400;
      throw error;
    }

    if (!headerFound) {
      const error = new Error(
        "El archivo 2CNV no contiene encabezados en la fila 1"
      );

      error.statusCode = 400;
      throw error;
    }

    outputStream.end();
    await once(outputStream, "finish");

    completed = true;

    return {
      outputPath,
      outputName: path.basename(outputPath),
      statistics
    };
  } finally {
    if (!completed) {
      outputStream.destroy();

      await waitForStreamClose(
        outputStream
      );

      await fs.promises.rm(
        outputPath,
        {
          force: true
        }
      ).catch(() => {});
    }
  }
}

module.exports = {
  cleanCnvExcelFile
};