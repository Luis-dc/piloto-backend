const fs = require("fs");
const path = require("path");
const { once } = require("events");
const ExcelJS = require("exceljs");

const OUTPUT_HEADERS = [
  "ID",
  "DEPARTAMENTO",
  "MUNICIPIO",
  "CIRCUITO",
  "EPIN",
  "ES EPIN",
  "ESTADO",
  "NOMBRE",
  "DIRECCION",
  "CATEGORIA",
  "X",
  "Y",
  "PROPIETARIO",
  "DISTRIBUIDOR"
];

const HEADER_ALIASES = {
  ID: ["ID", "ID DMS"],
  DEPARTAMENTO: ["DEPARTAMENTO"],
  MUNICIPIO: ["MUNICIPIO"],
  CIRCUITO: ["CIRCUITO"],
  EPIN: ["EPIN"],
  "ES EPIN": ["ES EPIN"],
  ESTADO: ["ESTADO", "ESTADO PDV"],
  NOMBRE: ["NOMBRE", "NOMBRE PDV"],
  DIRECCION: ["DIRECCION"],
  CATEGORIA: ["CATEGORIA"],
  X: ["X", "LATITUD", "LAT"],
  Y: ["Y", "LONGITUD", "LON"],
  PROPIETARIO: ["PROPIETARIO"],
  DISTRIBUIDOR: ["DISTRIBUIDOR"]
};

function normalizeExcelValue(value) {
  if (value === null || value === undefined) {
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
      "formula"
    )
  ) {
    return normalizeExcelValue(value.result);
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
    return normalizeExcelValue(value.text);
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

function normalizeHeader(value) {
  return String(
    normalizeExcelValue(value)
  )
    .trim()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .toUpperCase();
}

function buildColumnMap(headerRow) {
  const availableHeaders = new Map();

  for (
    let columnIndex = 1;
    columnIndex <= headerRow.cellCount;
    columnIndex += 1
  ) {
    const normalizedHeader = normalizeHeader(
      headerRow.getCell(columnIndex).value
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
      "El archivo BDO no contiene los encabezados obligatorios: " +
      missingHeaders.join(", ")
    );

    error.statusCode = 400;
    throw error;
  }

  return columnMap;
}

function getCellText(row, columnIndex) {
  return String(
    normalizeExcelValue(
      row.getCell(columnIndex).value
    )
  ).trim();
}

/**
 * Considera vacía una fila cuando todas las columnas
 * utilizadas por el BDO están vacías.
 */
function isMappedRowEmpty(
  row,
  columnMap
) {
  for (const header of OUTPUT_HEADERS) {
    const value = getCellText(
      row,
      columnMap[header]
    );

    if (value !== "") {
      return false;
    }
  }

  return true;
}

/**
 * Convierte una coordenada en número.
 *
 * X se interpreta como latitud:
 * -90 a 90.
 *
 * Y se interpreta como longitud:
 * -180 a 180.
 *
 * Las coordenadas vacías son permitidas.
 */
function normalizeCoordinate(
  value,
  minimum,
  maximum
) {
  const normalizedValue =
    normalizeExcelValue(value);

  if (typeof normalizedValue === "number") {
    if (
      !Number.isFinite(normalizedValue) ||
      normalizedValue < minimum ||
      normalizedValue > maximum
    ) {
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

  if (
    !Number.isFinite(numberValue) ||
    numberValue < minimum ||
    numberValue > maximum
  ) {
    return null;
  }

  return String(
    Object.is(numberValue, -0)
      ? 0
      : numberValue
  );
}

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

async function waitForStreamClose(stream) {
  if (stream.closed) {
    return;
  }

  await once(stream, "close").catch(
    () => {}
  );
}

/**
 * Limpia un archivo BDO XLSX y genera
 * bdo-clean.csv dentro de la misma preparación.
 */
async function cleanBdoExcelFile(
  inputPath,
  outputPath = path.join(
    path.dirname(inputPath),
    "bdo-clean.csv"
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
        `No existe el archivo BDO indicado: ${inputPath}`
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
    rowsWithoutEpin: 0,
    invalidXCoordinates: 0,
    invalidYCoordinates: 0
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

    const workbookReader =
      new ExcelJS.stream.xlsx.WorkbookReader(
        inputPath,
        {
          entries: "emit",
          sharedStrings: "cache",
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
          "El archivo BDO debe contener una sola hoja"
        );

        error.statusCode = 400;
        throw error;
      }

      let columnMap = null;

      for await (
        const row of worksheetReader
      ) {
        if (row.number === 1) {
          columnMap = buildColumnMap(row);
          headerFound = true;
          continue;
        }

        if (!columnMap) {
          const error = new Error(
            "El archivo BDO debe contener los encabezados en la fila 1"
          );

          error.statusCode = 400;
          throw error;
        }

        statistics.inputRows += 1;

        if (
          isMappedRowEmpty(
            row,
            columnMap
          )
        ) {
          statistics.emptyRowsRemoved += 1;
          statistics.excludedRows += 1;
          continue;
        }

        const epin = getCellText(
          row,
          columnMap.EPIN
        );

        if (!epin) {
          statistics.rowsWithoutEpin += 1;
        }

        const xCoordinate =
          normalizeCoordinate(
            row.getCell(
              columnMap.X
            ).value,
            -90,
            90
          );

        const yCoordinate =
          normalizeCoordinate(
            row.getCell(
              columnMap.Y
            ).value,
            -180,
            180
          );

        if (xCoordinate === null) {
          statistics.invalidXCoordinates += 1;
        }

        if (yCoordinate === null) {
          statistics.invalidYCoordinates += 1;
        }

        const cleanedRow = [
          getCellText(
            row,
            columnMap.ID
          ),
          getCellText(
            row,
            columnMap.DEPARTAMENTO
          ),
          getCellText(
            row,
            columnMap.MUNICIPIO
          ),
          getCellText(
            row,
            columnMap.CIRCUITO
          ),
          epin,
          getCellText(
            row,
            columnMap["ES EPIN"]
          ),
          getCellText(
            row,
            columnMap.ESTADO
          ),
          getCellText(
            row,
            columnMap.NOMBRE
          ),
          getCellText(
            row,
            columnMap.DIRECCION
          ),
          getCellText(
            row,
            columnMap.CATEGORIA
          ),
          xCoordinate === null
            ? ""
            : xCoordinate,
          yCoordinate === null
            ? ""
            : yCoordinate,
          getCellText(
            row,
            columnMap.PROPIETARIO
          ),
          getCellText(
            row,
            columnMap.DISTRIBUIDOR
          )
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
        "El archivo BDO no contiene hojas disponibles"
      );

      error.statusCode = 400;
      throw error;
    }

    if (!headerFound) {
      const error = new Error(
        "El archivo BDO no contiene encabezados en la fila 1"
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
  cleanBdoExcelFile
};