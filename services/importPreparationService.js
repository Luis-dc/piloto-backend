const fs = require("fs");
const path = require("path");

const {
  inspectExcelFile
} = require("./fileReaders/excelFileReader");

const {
    cleanBdoExcelFile
  } = require("./fileCleaners/bdoFileCleaner");
  
  const {
    cleanCnvExcelFile
  } = require("./fileCleaners/cnvFileCleaner");

  const {
    resolveAsOfDateFromOptionalFiles
  } = require("./importService");

const ALLOWED_FIELDS = new Set(["bdo", "cnv"]);

/*
 * Encabezados requeridos del BDO.
 *
 * Cada arreglo representa una columna obligatoria y sus
 * equivalencias controladas.
 */
const BDO_HEADER_GROUPS = [
  ["ID", "ID DMS"],
  ["DEPARTAMENTO"],
  ["MUNICIPIO"],
  ["CIRCUITO"],
  ["EPIN"],
  ["ES EPIN"],
  ["ESTADO", "ESTADO PDV"],
  ["NOMBRE", "NOMBRE PDV"],
  ["DIRECCION"],
  ["CATEGORIA"],
  ["X", "LATITUD", "LAT"],
  ["Y", "LONGITUD", "LON"],
  ["PROPIETARIO"],
  ["DISTRIBUIDOR"]
];

/*
 * Encabezados requeridos del 2CNV.
 *
 * El archivo real puede traer DISTRIBUIDOR, pero el
 * resultado final se normalizará como DISTRIBUIDORA
 * durante el Sprint E2.
 */
const CNV_HEADER_GROUPS = [
  ["EPIN"],
  ["SALDO"]
];

/**
 * Convierte la información de Multer en un resumen seguro
 * para devolver al frontend.
 */
function buildUploadedFileInfo(file) {
  if (!file) {
    return null;
  }

  return {
    field: file.fieldname,
    originalName: file.originalname,
    storedName: file.filename,
    extension: path
      .extname(file.originalname || "")
      .toLowerCase(),
    sizeBytes: file.size,
    mimeType: file.mimetype
  };
}

/**
 * Obtiene un archivo de req.files según su campo.
 */
function getUploadedFile(files, fieldName) {
  if (!ALLOWED_FIELDS.has(fieldName)) {
    return null;
  }

  return files?.[fieldName]?.[0] || null;
}

/**
 * Construye el resumen inicial de la preparación.
 */
function createPreparationSummary({
  preparationId,
  files
}) {
  const bdoFile = getUploadedFile(files, "bdo");
  const cnvFile = getUploadedFile(files, "cnv");

  if (!bdoFile && !cnvFile) {
    const error = new Error(
      "Debes seleccionar al menos un archivo: BDO o 2CNV"
    );

    error.statusCode = 400;
    throw error;
  }

  return {
    preparationId,
    received: {
      bdo: Boolean(bdoFile),
      cnv: Boolean(cnvFile)
    },
    files: {
      bdo: buildUploadedFileInfo(bdoFile),
      cnv: buildUploadedFileInfo(cnvFile)
    }
  };
}

/**
 * Obtiene los encabezados esperados según el campo
 * donde se cargó el archivo.
 */
function getExpectedHeaderGroups(expectedType) {
  if (expectedType === "bdo") {
    return BDO_HEADER_GROUPS;
  }

  if (expectedType === "cnv") {
    return CNV_HEADER_GROUPS;
  }

  const error = new Error(
    `Tipo de archivo no reconocido: ${expectedType}`
  );

  error.statusCode = 400;
  throw error;
}

/**
 * Analiza un archivo Excel según el tipo esperado.
 */
async function analyzeUploadedFile(
    file,
    expectedType
  ) {
    if (!file) {
      return null;
    }
  
    const extension = path
      .extname(file.originalname || "")
      .toLowerCase();
  
    if (extension !== ".xlsx") {
      const error = new Error(
        `Formato no permitido: ${extension || "desconocido"}. ` +
        "Solo se permiten archivos Excel .xlsx"
      );
  
      error.statusCode = 400;
      throw error;
    }
  
    const expectedHeaderGroups =
      getExpectedHeaderGroups(expectedType);
  
    const analysis = await inspectExcelFile(
      file.path,
      {
        expectedHeaderGroups
      }
    );
  
    if (!analysis.templateValid) {
      const fileLabel =
        expectedType === "bdo"
          ? "BDO"
          : "2CNV";
  
      const error = new Error(
        `El archivo ${fileLabel} no cumple la plantilla requerida. ` +
        `Se reconocieron ${analysis.matchedHeaders} de ` +
        `${analysis.expectedHeaders} encabezados obligatorios. ` +
        "Los encabezados deben encontrarse en la fila 1."
      );
  
      error.statusCode = 400;
      throw error;
    }
  
    return analysis;
  }

/**
 * Analiza BDO y 2CNV de una misma preparación.
 */
async function analyzePreparationFiles(files) {
    const bdoFile = getUploadedFile(
      files,
      "bdo"
    );
  
    const cnvFile = getUploadedFile(
      files,
      "cnv"
    );
  
    const bdoAnalysis = bdoFile
      ? await analyzeUploadedFile(
          bdoFile,
          "bdo"
        )
      : null;
  
    const cnvAnalysis = cnvFile
      ? await analyzeUploadedFile(
          cnvFile,
          "cnv"
        )
      : null;
  
    return {
      bdo: bdoAnalysis,
      cnv: cnvAnalysis
    };
  }

/**
 * Construye información segura del CSV generado
 * sin exponer rutas internas del servidor.
 */
async function buildCleanedFileInfo(
    cleaningResult
  ) {
    if (!cleaningResult) {
      return null;
    }
  
    const fileStats = await fs.promises.stat(
      cleaningResult.outputPath
    );
  
    return {
      storedName: cleaningResult.outputName,
      extension: ".csv",
      sizeBytes: fileStats.size,
      statistics: cleaningResult.statistics
    };
  }
  
  /**
   * Limpia los archivos válidos de una preparación.
   *
   * Genera:
   * - bdo-clean.csv
   * - cnv-clean.csv
   */
  async function cleanPreparationFiles(files) {
    const bdoFile = getUploadedFile(
      files,
      "bdo"
    );
  
    const cnvFile = getUploadedFile(
      files,
      "cnv"
    );
  
    const bdoCleaningResult = bdoFile
      ? await cleanBdoExcelFile(
          bdoFile.path
        )
      : null;
  
    const cnvCleaningResult = cnvFile
      ? await cleanCnvExcelFile(
          cnvFile.path
        )
      : null;
  
    const bdoCleanedFile =
      await buildCleanedFileInfo(
        bdoCleaningResult
      );
  
    const cnvCleanedFile =
      await buildCleanedFileInfo(
        cnvCleaningResult
      );
  
    return {
      bdo: bdoCleanedFile,
      cnv: cnvCleanedFile
    };
  }

/**
 * Elimina una preparación temporal completa.
 */
function removePreparationDirectory(preparationDir) {
  if (!preparationDir) {
    return;
  }

  fs.rmSync(preparationDir, {
    recursive: true,
    force: true
  });
}

/**
 * Guarda los datos necesarios para ejecutar posteriormente
 * la importación de los CSV limpios.
 *
 * No crea batch ni modifica la base de datos.
 */
async function savePreparationMetadata({
  preparationId,
  preparationDir,
  files,
  cleanedFiles
}) {
  const bdoFile = getUploadedFile(
    files,
    "bdo"
  );

  const cnvFile = getUploadedFile(
    files,
    "cnv"
  );

  if (!preparationId || !preparationDir) {
    const error = new Error(
      "No fue posible identificar la preparación"
    );

    error.statusCode = 500;
    throw error;
  }

  const asOfDate =
    resolveAsOfDateFromOptionalFiles({
      bdoOriginalName:
        bdoFile?.originalname || null,

      cnvOriginalName:
        cnvFile?.originalname || null
    });

  const metadata = {
    preparationId,
    status: "PREPARED",
    asOfDate,
    preparedAt: new Date().toISOString(),

    files: {
      bdo: bdoFile
        ? {
            originalName:
              bdoFile.originalname,

            uploadedName:
              bdoFile.filename,

            cleanedName:
              cleanedFiles?.bdo
                ?.storedName || null,

            originalSizeBytes:
              bdoFile.size,

            cleanedSizeBytes:
              cleanedFiles?.bdo
                ?.sizeBytes || null
          }
        : null,

      cnv: cnvFile
        ? {
            originalName:
              cnvFile.originalname,

            uploadedName:
              cnvFile.filename,

            cleanedName:
              cleanedFiles?.cnv
                ?.storedName || null,

            originalSizeBytes:
              cnvFile.size,

            cleanedSizeBytes:
              cleanedFiles?.cnv
                ?.sizeBytes || null
          }
        : null
    }
  };

  const metadataPath = path.join(
    preparationDir,
    "preparation.json"
  );

  await fs.promises.writeFile(
    metadataPath,
    JSON.stringify(
      metadata,
      null,
      2
    ),
    {
      encoding: "utf8",
      flag: "wx"
    }
  );

  return metadata;
}

module.exports = {
  createPreparationSummary,
  analyzePreparationFiles,
  cleanPreparationFiles,
  savePreparationMetadata,
  getUploadedFile,
  removePreparationDirectory
};