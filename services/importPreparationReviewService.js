const fs = require("fs");
const path = require("path");

const {
  PREPARATIONS_ROOT
} = require("../config/importPreparationConfig");

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

/**
 * Crea un error controlado para el endpoint.
 */
function createServiceError(
  message,
  statusCode
) {
  const error = new Error(message);
  error.statusCode = statusCode;

  return error;
}

/**
 * Valida el UUID y construye la ruta segura
 * de una preparación.
 */
function getPreparationDirectory(
  preparationId
) {
  const normalizedId = String(
    preparationId || ""
  ).trim();

  if (!UUID_PATTERN.test(normalizedId)) {
    throw createServiceError(
      "El identificador de preparación no es válido",
      400
    );
  }

  return {
    preparationId: normalizedId,
    preparationDir: path.join(
      PREPARATIONS_ROOT,
      normalizedId
    )
  };
}

/**
 * Verifica que la preparación exista y que sea
 * una carpeta real, no un enlace simbólico.
 */
async function validatePreparationDirectory(
  preparationDir
) {
  let stats;

  try {
    stats = await fs.promises.lstat(
      preparationDir
    );
  } catch (error) {
    if (error.code === "ENOENT") {
      throw createServiceError(
        "La preparación no existe o ya expiró",
        404
      );
    }

    throw error;
  }

  if (
    !stats.isDirectory() ||
    stats.isSymbolicLink()
  ) {
    throw createServiceError(
      "La preparación solicitada no es válida",
      400
    );
  }
}

/**
 * Obtiene información de un CSV limpio
 * cuando está disponible.
 */
async function getCleanFileInfo(
  preparationDir,
  fileName
) {
  const filePath = path.join(
    preparationDir,
    fileName
  );

  try {
    const stats = await fs.promises.stat(
      filePath
    );

    if (!stats.isFile()) {
      return null;
    }

    return {
      storedName: fileName,
      sizeBytes: stats.size
    };
  } catch (error) {
    if (error.code === "ENOENT") {
      return null;
    }

    throw error;
  }
}

/**
 * Lee una confirmación existente.
 */
async function readExistingConfirmation(
  confirmationPath
) {
  try {
    const content = await fs.promises.readFile(
      confirmationPath,
      "utf8"
    );

    return JSON.parse(content);
  } catch (error) {
    if (error.code === "ENOENT") {
      return null;
    }

    if (error instanceof SyntaxError) {
      throw createServiceError(
        "El archivo de confirmación está dañado",
        500
      );
    }

    throw error;
  }
}

/**
 * Conserva únicamente datos seguros del
 * administrador que confirma.
 */
function normalizeConfirmedBy(
  confirmedBy = {}
) {
  return {
    userId:
      confirmedBy.userId ??
      confirmedBy.id ??
      null,

    name:
      confirmedBy.name ??
      confirmedBy.nombre ??
      null,

    email:
      confirmedBy.email ??
      confirmedBy.correo ??
      null,

    role:
      confirmedBy.role ??
      confirmedBy.rol ??
      null
  };
}

/**
 * Confirma una preparación ya analizada y limpiada.
 *
 * No crea batch ni modifica la base de datos.
 */
async function confirmPreparation({
  preparationId,
  confirmedBy
}) {
  const {
    preparationDir
  } = getPreparationDirectory(
    preparationId
  );

  await validatePreparationDirectory(
    preparationDir
  );

  const confirmationPath = path.join(
    preparationDir,
    "confirmation.json"
  );

  const existingConfirmation =
    await readExistingConfirmation(
      confirmationPath
    );

  if (existingConfirmation) {
    return {
      alreadyConfirmed: true,
      confirmation: existingConfirmation
    };
  }

  const [
    bdoCleanFile,
    cnvCleanFile
  ] = await Promise.all([
    getCleanFileInfo(
      preparationDir,
      "bdo-clean.csv"
    ),

    getCleanFileInfo(
      preparationDir,
      "cnv-clean.csv"
    )
  ]);

  if (!bdoCleanFile && !cnvCleanFile) {
    throw createServiceError(
      "La preparación no contiene archivos CSV limpios para confirmar",
      400
    );
  }

  const confirmation = {
    preparationId,
    status: "CONFIRMED",
    confirmedAt: new Date().toISOString(),
    confirmedBy: normalizeConfirmedBy(
      confirmedBy
    ),
    files: {
      bdo: bdoCleanFile,
      cnv: cnvCleanFile
    }
  };

  try {
    await fs.promises.writeFile(
      confirmationPath,
      JSON.stringify(
        confirmation,
        null,
        2
      ),
      {
        encoding: "utf8",
        flag: "wx"
      }
    );
  } catch (error) {
    /*
     * Si dos solicitudes confirman al mismo tiempo,
     * se devuelve la confirmación ya creada.
     */
    if (error.code === "EEXIST") {
      const savedConfirmation =
        await readExistingConfirmation(
          confirmationPath
        );

      return {
        alreadyConfirmed: true,
        confirmation: savedConfirmation
      };
    }

    throw error;
  }

  return {
    alreadyConfirmed: false,
    confirmation
  };
}

/**
 * Lee un archivo JSON obligatorio.
 */
async function readRequiredJsonFile({
  filePath,
  missingMessage,
  invalidMessage
}) {
  try {
    const content =
      await fs.promises.readFile(
        filePath,
        "utf8"
      );

    const data = JSON.parse(content);

    if (
      !data ||
      typeof data !== "object" ||
      Array.isArray(data)
    ) {
      throw createServiceError(
        invalidMessage,
        500
      );
    }

    return data;
  } catch (error) {
    if (error.code === "ENOENT") {
      throw createServiceError(
        missingMessage,
        409
      );
    }

    if (error instanceof SyntaxError) {
      throw createServiceError(
        invalidMessage,
        500
      );
    }

    throw error;
  }
}

/**
 * Verifica que una fecha tenga el formato
 * YYYY-MM-DD y que sea una fecha real.
 */
function isValidAsOfDate(value) {
  const text = String(
    value || ""
  ).trim();

  const match =
    /^(\d{4})-(\d{2})-(\d{2})$/.exec(
      text
    );

  if (!match) {
    return false;
  }

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);

  const date = new Date(
    Date.UTC(
      year,
      month - 1,
      day
    )
  );

  return (
    date.getUTCFullYear() === year &&
    date.getUTCMonth() === month - 1 &&
    date.getUTCDate() === day
  );
}

/**
 * Verifica un CSV limpio antes de entregarlo
 * al pipeline de importación.
 */
async function getCleanFileForImport({
  preparationDir,
  fileMetadata,
  confirmationFile,
  expectedName,
  label
}) {
  if (!fileMetadata) {
    if (confirmationFile) {
      throw createServiceError(
        `La confirmación contiene un archivo ${label} inesperado`,
        409
      );
    }

    return null;
  }

  if (
    fileMetadata.cleanedName !==
    expectedName
  ) {
    throw createServiceError(
      `El nombre del archivo limpio ${label} no coincide con la preparación`,
      409
    );
  }

  if (
    !fileMetadata.originalName ||
    typeof fileMetadata.originalName !==
      "string"
  ) {
    throw createServiceError(
      `No se encontró el nombre original del archivo ${label}`,
      409
    );
  }

  if (
    !confirmationFile ||
    confirmationFile.storedName !==
      expectedName
  ) {
    throw createServiceError(
      `El archivo ${label} no fue incluido correctamente en la confirmación`,
      409
    );
  }

  const filePath = path.join(
    preparationDir,
    expectedName
  );

  let stats;

  try {
    stats = await fs.promises.lstat(
      filePath
    );
  } catch (error) {
    if (error.code === "ENOENT") {
      throw createServiceError(
        `El archivo limpio ${label} ya no existe`,
        409
      );
    }

    throw error;
  }

  if (
    !stats.isFile() ||
    stats.isSymbolicLink()
  ) {
    throw createServiceError(
      `El archivo limpio ${label} no es válido`,
      400
    );
  }

  if (
    Number.isFinite(
      confirmationFile.sizeBytes
    ) &&
    confirmationFile.sizeBytes !==
      stats.size
  ) {
    throw createServiceError(
      `El archivo limpio ${label} cambió después de ser confirmado`,
      409
    );
  }

  if (
    Number.isFinite(
      fileMetadata.cleanedSizeBytes
    ) &&
    fileMetadata.cleanedSizeBytes !==
      stats.size
  ) {
    throw createServiceError(
      `El tamaño del archivo limpio ${label} no coincide con la preparación`,
      409
    );
  }

  return {
    path: filePath,
    originalName:
      fileMetadata.originalName,
    cleanedName: expectedName,
    sizeBytes: stats.size
  };
}

/**
 * Obtiene una preparación confirmada y lista
 * para enviarse al pipeline.
 *
 * No crea batch ni modifica la base de datos.
 */
async function getConfirmedPreparationForImport(
  preparationId
) {
  const {
    preparationId: normalizedId,
    preparationDir
  } = getPreparationDirectory(
    preparationId
  );

  await validatePreparationDirectory(
    preparationDir
  );

  const preparationMetadata =
    await readRequiredJsonFile({
      filePath: path.join(
        preparationDir,
        "preparation.json"
      ),

      missingMessage:
        "La preparación no contiene preparation.json",

      invalidMessage:
        "El archivo preparation.json está dañado"
    });

  const confirmation =
    await readRequiredJsonFile({
      filePath: path.join(
        preparationDir,
        "confirmation.json"
      ),

      missingMessage:
        "La preparación todavía no ha sido confirmada",

      invalidMessage:
        "El archivo confirmation.json está dañado"
    });

  if (
    preparationMetadata.preparationId !==
      normalizedId ||
    confirmation.preparationId !==
      normalizedId
  ) {
    throw createServiceError(
      "Los identificadores de la preparación no coinciden",
      409
    );
  }

  if (
    preparationMetadata.status !==
    "PREPARED"
  ) {
    throw createServiceError(
      "La metadata de la preparación no tiene un estado válido",
      409
    );
  }

  if (
    confirmation.status !==
    "CONFIRMED"
  ) {
    throw createServiceError(
      "La preparación todavía no está confirmada",
      409
    );
  }

  if (
    !isValidAsOfDate(
      preparationMetadata.asOfDate
    )
  ) {
    throw createServiceError(
      "La fecha de corte de la preparación no es válida",
      409
    );
  }

  const bdoMetadata =
    preparationMetadata.files?.bdo ||
    null;

  const cnvMetadata =
    preparationMetadata.files?.cnv ||
    null;

  if (!bdoMetadata && !cnvMetadata) {
    throw createServiceError(
      "La preparación no contiene archivos para importar",
      409
    );
  }

  const [
    bdoFile,
    cnvFile
  ] = await Promise.all([
    getCleanFileForImport({
      preparationDir,
      fileMetadata: bdoMetadata,
      confirmationFile:
        confirmation.files?.bdo ||
        null,
      expectedName:
        "bdo-clean.csv",
      label: "BDO"
    }),

    getCleanFileForImport({
      preparationDir,
      fileMetadata: cnvMetadata,
      confirmationFile:
        confirmation.files?.cnv ||
        null,
      expectedName:
        "cnv-clean.csv",
      label: "2CNV"
    })
  ]);

  return {
    preparationId: normalizedId,
    preparationDir,
    asOfDate:
      preparationMetadata.asOfDate,
    preparation:
      preparationMetadata,
    confirmation,
    files: {
      bdo: bdoFile,
      cnv: cnvFile
    }
  };
}

/**
 * Descarta una preparación y elimina
 * toda su carpeta temporal.
 */
async function discardPreparation(
  preparationId
) {
  const {
    preparationDir
  } = getPreparationDirectory(
    preparationId
  );

  await validatePreparationDirectory(
    preparationDir
  );

  await fs.promises.rm(
    preparationDir,
    {
      recursive: true,
      force: true
    }
  );

  return {
    preparationId,
    discarded: true
  };
}

module.exports = {
  confirmPreparation,
  discardPreparation,
  getConfirmedPreparationForImport
};