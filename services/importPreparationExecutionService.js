const fs = require("fs");
const path = require("path");

const {
  getConfirmedPreparationForImport
} = require(
  "./importPreparationReviewService"
);

const {
    validateIncomingImportFiles,
    createOrGetBatch,
    runImportPipeline
  } = require("./importService");

const IMPORT_EXECUTIONS_ROOT =
  path.join(
    process.cwd(),
    "uploads",
    "import-executions"
  );

/**
 * Crea un error controlado.
 */
function createExecutionError(
  message,
  statusCode
) {
  const error = new Error(message);
  error.statusCode = statusCode;

  return error;
}

/**
 * Copia un CSV limpio a la carpeta estable
 * que utilizará el pipeline.
 */
async function copyCleanFile({
  sourceFile,
  executionDir,
  expectedName
}) {
  if (!sourceFile) {
    return null;
  }

  const destinationPath = path.join(
    executionDir,
    expectedName
  );

  await fs.promises.copyFile(
    sourceFile.path,
    destinationPath,
    fs.constants.COPYFILE_EXCL
  );

  const stats = await fs.promises.stat(
    destinationPath
  );

  if (
    !stats.isFile() ||
    stats.size !== sourceFile.sizeBytes
  ) {
    throw createExecutionError(
      `No fue posible preparar ${expectedName} para la importación`,
      500
    );
  }

  return {
    path: destinationPath,
    originalName:
      sourceFile.originalName,
    cleanedName:
      expectedName,
    sizeBytes:
      stats.size
  };
}

/**
 * Copia una preparación confirmada a una carpeta
 * estable antes de iniciar el pipeline.
 *
 * Todavía no crea batch ni modifica MySQL.
 */
async function stageConfirmedPreparation(
  preparationId
) {
  const confirmedPreparation =
    await getConfirmedPreparationForImport(
      preparationId
    );

  await fs.promises.mkdir(
    IMPORT_EXECUTIONS_ROOT,
    {
      recursive: true
    }
  );

  const executionDir = path.join(
    IMPORT_EXECUTIONS_ROOT,
    confirmedPreparation.preparationId
  );

  try {
    await fs.promises.mkdir(
      executionDir,
      {
        recursive: false
      }
    );
  } catch (error) {
    if (error.code === "EEXIST") {
      throw createExecutionError(
        "Esta preparación ya fue enviada para importación",
        409
      );
    }

    throw error;
  }

  try {
    /*
     * Se copian de forma secuencial para reducir
     * el consumo de memoria y disco simultáneo.
     */
    const bdoFile = await copyCleanFile({
      sourceFile:
        confirmedPreparation.files.bdo,
      executionDir,
      expectedName:
        "bdo-clean.csv"
    });

    const cnvFile = await copyCleanFile({
      sourceFile:
        confirmedPreparation.files.cnv,
      executionDir,
      expectedName:
        "cnv-clean.csv"
    });

    return {
      preparationId:
        confirmedPreparation.preparationId,
      preparationDir:
        confirmedPreparation.preparationDir,
      executionDir,
      asOfDate:
        confirmedPreparation.asOfDate,
      confirmation:
        confirmedPreparation.confirmation,
      files: {
        bdo: bdoFile,
        cnv: cnvFile
      }
    };
  } catch (error) {
    await fs.promises.rm(
      executionDir,
      {
        recursive: true,
        force: true
      }
    );

    throw error;
  }
}

/**
 * Elimina los archivos estables de una ejecución.
 */
async function removeImportExecutionDirectory(
  executionDir
) {
  if (!executionDir) {
    return;
  }

  await fs.promises.rm(
    executionDir,
    {
      recursive: true,
      force: true
    }
  );
}

/**
 * Elimina una carpeta sin interrumpir el resultado
 * principal de la importación.
 */
async function removeDirectorySafely(
    directoryPath,
    label
  ) {
    if (!directoryPath) {
      return;
    }
  
    try {
      await fs.promises.rm(
        directoryPath,
        {
          recursive: true,
          force: true
        }
      );
    } catch (error) {
      console.error(
        "[IMPORT_PREPARATION_CLEANUP_ERROR]",
        {
          label,
          directoryPath,
          message: error.message
        }
      );
    }
  }
  
  /**
   * Inicia el pipeline utilizando una preparación
   * previamente confirmada.
   */
  async function startConfirmedPreparationImport({
    preparationId,
    userLabel
  }) {
    let stagedPreparation = null;
  
    try {
      stagedPreparation =
        await stageConfirmedPreparation(
          preparationId
        );
  
      const bdoFile =
        stagedPreparation.files.bdo;
  
      const cnvFile =
        stagedPreparation.files.cnv;
  
      /*
       * Última barrera de seguridad antes de tocar
       * staging y crear o reutilizar el batch.
       */
      await validateIncomingImportFiles({
        bdoPath:
          bdoFile?.path || null,
  
        cnvPath:
          cnvFile?.path || null
      });
  
      const batch =
        await createOrGetBatch({
          asOfDate:
            stagedPreparation.asOfDate,
  
          bdoName:
            bdoFile?.originalName || null,
  
          cnvName:
            cnvFile?.originalName || null,
  
          userLabel:
            userLabel || "unknown"
        });
  
      /*
       * El pipeline continúa en segundo plano,
       * igual que la importación manual actual.
       */
      void runImportPipeline({
        batchId:
          batch.batchId,
  
        asOfDate:
          stagedPreparation.asOfDate,
  
        bdoPath:
          bdoFile?.path || null,
  
        cnvPath:
          cnvFile?.path || null,
  
        bdoOriginalName:
          bdoFile?.originalName || null,
  
        cnvOriginalName:
          cnvFile?.originalName || null,
  
        userLabel:
          userLabel || "unknown"
      })
        .then(async () => {
          /*
           * Si finaliza correctamente, ya no se
           * necesita la preparación ni sus copias.
           */
          await removeDirectorySafely(
            stagedPreparation.executionDir,
            "execution"
          );
  
          await removeDirectorySafely(
            stagedPreparation.preparationDir,
            "preparation"
          );
        })
        .catch(async (error) => {
          /*
           * Si falla, se elimina solo la copia de
           * ejecución. La preparación confirmada
           * permanece disponible para revisar o reintentar.
           */
          await removeDirectorySafely(
            stagedPreparation.executionDir,
            "execution"
          );
  
          console.error(
            "[CONFIRMED_PREPARATION_PIPELINE_ERROR]",
            {
              preparationId:
                stagedPreparation.preparationId,
  
              batchId:
                batch.batchId,
  
              message:
                error.message
            }
          );
        });
  
      return {
        preparationId:
          stagedPreparation.preparationId,
  
        batchId:
          batch.batchId,
  
        asOfDate:
          stagedPreparation.asOfDate,
  
        reusedBatch:
          Boolean(batch.reused),
  
        received: {
          bdo: Boolean(bdoFile),
          cnv: Boolean(cnvFile)
        }
      };
    } catch (error) {
      /*
       * Si el pipeline todavía no comenzó, se elimina
       * cualquier copia parcial de ejecución.
       */
      if (stagedPreparation?.executionDir) {
        await removeDirectorySafely(
          stagedPreparation.executionDir,
          "execution"
        );
      }
  
      throw error;
    }
  }

module.exports = {
  stageConfirmedPreparation,
  startConfirmedPreparationImport,
  removeImportExecutionDirectory
};