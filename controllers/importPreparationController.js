const {
    createPreparationSummary,
    analyzePreparationFiles,
    cleanPreparationFiles,
    savePreparationMetadata,
    removePreparationDirectory
  } = require("../services/importPreparationService");
const {
  PREPARATION_TTL_MS
} = require("../config/importPreparationConfig");

const {
  confirmPreparation:
    confirmPreparationService,
  discardPreparation:
    discardPreparationService
} = require(
  "../services/importPreparationReviewService"
);

const {
  startConfirmedPreparationImport
} = require(
  "../services/importPreparationExecutionService"
);

const {
  analyzeCnvVolumeSafety
} = require("../services/importCnvSafetyService");
  
  /**
   * Recibe y analiza los archivos originales.
   *
   * En esta etapa:
   * - No crea import_batch.
   * - No ejecuta el pipeline.
   * - No modifica la base de datos.
   */
  async function prepareImportFiles(req, res) {
    try {
      const summary = createPreparationSummary({
        preparationId: req.preparationId,
        files: req.files
      });
  
      const analysis = await analyzePreparationFiles(
        req.files
      );

      const cleanedFiles = await cleanPreparationFiles(
        req.files
      );

      let cnvSafety = null;

      if (cleanedFiles?.cnv?.storedName) {
        cnvSafety = await analyzeCnvVolumeSafety({
          preparationDir: req.preparationDir,
          cleanedFileName: cleanedFiles.cnv.storedName
        });
      }

      const preparationMetadata =
        await savePreparationMetadata({
          preparationId:
            summary.preparationId,

          preparationDir:
            req.preparationDir,

          files:
            req.files,

          cleanedFiles
        });
  
      const expiresAt = new Date(
        Date.now() + PREPARATION_TTL_MS
      ).toISOString();
  
      return res.status(201).json({
        ok: true,
        message: "Archivos recibidos, analizados y limpiados",
        preparationId: summary.preparationId,
        expiresAt,
        received: summary.received,
        files: summary.files,
        analysis,
        cleanedFiles,
        cnvSafety,
        preparation: preparationMetadata
      });
    } catch (error) {
      removePreparationDirectory(req.preparationDir);
  
      console.error(
        "[IMPORT_PREPARATION_ERROR]",
        error
      );
  
      return res.status(error.statusCode || 500).json({
        ok: false,
        error:
          error.message ||
          "No fue posible analizar los archivos"
      });
    }
  }

  /**
 * Confirma una preparación ya limpiada.
 *
 * No crea batch ni modifica la base de datos.
 */
async function confirmPreparationController(
  req,
  res
) {
  try {
    const result =
      await confirmPreparationService({
        preparationId:
          req.params.preparationId,
        confirmedBy: req.user
      });

    return res
      .status(
        result.alreadyConfirmed
          ? 200
          : 201
      )
      .json({
        ok: true,
        message: result.alreadyConfirmed
          ? "La preparación ya estaba confirmada"
          : "Preparación confirmada correctamente",
        alreadyConfirmed:
          result.alreadyConfirmed,
        confirmation:
          result.confirmation
      });
  } catch (error) {
    console.error(
      "[IMPORT_PREPARATION_CONFIRM_ERROR]",
      error
    );

    return res
      .status(error.statusCode || 500)
      .json({
        ok: false,
        error:
          error.message ||
          "No fue posible confirmar la preparación"
      });
  }
}

/**
 * Descarta una preparación temporal.
 */
async function discardPreparationController(
  req,
  res
) {
  try {
    const result =
      await discardPreparationService(
        req.params.preparationId
      );

    return res.status(200).json({
      ok: true,
      message:
        "Preparación descartada correctamente",
      preparationId:
        result.preparationId,
      discarded:
        result.discarded
    });
  } catch (error) {
    console.error(
      "[IMPORT_PREPARATION_DISCARD_ERROR]",
      error
    );

    return res
      .status(error.statusCode || 500)
      .json({
        ok: false,
        error:
          error.message ||
          "No fue posible descartar la preparación"
      });
  }
}

/**
 * Inicia la importación utilizando los CSV
 * de una preparación previamente confirmada.
 */
async function startConfirmedPreparationImportController(
  req,
  res
) {
  try {
    const userLabel =
      req.user?.email ||
      req.user?.uname ||
      req.user?.name ||
      "unknown";

    const result =
      await startConfirmedPreparationImport({
        preparationId:
          req.params.preparationId,
        userLabel
      });

    return res.status(202).json({
      ok: true,
      message:
        "Importación iniciada desde la preparación confirmada",
      preparationId:
        result.preparationId,
      batchId:
        result.batchId,
      asOfDate:
        result.asOfDate,
      reusedBatch:
        result.reusedBatch,
      received:
        result.received
    });
  } catch (error) {
    console.error(
      "[CONFIRMED_PREPARATION_IMPORT_ERROR]",
      error
    );

    return res
      .status(error.statusCode || 500)
      .json({
        ok: false,
        error:
          error.message ||
          "No fue posible iniciar la importación confirmada"
      });
  }
}
  
  module.exports = {
    prepareImportFiles,
    confirmPreparationController,
    discardPreparationController,
    startConfirmedPreparationImportController
  };