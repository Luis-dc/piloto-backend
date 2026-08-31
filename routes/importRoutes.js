const express = require("express");

const {
  uploadImport,
  getImportStatus,
  getImportHistory
} = require("../controllers/importController");

const {
  prepareImportFiles,
  confirmPreparationController,
  discardPreparationController,
  startConfirmedPreparationImportController
} = require("../controllers/importPreparationController");

const {
  uploadImportFiles
} = require("../middlewares/uploadMiddleware");

const {
  uploadPreparationFiles
} = require("../middlewares/importPreparationUploadMiddleware");

const {
  verifyToken
} = require("../middlewares/authMiddleware");

const router = express.Router();

function requireAdmin(req, res, next) {
  if (req.user?.role !== "ADMIN") {
    return res.status(403).json({
      ok: false,
      error: "Solo ADMIN"
    });
  }

  next();
}

/*
 * Preparación de archivos originales.
 *
 * Recibe:
 * - .xlsx
 *
 * No crea batch ni modifica la base de datos.
 */
router.post(
  "/import/prepare",
  verifyToken,
  requireAdmin,
  uploadPreparationFiles,
  prepareImportFiles
);

/*
 * Confirma una preparación previamente
 * analizada y limpiada.
 *
 * No crea batch ni modifica la base de datos.
 */
router.post(
  "/import/preparations/:preparationId/confirm",
  verifyToken,
  requireAdmin,
  confirmPreparationController
);

/*
 * Inicia el pipeline utilizando una preparación
 * previamente confirmada.
 */
router.post(
  "/import/preparations/:preparationId/import",
  verifyToken,
  requireAdmin,
  startConfirmedPreparationImportController
);

/*
 * Descarta una preparación temporal
 * y elimina sus archivos.
 */
router.delete(
  "/import/preparations/:preparationId",
  verifyToken,
  requireAdmin,
  discardPreparationController
);

/*
 * Importación actual.
 *
 * Continúa recibiendo únicamente los CSV
 * preparados para el pipeline existente.
 */
router.post(
  "/import",
  verifyToken,
  requireAdmin,
  uploadImportFiles,
  uploadImport
);

router.get(
  "/import/status/:batchId",
  verifyToken,
  requireAdmin,
  getImportStatus
);

router.get(
  "/import/history",
  verifyToken,
  requireAdmin,
  getImportHistory
);

module.exports = router;