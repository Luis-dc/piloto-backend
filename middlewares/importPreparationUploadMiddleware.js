const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const multer = require("multer");
const {
    PREPARATIONS_ROOT
  } = require("../config/importPreparationConfig");

const ALLOWED_EXTENSION = ".xlsx";
const MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024; // 50 MB por archivo

fs.mkdirSync(PREPARATIONS_ROOT, { recursive: true });

/**
 * Crea una carpeta temporal única para cada preparación.
 * BDO y 2CNV quedan dentro de la misma carpeta.
 */
function ensurePreparationDirectory(req) {
  if (req.preparationId && req.preparationDir) {
    return;
  }

  const preparationId = crypto.randomUUID();

  const preparationDir = path.join(
    PREPARATIONS_ROOT,
    preparationId
  );

  fs.mkdirSync(preparationDir, { recursive: true });

  req.preparationId = preparationId;
  req.preparationDir = preparationDir;
}

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    try {
      ensurePreparationDirectory(req);
      cb(null, req.preparationDir);
    } catch (error) {
      cb(error);
    }
  },

  filename: (req, file, cb) => {
    cb(null, `${file.fieldname}-original.xlsx`);
  }
});

function fileFilter(req, file, cb) {
  const extension = path
    .extname(file.originalname || "")
    .toLowerCase();

  if (extension !== ALLOWED_EXTENSION) {
    return cb(
      new Error(
        `Archivo inválido: ${file.originalname}. ` +
        "Solo se permiten archivos Excel en formato .xlsx"
      )
    );
  }

  cb(null, true);
}

const preparationUpload = multer({
  storage,
  fileFilter,
  limits: {
    fileSize: MAX_FILE_SIZE_BYTES,
    files: 2
  }
}).fields([
  { name: "bdo", maxCount: 1 },
  { name: "cnv", maxCount: 1 }
]);

function removePreparationDirectory(req) {
  if (!req.preparationDir) {
    return;
  }

  try {
    fs.rmSync(req.preparationDir, {
      recursive: true,
      force: true
    });
  } catch (error) {
    console.error(
      "[PREPARATION_TEMP_CLEANUP_ERROR]",
      error.message
    );
  }
}

/**
 * Ejecuta Multer y devuelve errores controlados en JSON.
 */
function uploadPreparationFiles(req, res, next) {
  preparationUpload(req, res, (error) => {
    if (!error) {
      return next();
    }

    removePreparationDirectory(req);

    if (error instanceof multer.MulterError) {
      if (error.code === "LIMIT_FILE_SIZE") {
        return res.status(413).json({
          ok: false,
          error: "Cada archivo puede tener un máximo de 50 MB"
        });
      }

      if (error.code === "LIMIT_UNEXPECTED_FILE") {
        return res.status(400).json({
          ok: false,
          error:
            "Solo se permite un archivo BDO y un archivo 2CNV"
        });
      }

      if (error.code === "LIMIT_FILE_COUNT") {
        return res.status(400).json({
          ok: false,
          error:
            "Solo se permite cargar un máximo de dos archivos"
        });
      }

      return res.status(400).json({
        ok: false,
        error: `Error al recibir archivos: ${error.message}`
      });
    }

    return res.status(400).json({
      ok: false,
      error:
        error.message ||
        "No fue posible recibir los archivos seleccionados"
    });
  });
}

module.exports = {
  uploadPreparationFiles
};