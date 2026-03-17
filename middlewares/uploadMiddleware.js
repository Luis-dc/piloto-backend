const fs = require("fs");
const path = require("path");
const multer = require("multer");

const uploadDir = path.join(process.cwd(), "uploads");
fs.mkdirSync(uploadDir, { recursive: true });

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => {
    const safeName = file.originalname.replace(/[^a-zA-Z0-9._-]/g, "_");
    cb(null, `${Date.now()}__${safeName}`);
  }
});

function fileFilter(req, file, cb) {
  const ext = path.extname(file.originalname || "").toLowerCase();
  if (ext !== ".csv") {
    return cb(new Error(`Archivo inválido: ${file.originalname}. Solo se permite .csv`));
  }
  cb(null, true);
}

const upload = multer({
  storage,
  fileFilter,
  limits: {
    fileSize: 200 * 1024 * 1024 // 200MB
  }
});

// Esperamos 2 archivos: bdo y cnv
const uploadImportFiles = upload.fields([
  { name: "bdo", maxCount: 1 },
  { name: "cnv", maxCount: 1 }
]);

module.exports = { uploadImportFiles };