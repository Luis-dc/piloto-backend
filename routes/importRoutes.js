const express = require("express");
const { uploadImport, getImportStatus } = require("../controllers/importController");
const { uploadImportFiles } = require("../middlewares/uploadMiddleware");
const { authMiddleware } = require("../middlewares/authMiddleware");

const router = express.Router();

function requireAdmin(req, res, next) {
  if (req.user?.role !== "ADMIN") {
    return res.status(403).json({ ok: false, error: "Solo ADMIN" });
  }
  next();
}

// POST /import  (form-data: bdo=file, cnv=file)
router.post("/import", authMiddleware, requireAdmin, uploadImportFiles, uploadImport);
router.get("/import/status/:batchId", authMiddleware, requireAdmin, getImportStatus);

module.exports = router;