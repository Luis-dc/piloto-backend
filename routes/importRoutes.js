const express = require("express");
const { uploadImport, getImportStatus, getImportHistory } = require("../controllers/importController");
const { uploadImportFiles } = require("../middlewares/uploadMiddleware");
const { verifyToken } = require("../middlewares/authMiddleware");

const router = express.Router();

function requireAdmin(req, res, next) {
  if (req.user?.role !== "ADMIN") {
    return res.status(403).json({ ok: false, error: "Solo ADMIN" });
  }
  next();
}

// POST /import  (form-data: bdo=file, cnv=file)
router.post("/import", verifyToken, requireAdmin, uploadImportFiles, uploadImport);
router.get("/import/status/:batchId", verifyToken, requireAdmin, getImportStatus);
router.get("/import/history", verifyToken, requireAdmin, getImportHistory);

module.exports = router;