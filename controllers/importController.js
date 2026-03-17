const {
  validateAndGetFechaCorte,
  createOrGetBatch,
  runImportPipeline,
  getBatchStatus
} = require("../services/importService");

async function uploadImport(req, res) {
  const bdo = req.files?.bdo?.[0];
  const cnv = req.files?.cnv?.[0];

  if (!bdo || !cnv) {
    return res.status(400).json({
      ok: false,
      error: "Debes enviar 2 archivos: bdo (BDO.csv) y cnv (2CNV.csv)"
    });
  }

  try {
    // B.2
    const asOfDate = validateAndGetFechaCorte(bdo.originalname, cnv.originalname);

    // B.3
    const userLabel = req.user?.email || req.user?.uname || req.user?.name || "unknown";
    const batch = await createOrGetBatch({
      asOfDate,
      bdoName: bdo.originalname,
      cnvName: cnv.originalname,
      userLabel
    });

    // B.4 (import completo)
    runImportPipeline({
      batchId: batch.batchId,
      asOfDate,
      bdoPath: bdo.path,
      cnvPath: cnv.path,
      bdoOriginalName: bdo.originalname,
      cnvOriginalName: cnv.originalname,
      userLabel
    })
      .then(() => {})
      .catch(() => {});

    return res.status(202).json({
      ok: true,
      message: "Import iniciado",
      batchId: batch.batchId,
      asOfDate
    });
  } catch (err) {
    return res.status(400).json({ ok: false, error: err.message });
  }
}

async function getImportStatus(req, res) {
  const batchId = Number(req.params.batchId);

  if (!Number.isFinite(batchId)) {
    return res.status(400).json({ ok: false, error: "batchId inválido" });
  }

  const batch = await getBatchStatus(batchId);

  if (!batch) {
    return res.status(404).json({ ok: false, error: "Batch no encontrado" });
  }

  return res.json({ ok: true, batch });
}

module.exports = { uploadImport, getImportStatus };