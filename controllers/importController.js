const {
  resolveAsOfDateFromOptionalFiles,
  createOrGetBatch,
  runImportPipeline,
  getBatchStatus,
  listImportBatches
} = require("../services/importService");

async function uploadImport(req, res) {
  const bdo = req.files?.bdo?.[0] || null;
  const cnv = req.files?.cnv?.[0] || null;

  if (!bdo && !cnv) {
    return res.status(400).json({
      ok: false,
      error: "Debes enviar al menos 1 archivo: bdo o cnv"
    });
  }

  try {
    const asOfDate = resolveAsOfDateFromOptionalFiles({
      bdoOriginalName: bdo?.originalname || null,
      cnvOriginalName: cnv?.originalname || null
    });

    const userLabel = req.user?.email || req.user?.uname || req.user?.name || "unknown";

    const batch = await createOrGetBatch({
      asOfDate,
      bdoName: bdo?.originalname || null,
      cnvName: cnv?.originalname || null,
      userLabel
    });

    runImportPipeline({
      batchId: batch.batchId,
      asOfDate,
      bdoPath: bdo?.path || null,
      cnvPath: cnv?.path || null,
      bdoOriginalName: bdo?.originalname || null,
      cnvOriginalName: cnv?.originalname || null,
      userLabel
    })
      .then(() => {})
      .catch(() => {});

    return res.status(202).json({
      ok: true,
      message: "Import iniciado",
      batchId: batch.batchId,
      asOfDate,
      received: {
        bdo: Boolean(bdo),
        cnv: Boolean(cnv)
      }
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

async function getImportHistory(req, res) {
  try {
    const limit = Number(req.query.limit || 20);
    const items = await listImportBatches(limit);

    return res.json({
      ok: true,
      items
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: "No se pudo obtener el historial de importaciones"
    });
  }
}

module.exports = { uploadImport, getImportStatus, getImportHistory };