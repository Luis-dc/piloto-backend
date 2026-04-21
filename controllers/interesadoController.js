const interesadoService = require("../services/interesadoService");

async function getPeriodos(req, res, next) {
  try {
    const data = await interesadoService.getPeriodos();

    return res.json({
      ok: true,
      ...data
    });
  } catch (error) {
    next(error);
  }
}

async function getResumenPorEr(req, res, next) {
  try {
    const filters = {
      year: req.query.year || null,
      month: req.query.month || null
    };

    const result = await interesadoService.getResumenER(req.user, filters);

    return res.json({
      ok: true,
      ...result
    });
  } catch (error) {
    next(error);
  }
}

async function exportInteresados(req, res, next) {
  try {
    const filters = {
      createdByWebUserId: req.query.createdByWebUserId || null,
      year: req.query.year || null,
      month: req.query.month || null
    };

    const format = (req.query.format || "csv").toLowerCase();
    const file = await interesadoService.exportInteresados(req.user, filters, format);

    const scopeName = filters.createdByWebUserId ? "por-ejecutivo" : "general";
    const timestamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
    const fileName = `interesados-${scopeName}-${filters.month || "m"}-${filters.year || "y"}-${timestamp}.${file.extension}`;

    res.setHeader("Content-Type", file.contentType);
    res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);

    return res.send(file.buffer);
  } catch (error) {
    next(error);
  }
}

module.exports = {
  getPeriodos,
  getResumenPorEr,
  exportInteresados
};