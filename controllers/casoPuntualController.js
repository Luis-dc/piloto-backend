const casoPuntualService = require("../services/casoPuntualService");

async function getPeriodos(req, res, next) {
  try {
    const data = await casoPuntualService.getPeriodos();

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

    const result = await casoPuntualService.getResumenER(req.user, filters);

    return res.json({
      ok: true,
      ...result
    });
  } catch (error) {
    next(error);
  }
}

async function exportCasosPuntuales(req, res, next) {
  try {
    const filters = {
      createdByWebUserId: req.query.createdByWebUserId || null,
      year: req.query.year || null,
      month: req.query.month || null
    };

    const file = await casoPuntualService.exportCasosPuntuales(
      req.user,
      filters
    );

    const scopeName = filters.createdByWebUserId ? "por-ejecutivo" : "general";
    const timestamp = new Date()
      .toISOString()
      .slice(0, 19)
      .replace(/[:T]/g, "-");

    const fileName = `casos-puntuales-${scopeName}-${filters.month || "m"}-${filters.year || "y"}-${timestamp}.${file.extension}`;

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
  exportCasosPuntuales
};