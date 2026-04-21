const analyticsService = require("../services/analyticsService");

async function getEpinSummary(req, res, next) {
  try {
    const data = await analyticsService.getEpinSummary();

    return res.json({
      ok: true,
      ...data
    });
  } catch (error) {
    next(error);
  }
}

async function getEpinRecency(req, res, next) {
  try {
    const data = await analyticsService.getEpinRecencyDistribution();

    return res.json({
      ok: true,
      ...data
    });
  } catch (error) {
    next(error);
  }
}

async function getEpinSegments(req, res, next) {
    try {
      const groupBy = req.query.groupBy || "departamento";
      const data = await analyticsService.getEpinSegments(groupBy);
  
      return res.json({
        ok: true,
        ...data
      });
    } catch (error) {
      next(error);
    }
  }

  async function downloadEpinSegments(req, res, next) {
    try {
      const groupBy = req.query.groupBy || "departamento";
      const result = await analyticsService.exportBlockedInactivePdvsExcel(groupBy);
  
      res.setHeader(
        "Content-Type",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
      );
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="${result.fileName}"`
      );
  
      return res.send(Buffer.from(result.content));
    } catch (error) {
      next(error);
    }
  }

/*Tendencias*/

async function getEpinTrends(req, res, next) {
    try {
      const limit = Number(req.query.limit || 12);
      const data = await analyticsService.getEpinTrendSeries(limit);
  
      return res.json({
        ok: true,
        ...data
      });
    } catch (error) {
      next(error);
    }
  }
  
  async function getEpinTrendComparison(req, res, next) {
    try {
      const data = await analyticsService.getEpinTrendComparison();
  
      return res.json({
        ok: true,
        ...data
      });
    } catch (error) {
      next(error);
    }
  }

  module.exports = {
  getEpinSummary,
  getEpinRecency,
  getEpinSegments,
  downloadEpinSegments,
  getEpinTrends,
  getEpinTrendComparison
};