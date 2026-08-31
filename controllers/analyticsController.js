const analyticsService = require(
  "../services/analyticsService"
);

const epinAnalyticsService = require(
  "../services/epinAnalyticsService"
);


// =========================================
// ANÁLISIS EPIN
// =========================================

async function getEpinCuts(req, res, next) {
  try {
    const items =
      await epinAnalyticsService.getAvailableEpinCuts();

    return res.json({
      ok: true,
      items
    });
  } catch (error) {
    next(error);
  }
}


async function getEpinSummary(req, res, next) {
  try {
    const batchId = req.query.batchId;

    const data =
      await epinAnalyticsService.getEpinAnalysisSummary(
        batchId
      );

    return res.json({
      ok: true,
      ...data
    });
  } catch (error) {
    next(error);
  }
}


async function getEpinEvents(req, res, next) {
  try {
    const batchId = req.query.batchId;
    const eventType = req.query.type;

    const data =
      await epinAnalyticsService.getEpinEventPreview(
        batchId,
        eventType,
        20
      );

    return res.json({
      ok: true,
      ...data
    });
  } catch (error) {
    next(error);
  }
}


async function downloadEpinEvents(
  req,
  res,
  next
) {
  try {
    const batchId = req.query.batchId;
    const eventType = req.query.type;

    const result =
      await epinAnalyticsService.exportEpinEventsExcel(
        batchId,
        eventType
      );

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );

    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${result.fileName}"`
    );

    return res.send(
      Buffer.from(result.content)
    );
  } catch (error) {
    next(error);
  }
}


// =========================================
// TENDENCIAS EPIN
// =========================================

async function getEpinTrends(
  req,
  res,
  next
) {
  try {
    const period =
      req.query.period || "12";

    const data =
      await analyticsService.getEpinTrendSeries(
        period
      );

    return res.json({
      ok: true,
      ...data
    });
  } catch (error) {
    next(error);
  }
}


module.exports = {
  getEpinCuts,
  getEpinSummary,
  getEpinEvents,
  downloadEpinEvents,
  getEpinTrends
};