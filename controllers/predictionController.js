const predictionService = require(
    "../services/predictionService"
  );
  
  
  // =========================================
  // RESUMEN DE PREDICCION
  // =========================================
  
async function getPredictionSummary(
    req,
    res,
    next
  ) {
    try {
      const data =
        await predictionService.getPredictionSummary(
          req.user
        );
  
      return res.json({
        ok: true,
        ...data
      });
    } catch (error) {
      next(error);
    }
  }

  async function getPredictionTop(
    req,
    res,
    next
  ) {
    try {
      const limit =
        req.query.limit || 20;
  
      const data =
        await predictionService.getPredictionTop(
          req.user,
          limit
        );
  
      return res.json({
        ok: true,
        ...data
      });
    } catch (error) {
      next(error);
    }
  }

  async function downloadPredictions(
    req,
    res,
    next
  ) {
    try {
      const result =
        await predictionService.exportPredictionsExcel(
          req.user
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
  
  
  module.exports = {
    getPredictionSummary,
    getPredictionTop,
    downloadPredictions
  };