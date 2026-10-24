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
      const risk =
        String(req.query.risk || "")
          .trim()
          .toUpperCase();
  
      const allowedRisks = [
        "VERDE",
        "AMARILLO",
        "ROJO"
      ];
  
      if (!allowedRisks.includes(risk)) {
        return res.status(400).json({
          ok: false,
          error:
            "Debe indicar risk=VERDE, AMARILLO o ROJO"
        });
      }
  
      res.setHeader(
        "Content-Type",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
      );
  
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="predicciones_${risk.toLowerCase()}.xlsx"`
      );
  
      await predictionService.exportPredictionsExcel(
        req.user,
        risk,
        res
      );
    } catch (error) {
      if (!res.headersSent) {
        return next(error);
      }
  
      console.error(
        "Error durante exportación de predicciones:",
        error
      );
  
      res.end();
    }
  }
  
  
  module.exports = {
    getPredictionSummary,
    getPredictionTop,
    downloadPredictions
  };