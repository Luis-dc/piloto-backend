const express = require("express");

const {
  getPredictionSummary,
  getPredictionTop,
  downloadPredictions
} = require(
  "../controllers/predictionController"
);

const {
  verifyToken,
  authorizeRoles
} = require(
  "../middlewares/authMiddleware"
);

const router = express.Router();


// =========================================
// PREDICCION
// =========================================

router.get(
  "/api/prediction/summary",
  verifyToken,
  authorizeRoles(
    "SUPERVISOR",
    "ADMIN"
  ),
  getPredictionSummary
);

router.get(
    "/api/prediction/top",
    verifyToken,
    authorizeRoles(
      "SUPERVISOR",
      "ADMIN"
    ),
    getPredictionTop
);

router.get(
    "/api/prediction/export",
    verifyToken,
    authorizeRoles(
      "SUPERVISOR",
      "ADMIN"
    ),
    downloadPredictions
);

module.exports = router;