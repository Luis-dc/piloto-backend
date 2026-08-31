const express = require("express");

const {
  getEpinCuts,
  getEpinSummary,
  getEpinEvents,
  downloadEpinEvents,
  getEpinTrends
} = require(
  "../controllers/analyticsController"
);

const {
  verifyToken,
  authorizeRoles
} = require(
  "../middlewares/authMiddleware"
);

const router = express.Router();


// =========================================
// ANÁLISIS EPIN
// =========================================

router.get(
  "/api/analytics/epin/cuts",
  verifyToken,
  authorizeRoles(
    "SUPERVISOR",
    "ADMIN"
  ),
  getEpinCuts
);


router.get(
  "/api/analytics/epin/summary",
  verifyToken,
  authorizeRoles(
    "SUPERVISOR",
    "ADMIN"
  ),
  getEpinSummary
);


router.get(
  "/api/analytics/epin/events",
  verifyToken,
  authorizeRoles(
    "SUPERVISOR",
    "ADMIN"
  ),
  getEpinEvents
);


router.get(
  "/api/analytics/epin/events/export",
  verifyToken,
  authorizeRoles(
    "SUPERVISOR",
    "ADMIN"
  ),
  downloadEpinEvents
);


// =========================================
// TENDENCIAS
// =========================================

router.get(
  "/api/analytics/epin/trends",
  verifyToken,
  authorizeRoles(
    "SUPERVISOR",
    "ADMIN"
  ),
  getEpinTrends
);

module.exports = router;