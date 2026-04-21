const express = require("express");
const {
  getEpinSummary,
  getEpinRecency,
  getEpinSegments,
  downloadEpinSegments,
  getEpinTrends,
  getEpinTrendComparison
} = require("../controllers/analyticsController");
const { verifyToken, authorizeRoles } = require("../middlewares/authMiddleware");

const router = express.Router();

router.get(
  "/api/analytics/epin/summary",
  verifyToken,
  authorizeRoles("SUPERVISOR", "ADMIN"),
  getEpinSummary
);

router.get(
  "/api/analytics/epin/recency",
  verifyToken,
  authorizeRoles("SUPERVISOR", "ADMIN"),
  getEpinRecency
);

router.get(
    "/api/analytics/epin/segments",
    verifyToken,
    authorizeRoles("SUPERVISOR", "ADMIN"),
    getEpinSegments
  );
  
router.get(
    "/api/analytics/epin/segments/export",
    verifyToken,
    authorizeRoles("SUPERVISOR", "ADMIN"),
    downloadEpinSegments
);

router.get(
    "/api/analytics/epin/trends",
    verifyToken,
    authorizeRoles("SUPERVISOR", "ADMIN"),
    getEpinTrends
  );
  
router.get(
    "/api/analytics/epin/trends/comparison",
    verifyToken,
    authorizeRoles("SUPERVISOR", "ADMIN"),
    getEpinTrendComparison
  );
module.exports = router;