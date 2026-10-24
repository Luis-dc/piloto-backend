const express = require("express");

const {
  getModelVersions,
  activateModelVersion,
  discardModelVersion
} = require(
  "../controllers/modelVersionController"
);

const {
  verifyToken,
  authorizeRoles
} = require(
  "../middlewares/authMiddleware"
);

const router = express.Router();


// =========================================
// VERSIONES DE MODELO
// SOLO ADMIN
// =========================================

router.get(
  "/api/model-versions",
  verifyToken,
  authorizeRoles("ADMIN"),
  getModelVersions
);


router.patch(
  "/api/model-versions/:id/activate",
  verifyToken,
  authorizeRoles("ADMIN"),
  activateModelVersion
);


router.patch(
  "/api/model-versions/:id/discard",
  verifyToken,
  authorizeRoles("ADMIN"),
  discardModelVersion
);


module.exports = router;