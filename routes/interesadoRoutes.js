const express = require("express");
const {
  getPeriodos,
  getResumenPorEr,
  exportInteresados,
} = require("../controllers/interesadoController");
const { verifyToken, authorizeRoles } = require("../middlewares/authMiddleware");

const router = express.Router();

router.get(
  "/api/interesados/periodos",
  verifyToken,
  authorizeRoles("SUPERVISOR", "ADMIN"),
  getPeriodos
);

router.get(
  "/api/interesados/resumen-er",
  verifyToken,
  authorizeRoles("SUPERVISOR", "ADMIN"),
  getResumenPorEr
);

router.get(
  "/api/interesados/export",
  verifyToken,
  authorizeRoles("SUPERVISOR", "ADMIN"),
  exportInteresados
);

module.exports = router;