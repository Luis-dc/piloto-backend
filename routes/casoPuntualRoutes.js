const express = require("express");
const {
  getPeriodos,
  getResumenPorEr,
  exportCasosPuntuales
} = require("../controllers/casoPuntualController");

const { verifyToken, authorizeRoles } = require("../middlewares/authMiddleware");

const router = express.Router();

router.get(
  "/api/casos-puntuales/periodos",
  verifyToken,
  authorizeRoles("SUPERVISOR", "ADMIN"),
  getPeriodos
);

router.get(
  "/api/casos-puntuales/resumen-er",
  verifyToken,
  authorizeRoles("SUPERVISOR", "ADMIN"),
  getResumenPorEr
);

router.get(
  "/api/casos-puntuales/export",
  verifyToken,
  authorizeRoles("SUPERVISOR", "ADMIN"),
  exportCasosPuntuales
);

module.exports = router;