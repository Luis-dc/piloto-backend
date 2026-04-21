const express = require("express");
const {
  getUsers,
  createUser,
  updateUser,
  deleteUser
} = require("../controllers/userController");
const { verifyToken, authorizeRoles } = require("../middlewares/authMiddleware");

const router = express.Router();

router.get(
  "/api/users",
  verifyToken,
  authorizeRoles("ADMIN", "SUPERVISOR"),
  getUsers
);

router.post(
  "/api/users",
  verifyToken,
  authorizeRoles("ADMIN", "SUPERVISOR"),
  createUser
);

router.put(
  "/api/users/:id",
  verifyToken,
  authorizeRoles("ADMIN", "SUPERVISOR"),
  updateUser
);

router.delete(
  "/api/users/:id",
  verifyToken,
  authorizeRoles("ADMIN", "SUPERVISOR"),
  deleteUser
);

module.exports = router;