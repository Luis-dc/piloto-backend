const express = require("express");
const {
  login,
  forgotPassword,
  resetPassword
} = require("../controllers/authController");

const router = express.Router();

router.post("/auth/login", login);
router.post("/auth/forgot-password", forgotPassword);
router.post("/auth/reset-password", resetPassword);

module.exports = router;