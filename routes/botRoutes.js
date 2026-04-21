const express = require("express");
const { handleMessage } = require("../controllers/botController");
const { processTeamsRequest } = require("../adapters/teamsAdapter");
const { verifyToken, authorizeRoles } = require("../middlewares/authMiddleware");

const router = express.Router();

router.post(
  "/api/bot/message",
  verifyToken,
  authorizeRoles("ER", "SUPERVISOR", "ADMIN"),
  handleMessage
);

router.post("/api/teams/messages", async (req, res, next) => {
  try {
    await processTeamsRequest(req, res);
  } catch (error) {
    next(error);
  }
});

module.exports = router;