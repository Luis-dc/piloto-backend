// middlewares/errorMiddleware.js
const logger = require("../utils/logger");

function errorMiddleware(err, req, res, next) {
  logger.error("Unhandled error", {
    path: req.originalUrl,
    method: req.method,
    requestId: req.requestId,
    error: err.message
  });

  return res.status(500).json({
    ok: false,
    error: "Internal Server Error",
    requestId: req.requestId
  });
}

module.exports = { errorMiddleware };