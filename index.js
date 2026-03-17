require("dotenv").config();

const express = require("express");
const healthRoutes = require("./routes/healthRoutes");
const { errorMiddleware } = require("./middlewares/errorMiddleware");
const logger = require("./utils/logger");
const authRoutes = require("./routes/authRoutes");
const importRoutes = require("./routes/importRoutes");

const app = express();
app.use(express.json());

// requestId simple
app.use((req, res, next) => {
  req.requestId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  next();
});

// log por request (inicio/fin)
app.use((req, res, next) => {
  const start = Date.now();

  res.on("finish", () => {
    logger.info("HTTP request", {
      requestId: req.requestId,
      method: req.method,
      path: req.originalUrl,
      status: res.statusCode,
      ms: Date.now() - start
    });
  });

  next();
});

// Rutas
app.use(healthRoutes);
app.use(authRoutes);
app.use(importRoutes)

// Middleware de errores
app.use(errorMiddleware);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API running on port ${PORT}`);
});


