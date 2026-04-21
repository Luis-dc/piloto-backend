require("dotenv").config();

const express = require("express");
const cors = require('cors');
const healthRoutes = require("./routes/healthRoutes");
const { errorMiddleware } = require("./middlewares/errorMiddleware");
const logger = require("./utils/logger");
const authRoutes = require("./routes/authRoutes");
const importRoutes = require("./routes/importRoutes");
const botRoutes = require("./routes/botRoutes");
const interesadoRoutes = require("./routes/interesadoRoutes");
const analyticsRoutes = require("./routes/analyticsRoutes");

const app = express();

// uso de cors
app.use(cors({
  origin: process.env.FRONTEND_URL,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}))

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
app.use(botRoutes);
app.use(interesadoRoutes);
app.use(analyticsRoutes);

// Middleware de errores
app.use(errorMiddleware);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API running on port ${PORT}`);
});


