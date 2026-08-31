const path = require("path");

/**
 * Tiempo durante el cual una preparación permanece disponible.
 * 60 minutos.
 */
const PREPARATION_TTL_MS =
  60 * 60 * 1000;

/**
 * Frecuencia con la que el backend buscará
 * preparaciones vencidas.
 * 10 minutos.
 */
const PREPARATION_CLEANUP_INTERVAL_MS =
  10 * 60 * 1000;

/**
 * Carpeta principal de archivos temporales.
 */
const PREPARATIONS_ROOT = path.join(
  __dirname,
  "..",
  "uploads",
  "preparations"
);

module.exports = {
  PREPARATION_TTL_MS,
  PREPARATION_CLEANUP_INTERVAL_MS,
  PREPARATIONS_ROOT
};