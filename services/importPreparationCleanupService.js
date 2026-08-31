const fs = require("fs");
const path = require("path");

const logger = require("../utils/logger");

const {
  PREPARATION_TTL_MS,
  PREPARATION_CLEANUP_INTERVAL_MS,
  PREPARATIONS_ROOT
} = require("../config/importPreparationConfig");

let cleanupRunning = false;
let cleanupTimer = null;

/**
 * Garantiza que la carpeta principal exista.
 */
async function ensurePreparationsRoot() {
  await fs.promises.mkdir(
    PREPARATIONS_ROOT,
    {
      recursive: true
    }
  );
}

/**
 * Obtiene la fecha utilizada para calcular
 * la antigüedad de una preparación.
 */
function getDirectoryReferenceTime(stats) {
  if (
    Number.isFinite(stats.birthtimeMs) &&
    stats.birthtimeMs > 0
  ) {
    return stats.birthtimeMs;
  }

  return stats.mtimeMs;
}

/**
 * Elimina todas las preparaciones que hayan
 * superado el tiempo máximo permitido.
 */
async function cleanupExpiredPreparations() {
  if (cleanupRunning) {
    return {
      skipped: true,
      removed: 0
    };
  }

  cleanupRunning = true;

  try {
    await ensurePreparationsRoot();

    const entries = await fs.promises.readdir(
      PREPARATIONS_ROOT,
      {
        withFileTypes: true
      }
    );

    let removed = 0;
    const now = Date.now();

    for (const entry of entries) {
      if (!entry.isDirectory()) {
        continue;
      }

      const preparationDir = path.join(
        PREPARATIONS_ROOT,
        entry.name
      );

      try {
        const stats = await fs.promises.stat(
          preparationDir
        );

        const referenceTime =
          getDirectoryReferenceTime(stats);

        const ageMs = now - referenceTime;

        if (ageMs < PREPARATION_TTL_MS) {
          continue;
        }

        await fs.promises.rm(
          preparationDir,
          {
            recursive: true,
            force: true
          }
        );

        removed += 1;

        logger.info(
          "Expired import preparation removed",
          {
            preparationId: entry.name,
            ageMs
          }
        );
      } catch (error) {
        logger.error(
          "Could not remove import preparation",
          {
            preparationId: entry.name,
            message: error.message
          }
        );
      }
    }

    return {
      skipped: false,
      removed
    };
  } finally {
    cleanupRunning = false;
  }
}

/**
 * Inicia el limpiador una sola vez.
 *
 * También realiza una limpieza inmediata
 * cuando arranca el backend.
 */
function startPreparationCleanup() {
  if (cleanupTimer) {
    return cleanupTimer;
  }

  cleanupExpiredPreparations().catch(
    (error) => {
      logger.error(
        "Initial import preparation cleanup failed",
        {
          message: error.message
        }
      );
    }
  );

  cleanupTimer = setInterval(() => {
    cleanupExpiredPreparations().catch(
      (error) => {
        logger.error(
          "Scheduled import preparation cleanup failed",
          {
            message: error.message
          }
        );
      }
    );
  }, PREPARATION_CLEANUP_INTERVAL_MS);

  /*
   * Evita que el temporizador por sí solo
   * impida cerrar el proceso de Node.
   */
  if (
    typeof cleanupTimer.unref === "function"
  ) {
    cleanupTimer.unref();
  }

  logger.info(
    "Import preparation cleanup started",
    {
      ttlMs: PREPARATION_TTL_MS,
      intervalMs:
        PREPARATION_CLEANUP_INTERVAL_MS,
      root: PREPARATIONS_ROOT
    }
  );

  return cleanupTimer;
}

module.exports = {
  cleanupExpiredPreparations,
  startPreparationCleanup
};