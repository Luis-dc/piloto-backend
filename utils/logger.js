// utils/logger.js
function now() {
  return new Date().toISOString();
}

function log(level, message, meta = {}) {
  const payload = {
    ts: now(),
    level,
    message,
    ...meta
  };
  // stdout (luego si quieres lo cambiamos a winston/pino)
  console.log(JSON.stringify(payload));
}

function info(message, meta) {
  log("info", message, meta);
}

function warn(message, meta) {
  log("warn", message, meta);
}

function error(message, meta) {
  log("error", message, meta);
}

module.exports = { info, warn, error };