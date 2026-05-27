const nodemailer = require("nodemailer");

let cachedTransporter = null;

function getBoolean(value) {
  return String(value || "").toLowerCase() === "true";
}

function getTransporter() {
  if (cachedTransporter) return cachedTransporter;

  const host = process.env.MAIL_HOST;
  const port = Number(process.env.MAIL_PORT || 587);
  const secure = getBoolean(process.env.MAIL_SECURE);

  if (!process.env.MAIL_USER || !process.env.MAIL_PASS) {
    throw new Error("Faltan MAIL_USER o MAIL_PASS en variables de entorno.");
  }

  cachedTransporter = nodemailer.createTransport({
    host,
    port,
    secure,
    requireTLS: !secure,
    auth: {
      user: process.env.MAIL_USER,
      pass: process.env.MAIL_PASS
    }
  });

  return cachedTransporter;
}

async function sendMail({ to, cc, subject, text, html }) {
  if (!to) {
    throw new Error("No se definió destinatario para el correo.");
  }

  const transporter = getTransporter();

  return transporter.sendMail({
    from: process.env.MAIL_FROM || process.env.MAIL_USER,
    to,
    cc: cc || undefined,
    subject,
    text,
    html
  });
}

module.exports = {
  sendMail
};