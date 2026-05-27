const crypto = require("crypto");
const bcrypt = require("bcryptjs");
const { getPool } = require("../db/pool");
const { sendMail } = require("./mailService");

const GENERIC_MESSAGE =
  "Si el correo está registrado, recibirás instrucciones para restablecer tu contraseña.";

function createHttpError(status, message) {
  const error = new Error(message);
  error.status = status;
  return error;
}

function normalizeText(value) {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  return text ? text : null;
}

function normalizeEmail(email) {
  const value = normalizeText(email);
  return value ? value.toLowerCase() : null;
}

function hashToken(token) {
  return crypto.createHash("sha256").update(token).digest("hex");
}

function getFrontendUrl() {
  return process.env.FRONTEND_URL;
}

function getExpirationMinutes() {
  const value = Number(process.env.RESET_PASSWORD_EXPIRES_MINUTES || 30);
  return Number.isFinite(value) && value > 0 ? value : 30;
}

function buildResetEmail({ name, resetUrl, expiresMinutes }) {
  const safeName = name || "usuario";

  const text = `
Hola ${safeName},

Recibimos una solicitud para restablecer tu contraseña en SmartTrack.

Ingresa al siguiente enlace para crear una nueva contraseña:
${resetUrl}

Este enlace vencerá en ${expiresMinutes} minutos.

Si no solicitaste este cambio, puedes ignorar este correo.

SmartTrack
`.trim();

  const html = `
    <div style="font-family: Arial, sans-serif; color: #222; line-height: 1.5;">
      <h2 style="color: #13293d;">Recuperación de contraseña - SmartTrack</h2>

      <p>Hola ${safeName},</p>

      <p>Recibimos una solicitud para restablecer tu contraseña en SmartTrack.</p>

      <p>
        <a href="${resetUrl}"
           style="display: inline-block; background: #13293d; color: #ffffff; padding: 10px 16px; text-decoration: none; border-radius: 6px;">
          Restablecer contraseña
        </a>
      </p>

      <p>Este enlace vencerá en <strong>${expiresMinutes} minutos</strong>.</p>

      <p>Si no solicitaste este cambio, puedes ignorar este correo.</p>

      <hr style="border: none; border-top: 1px solid #ddd;" />

      <p style="font-size: 12px; color: #666;">
        Si el botón no funciona, copia y pega este enlace en tu navegador:<br />
        ${resetUrl}
      </p>
    </div>
  `;

  return { text, html };
}

async function requestPasswordReset({ email, ip, userAgent }) {
  const normalizedEmail = normalizeEmail(email);

  if (!normalizedEmail) {
    throw createHttpError(400, "El correo es obligatorio");
  }

  const pool = getPool();

  const [users] = await pool.query(
    `SELECT 
        web_user_id,
        name,
        email,
        is_active
     FROM web_user
     WHERE email = ?
     LIMIT 1`,
    [normalizedEmail]
  );

  // Respuesta genérica para no revelar si el correo existe o no.
  if (!users.length) {
    return { message: GENERIC_MESSAGE };
  }

  const user = users[0];

  // También respondemos genérico si está inactivo.
  if (Number(user.is_active) !== 1) {
    return { message: GENERIC_MESSAGE };
  }

  const rawToken = crypto.randomBytes(32).toString("hex");
  const tokenHash = hashToken(rawToken);
  const expiresMinutes = getExpirationMinutes();

  const resetUrl = `${getFrontendUrl()}/reset-password?token=${encodeURIComponent(rawToken)}`;

  // Invalidamos tokens anteriores no usados del mismo usuario.
  await pool.query(
    `UPDATE password_reset_token
     SET used_at = UTC_TIMESTAMP()
     WHERE web_user_id = ?
       AND used_at IS NULL`,
    [user.web_user_id]
  );

  await pool.query(
    `INSERT INTO password_reset_token (
        web_user_id,
        token_hash,
        expires_at,
        request_ip,
        user_agent
     ) VALUES (
        ?,
        ?,
        DATE_ADD(UTC_TIMESTAMP(), INTERVAL ? MINUTE),
        ?,
        ?
     )`,
    [
      user.web_user_id,
      tokenHash,
      expiresMinutes,
      normalizeText(ip),
      normalizeText(userAgent)
    ]
  );

  const emailBody = buildResetEmail({
    name: user.name,
    resetUrl,
    expiresMinutes
  });

  await sendMail({
    to: user.email,
    subject: "Recuperación de contraseña - SmartTrack",
    text: emailBody.text,
    html: emailBody.html
  });

  return { message: GENERIC_MESSAGE };
}

async function resetPassword({ token, newPassword, confirmPassword }) {
  const rawToken = normalizeText(token);
  const password = normalizeText(newPassword);
  const passwordConfirm = normalizeText(confirmPassword);

  if (!rawToken) {
    throw createHttpError(400, "Token requerido");
  }

  if (!password || password.length < 6) {
    throw createHttpError(400, "La contraseña debe tener al menos 6 caracteres");
  }

  if (!passwordConfirm) {
    throw createHttpError(400, "Debe confirmar la contraseña");
  }

  if (password !== passwordConfirm) {
    throw createHttpError(400, "Las contraseñas no coinciden");
  }

  const tokenHash = hashToken(rawToken);
  const pool = getPool();

  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const [rows] = await connection.query(
      `SELECT
          prt.password_reset_token_id,
          prt.web_user_id,
          wu.name,
          wu.email,
          wu.is_active
       FROM password_reset_token prt
       INNER JOIN web_user wu
          ON wu.web_user_id = prt.web_user_id
       WHERE prt.token_hash = ?
         AND prt.used_at IS NULL
         AND prt.expires_at > UTC_TIMESTAMP()
       LIMIT 1`,
      [tokenHash]
    );

    if (!rows.length) {
      throw createHttpError(400, "El enlace de recuperación es inválido o ya venció");
    }

    const resetRequest = rows[0];

    if (Number(resetRequest.is_active) !== 1) {
      throw createHttpError(403, "Usuario inactivo");
    }

    const passwordHash = await bcrypt.hash(password, 10);

    await connection.query(
      `UPDATE web_user
       SET password_hash = ?
       WHERE web_user_id = ?`,
      [passwordHash, resetRequest.web_user_id]
    );

    await connection.query(
      `UPDATE password_reset_token
       SET used_at = UTC_TIMESTAMP()
       WHERE password_reset_token_id = ?`,
      [resetRequest.password_reset_token_id]
    );

    await connection.commit();

    // Aviso de seguridad. Si falla este correo, no revertimos el cambio de contraseña.
    try {
      await sendMail({
        to: resetRequest.email,
        subject: "Contraseña actualizada - SmartTrack",
        text: `Hola ${resetRequest.name || "usuario"},\n\nTu contraseña de SmartTrack fue actualizada correctamente.\n\nSi no realizaste este cambio, comunícate con el administrador.\n\nSmartTrack`,
        html: `
          <div style="font-family: Arial, sans-serif; color: #222; line-height: 1.5;">
            <h2 style="color: #13293d;">Contraseña actualizada</h2>
            <p>Hola ${resetRequest.name || "usuario"},</p>
            <p>Tu contraseña de SmartTrack fue actualizada correctamente.</p>
            <p>Si no realizaste este cambio, comunícate con el administrador.</p>
          </div>
        `
      });
    } catch (mailError) {
      console.error("[PASSWORD_RESET_CONFIRMATION_EMAIL_ERROR]", {
        message: mailError.message
      });
    }

    return {
      message: "Contraseña actualizada correctamente"
    };
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
}

module.exports = {
  requestPasswordReset,
  resetPassword
};