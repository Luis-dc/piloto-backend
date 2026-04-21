const bcrypt = require("bcryptjs");
const { getPool } = require("../db/pool");

const ALLOWED_ROLES = ["ADMIN", "SUPERVISOR", "ER"];
const ALLOWED_REGIONS = ["Central", "Oriente", "Occidente"];

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

function normalizeRole(role) {
  const value = normalizeText(role);
  return value ? value.toUpperCase() : null;
}

function normalizeRegion(region) {
  const value = normalizeText(region);
  if (!value) return null;

  const match = ALLOWED_REGIONS.find(
    (item) => item.toLowerCase() === value.toLowerCase()
  );

  return match || null;
}

function normalizeIsActive(value) {
  if (
    value === true ||
    value === 1 ||
    value === "1" ||
    String(value).toLowerCase() === "true"
  ) {
    return 1;
  }

  if (
    value === false ||
    value === 0 ||
    value === "0" ||
    String(value).toLowerCase() === "false"
  ) {
    return 0;
  }

  return null;
}

function validateRole(role) {
  if (!role || !ALLOWED_ROLES.includes(role)) {
    throw createHttpError(400, "Rol inválido");
  }
}

function validateRegionForRole(role, region) {
  if (role === "ADMIN") {
    if (region && !ALLOWED_REGIONS.includes(region)) {
      throw createHttpError(400, "Región inválida");
    }
    return region || null;
  }

  if (!region) {
    throw createHttpError(400, "La región es obligatoria");
  }

  if (!ALLOWED_REGIONS.includes(region)) {
    throw createHttpError(400, "Región inválida");
  }

  return region;
}

function ensureManagerRole(authUser) {
  if (!authUser || !["ADMIN", "SUPERVISOR"].includes(authUser.role)) {
    throw createHttpError(403, "Sin permisos");
  }
}

async function getUserById(userId) {
  const pool = getPool();

  const [rows] = await pool.query(
    `SELECT
        web_user_id AS id,
        name,
        email,
        role,
        region,
        is_active,
        created_at,
        last_login_at
     FROM web_user
     WHERE web_user_id = ?
     LIMIT 1`,
    [userId]
  );

  return rows[0] || null;
}

async function emailExists(email, excludeUserId = null) {
  const pool = getPool();

  if (excludeUserId) {
    const [rows] = await pool.query(
      `SELECT web_user_id
       FROM web_user
       WHERE email = ?
         AND web_user_id <> ?
       LIMIT 1`,
      [email, excludeUserId]
    );

    return rows.length > 0;
  }

  const [rows] = await pool.query(
    `SELECT web_user_id
     FROM web_user
     WHERE email = ?
     LIMIT 1`,
    [email]
  );

  return rows.length > 0;
}

function ensureSupervisorTargetAccess(authUser, targetUser) {
  if (authUser.role === "ADMIN") return;

  if (authUser.role !== "SUPERVISOR") {
    throw createHttpError(403, "Sin permisos");
  }

  if (!targetUser) {
    throw createHttpError(404, "Usuario no encontrado");
  }

  if (targetUser.role !== "ER") {
    throw createHttpError(403, "Solo puedes gestionar usuarios ER");
  }

  if ((targetUser.region || null) !== (authUser.region || null)) {
    throw createHttpError(403, "Solo puedes gestionar ER de tu región");
  }
}

async function listUsers(authUser, filters = {}) {
  ensureManagerRole(authUser);

  const pool = getPool();
  const where = [];
  const params = [];

  const q = normalizeText(filters.q);
  const includeInactive =
    authUser.role === "ADMIN" &&
    (filters.includeInactive === "1" || filters.includeInactive === "true");

  if (!includeInactive) {
    where.push("is_active = 1");
  }

  if (authUser.role === "SUPERVISOR") {
    where.push("role = 'ER'");
    where.push("region = ?");
    params.push(authUser.region || "");
  } else {
    const role = normalizeRole(filters.role);
    const region = normalizeRegion(filters.region);

    if (role) {
      validateRole(role);
      where.push("role = ?");
      params.push(role);
    }

    if (filters.region !== undefined && filters.region !== null && !region) {
      throw createHttpError(400, "Región inválida");
    }

    if (region) {
      where.push("region = ?");
      params.push(region);
    }
  }

  if (q) {
    where.push("(name LIKE ? OR email LIKE ?)");
    params.push(`%${q}%`, `%${q}%`);
  }

  const sql = `
    SELECT
      web_user_id AS id,
      name,
      email,
      role,
      region,
      is_active,
      created_at,
      last_login_at
    FROM web_user
    ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
    ORDER BY FIELD(role, 'ADMIN', 'SUPERVISOR', 'ER'), name ASC
  `;

  const [rows] = await pool.query(sql, params);

  return rows;
}

async function createUser(authUser, payload) {
  ensureManagerRole(authUser);

  const name = normalizeText(payload.name);
  const email = normalizeEmail(payload.email);
  const password = normalizeText(payload.password);
  let role = normalizeRole(payload.role);
  let region = normalizeRegion(payload.region);

  if (!name) {
    throw createHttpError(400, "El nombre es obligatorio");
  }

  if (!email) {
    throw createHttpError(400, "El correo es obligatorio");
  }

  if (!password || password.length < 6) {
    throw createHttpError(400, "La contraseña debe tener al menos 6 caracteres");
  }

  if (authUser.role === "SUPERVISOR") {
    role = "ER";
    region = authUser.region || null;
  }

  validateRole(role);
  region = validateRegionForRole(role, region);

  if (await emailExists(email)) {
    throw createHttpError(409, "Ya existe un usuario con ese correo");
  }

  const passwordHash = await bcrypt.hash(password, 10);
  const pool = getPool();

  const [result] = await pool.query(
    `INSERT INTO web_user (
      name,
      email,
      password_hash,
      role,
      region,
      is_active
    ) VALUES (?, ?, ?, ?, ?, 1)`,
    [name, email, passwordHash, role, region]
  );

  return getUserById(result.insertId);
}

async function updateUser(authUser, userId, payload) {
  ensureManagerRole(authUser);

  const targetUser = await getUserById(userId);

  if (!targetUser) {
    throw createHttpError(404, "Usuario no encontrado");
  }

  ensureSupervisorTargetAccess(authUser, targetUser);

  if (Number(authUser.uid) === Number(userId) && payload.is_active !== undefined) {
    throw createHttpError(400, "No puedes desactivarte a ti mismo");
  }

  const updates = [];
  const params = [];

  if (payload.name !== undefined) {
    const name = normalizeText(payload.name);
    if (!name) {
      throw createHttpError(400, "El nombre no puede ir vacío");
    }
    updates.push("name = ?");
    params.push(name);
  }

  if (payload.email !== undefined) {
    const email = normalizeEmail(payload.email);
    if (!email) {
      throw createHttpError(400, "El correo no puede ir vacío");
    }

    if (await emailExists(email, userId)) {
      throw createHttpError(409, "Ya existe un usuario con ese correo");
    }

    updates.push("email = ?");
    params.push(email);
  }

  if (payload.password !== undefined) {
    const password = normalizeText(payload.password);
    if (!password || password.length < 6) {
      throw createHttpError(400, "La contraseña debe tener al menos 6 caracteres");
    }

    const passwordHash = await bcrypt.hash(password, 10);
    updates.push("password_hash = ?");
    params.push(passwordHash);
  }

  if (authUser.role === "ADMIN") {
    let nextRole =
      payload.role !== undefined ? normalizeRole(payload.role) : targetUser.role;
    let nextRegion =
      payload.region !== undefined
        ? normalizeRegion(payload.region)
        : targetUser.region;

    validateRole(nextRole);
    nextRegion = validateRegionForRole(nextRole, nextRegion);

    if (payload.role !== undefined) {
      updates.push("role = ?");
      params.push(nextRole);
    }

    if (payload.region !== undefined) {
      updates.push("region = ?");
      params.push(nextRegion);
    }

    if (payload.is_active !== undefined) {
      const isActive = normalizeIsActive(payload.is_active);

      if (isActive === null) {
        throw createHttpError(400, "Valor inválido para is_active");
      }

      updates.push("is_active = ?");
      params.push(isActive);
    }
  } else {
    if (payload.role !== undefined || payload.region !== undefined || payload.is_active !== undefined) {
      throw createHttpError(
        403,
        "Como supervisor solo puedes editar nombre, correo y contraseña de tus ER"
      );
    }
  }

  if (!updates.length) {
    throw createHttpError(400, "No se enviaron cambios válidos");
  }

  const pool = getPool();

  await pool.query(
    `UPDATE web_user
     SET ${updates.join(", ")}
     WHERE web_user_id = ?`,
    [...params, userId]
  );

  return getUserById(userId);
}

async function deleteUser(authUser, userId) {
  ensureManagerRole(authUser);

  if (Number(authUser.uid) === Number(userId)) {
    throw createHttpError(400, "No puedes eliminarte a ti mismo");
  }

  const targetUser = await getUserById(userId);

  if (!targetUser) {
    throw createHttpError(404, "Usuario no encontrado");
  }

  ensureSupervisorTargetAccess(authUser, targetUser);

  const pool = getPool();

  await pool.query(
    `UPDATE web_user
     SET is_active = 0
     WHERE web_user_id = ?`,
    [userId]
  );

  return {
    id: Number(userId),
    deleted: true
  };
}

module.exports = {
  listUsers,
  createUser,
  updateUser,
  deleteUser
};