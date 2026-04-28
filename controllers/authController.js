const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const { getPool } = require("../db/pool");

async function login(req, res) {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({ ok: false, error: "Faltan credenciales" });
    }

    const pool = getPool();

    const [rows] = await pool.query(
      `SELECT 
          web_user_id AS id,
          name,
          email,
          password_hash,
          role,
          region,
          is_active
       FROM web_user
       WHERE email = ?
       LIMIT 1`,
      [email]
    );

    if (!rows.length) {
      return res.status(401).json({ ok: false, error: "Credenciales inválidas" });
    }

    const user = rows[0];

    if (user.is_active === 0) {
      return res.status(403).json({ ok: false, error: "Usuario inactivo" });
    }

    const match = await bcrypt.compare(password, user.password_hash);
    if (!match) {
      return res.status(401).json({ ok: false, error: "Credenciales inválidas" });
    }

    const token = jwt.sign(
      {
        uid: user.id,
        uname: user.name,
        role: user.role,
        email: user.email,
        region: user.region
      },
      process.env.JWT_SECRET,
      { expiresIn: "6h" }
    );

    return res.json({
      ok: true,
      token,
      user: {
        id: user.id,
        name: user.name,
        email: user.email,
        role: user.role,
        region: user.region
      }
    });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
}

module.exports = { login };