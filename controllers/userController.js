const userService = require("../services/userService");

async function getUsers(req, res, next) {
  try {
    const filters = {
      role: req.query.role || null,
      region: req.query.region || null,
      q: req.query.q || null,
      includeInactive: req.query.includeInactive || null
    };

    const users = await userService.listUsers(req.user, filters);

    return res.json({
      ok: true,
      data: users
    });
  } catch (error) {
    next(error);
  }
}

async function createUser(req, res, next) {
  try {
    const user = await userService.createUser(req.user, req.body);

    return res.status(201).json({
      ok: true,
      data: user
    });
  } catch (error) {
    next(error);
  }
}

async function updateUser(req, res, next) {
  try {
    const user = await userService.updateUser(
      req.user,
      req.params.id,
      req.body
    );

    return res.json({
      ok: true,
      data: user
    });
  } catch (error) {
    next(error);
  }
}

async function deleteUser(req, res, next) {
  try {
    const result = await userService.deleteUser(req.user, req.params.id);

    return res.json({
      ok: true,
      ...result
    });
  } catch (error) {
    next(error);
  }
}

module.exports = {
  getUsers,
  createUser,
  updateUser,
  deleteUser
};