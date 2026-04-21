const botService = require("../services/botService");

async function handleMessage(req, res, next) {
  try {
    const result = await botService.processMessage({
      channel: "WEB",
      userId: String(req.user.uid),
      userName: req.user.uname,
      userRole: req.user.role,
      userRegion: req.user.region || null,
      webUserId: req.user.uid,
      conversationId: req.body.conversationId || String(req.user.uid),
      text: req.body.text || "",
      payload: req.body.payload || {}
    });

    return res.json({
      ok: true,
      ...result
    });
  } catch (error) {
    next(error);
  }
}

module.exports = {
  handleMessage
};