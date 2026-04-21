const { CloudAdapter, ConfigurationBotFrameworkAuthentication, ActivityTypes } = require("botbuilder");
const botService = require("../services/botService");

const botFrameworkAuthentication = new ConfigurationBotFrameworkAuthentication(process.env);
const adapter = new CloudAdapter(botFrameworkAuthentication);

adapter.onTurnError = async (context, error) => {
  console.error("Teams adapter error:", error);

  try {
    await context.sendActivity("Ocurrió un error procesando la solicitud. Escriba menu para continuar.");
  } catch (sendError) {
    console.error("Teams adapter send error:", sendError);
  }
};

function toEvent(context) {
  const activity = context.activity || {};

  return {
    channel: "TEAMS",
    userId: activity.from?.id || "",
    userName: activity.from?.name || "",
    conversationId: activity.conversation?.id || activity.from?.id || "",
    text: activity.text || "",
    payload: {
      activityId: activity.id || null,
      serviceUrl: activity.serviceUrl || null,
      channelId: activity.channelId || null,
      conversationType: activity.conversation?.conversationType || null,
      tenantId: activity.conversation?.tenantId || activity.channelData?.tenant?.id || null,
      localTimestamp: activity.localTimestamp || null,
      channelData: activity.channelData || {}
    }
  };
}

async function handleTeamsTurn(context) {
  const activity = context.activity;

  if (activity.type === ActivityTypes.ConversationUpdate) {
    const membersAdded = activity.membersAdded || [];
    const botId = activity.recipient?.id;

    for (const member of membersAdded) {
      if (member.id !== botId) {
        await context.sendActivity(
          "Bienvenido a SmartTrack.\n¿Qué desea realizar?\n1. Consultar EPIN\n2. Consultar por ID DMS\n3. Interesado"
        );
      }
    }
    return;
  }

  if (activity.type !== ActivityTypes.Message) {
    return;
  }

  const result = await botService.processMessage(toEvent(context));

  if (result?.text) {
    await context.sendActivity(result.text);
  }
}

async function processTeamsRequest(req, res) {
  await adapter.process(req, res, async (context) => {
    await handleTeamsTurn(context);
  });
}

module.exports = {
  processTeamsRequest,
  adapter
};