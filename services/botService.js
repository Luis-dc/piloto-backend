const pdvModel = require("../models/pdvModel");
const epinModel = require("../models/epinModel");
const chatInteractionModel = require("../models/chatInteractionModel");
const conversationStateModel = require("../models/conversationStateModel");
const interesadoModel = require("../models/interesadoModel");

const STATES = {
  MENU: "MENU",
  WAIT_EPIN: "WAIT_EPIN",
  WAIT_ID_DMS: "WAIT_ID_DMS",
  INT_WAIT_ID_DMS: "INT_WAIT_ID_DMS",
  INT_WAIT_TELEFONO: "INT_WAIT_TELEFONO",
  INT_WAIT_NOMBRE_PDV: "INT_WAIT_NOMBRE_PDV",
  INT_WAIT_PROPIETARIO: "INT_WAIT_PROPIETARIO",
  INT_WAIT_DIRECCION: "INT_WAIT_DIRECCION",
  INT_WAIT_DEPARTAMENTO: "INT_WAIT_DEPARTAMENTO",
  INT_WAIT_MUNICIPIO: "INT_WAIT_MUNICIPIO",
  INT_WAIT_LAT: "INT_WAIT_LAT",
  INT_WAIT_LON: "INT_WAIT_LON"
};

function toText(value) {
  return value == null ? "" : String(value).trim();
}

function normalizeInput(value) {
  return toText(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function normalizeChannel(channel) {
  const value = toText(channel).toUpperCase();
  if (["TEAMS", "WHATSAPP", "WEB"].includes(value)) return value;
  return "WEB";
}

function isDigits(value) {
  return /^\d+$/.test(toText(value));
}

function isPhone(value) {
  return /^\d{8,20}$/.test(toText(value));
}

function isDecimal(value) {
  return /^-?\d+(\.\d+)?$/.test(toText(value));
}

function parseNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function parseJsonValue(value) {
  if (value == null) return {};
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return {};
  }
}

function menuResponse() {
  return {
    text:
      "Bienvenido a SmartTrack.\n" +
      "¿Qué desea realizar?\n" +
      "1. Consultar EPIN\n" +
      "2. Consultar por ID DMS\n" +
      "3. Interesado",
    suggested: ["1", "2", "3", "menu"],
    actions: []
  };
}

function buildMapUrls(record) {
  const lat = parseNumber(record?.lat);
  const lon = parseNumber(record?.lon);

  if (lat === null || lon === null) {
    return { waze: null, maps: null };
  }

  const latStr = lat.toFixed(6);
  const lonStr = lon.toFixed(6);

  return {
    waze: `https://waze.com/ul?ll=${latStr},${lonStr}&navigate=yes`,
    maps: `https://www.google.com/maps?q=${latStr},${lonStr}`
  };
}

function buildMapActions(record) {
  const { waze, maps } = buildMapUrls(record);
  const actions = [];

  if (waze) {
    actions.push({ type: "url", label: "Waze", url: waze });
  }

  if (maps) {
    actions.push({ type: "url", label: "Google Maps", url: maps });
  }

  return actions;
}

function formatMiTienda(value) {
  return Number(value) === 1 ? "SI" : "NO";
}

function resolveEpinBanner(record, includeEpinBanner) {
  if (!includeEpinBanner) return null;

  const estado = toText(record?.estado_epin).toUpperCase();

  if (estado === "ACTIVO") return "**ES EPIN**";
  if (estado === "BLOQUEADO") return "**EPIN BLOQUEADO**";
  if (estado === "INACTIVO") return "**EPIN INACTIVO**";

  return "**EPIN ENCONTRADO**";
}

function formatFicha(record, opts = {}) {
  const includeEpinBanner = Boolean(opts.includeEpinBanner);
  const actions = buildMapActions(record);

  const lines = [];
  const banner = resolveEpinBanner(record, includeEpinBanner);

  if (banner) lines.push(banner);
  lines.push("**Información asociada:**");
  lines.push(`**EPIN:** ${record?.epin || "N/D"}`);
  lines.push(`**Estado EPIN:** ${record?.estado_epin || "N/D"}`); 
  if (record?.otros_epin) {
    lines.push(`**Otros EPIN asociados:** ${record.otros_epin}`);
  }
  lines.push(`**ID DMS:** ${record?.id_dms || "N/D"}`);
  lines.push(`**Circuito:** ${record?.circuito || "N/D"}`);
  lines.push(`**Nombre PDV:** ${record?.nombre_pdv || "N/D"}`);
  lines.push(`**Estado:** ${record?.estado_pdv || "N/D"}`);
  lines.push(`**Propietario:** ${record?.propietario || "N/D"}`);
  lines.push(`**Departamento:** ${record?.departamento || "N/D"}`);
  lines.push(`**Municipio:** ${record?.municipio || "N/D"}`);
  lines.push(`**Categoria:** ${record?.categoria || "N/D"}`);
  lines.push(`**Distribuidor:** ${record?.distribuidor || "N/D"}`);
  lines.push(`**Tiene MI TIENDA:** ${formatMiTienda(record?.mi_tienda)}`);
  lines.push(`**Latitud:** ${record?.lat || "N/D"}`);
  lines.push(`**Longitud:** ${record?.lon || "N/D"}`);

  return {
    text: lines.join("\n"),
    suggested: ["1", "2", "3", "menu"],
    actions
  };
}

function responseAskEpin() {
  return {
    text: "Ingrese el número EPIN.",
    suggested: ["menu"],
    actions: []
  };
}

function responseAskId() {
  return {
    text: "Ingrese el ID DMS.",
    suggested: ["menu"],
    actions: []
  };
}

function responseAskInteresadoId() {
  return {
    text: "Ingrese ID DMS.",
    suggested: ["menu"],
    actions: []
  };
}

async function processMessage(event) {
  const channel = normalizeChannel(event.channel);
  const userId = toText(event.userId);
  const conversationId = toText(event.conversationId) || userId;
  const userName = toText(event.userName) || null;
  const rawText = toText(event.text);
  const normalizedText = normalizeInput(rawText);
  const payload = parseJsonValue(event.payload);

  if (!userId) {
    throw new Error("userId es requerido");
  }

  let stateBefore = STATES.MENU;
  let stateAfter = STATES.MENU;
  let stateData = {};
  let action = "SYSTEM";
  let result = "SUCCESS";
  let response = menuResponse();
  let pdvId = null;
  let epinId = null;

  try {
    const currentState = await conversationStateModel.getState(channel, userId, conversationId);
    stateBefore = currentState?.state || STATES.MENU;
    stateData = currentState?.data_json || {};

    // Si no mandan texto, devuelve menú
    if (!rawText) {
      action = "MENU";
      response = menuResponse();
      stateAfter = STATES.MENU;
      stateData = {};
    }
    // Comandos globales
    else if (["menu", "inicio", "start", "reset"].includes(normalizedText)) {
      action = normalizedText === "reset" ? "RESET" : "MENU";
      response = menuResponse();
      stateAfter = STATES.MENU;
      stateData = {};
    } else if (["1", "consultar epin", "epin"].includes(normalizedText)) {
      action = "CONSULTAR_EPIN";
      result = "IN_PROGRESS";
      response = responseAskEpin();
      stateAfter = STATES.WAIT_EPIN;
      stateData = {};
    } else if (["2", "consultar por id", "consultar id", "id", "id dms"].includes(normalizedText)) {
      action = "CONSULTAR_ID_DMS";
      result = "IN_PROGRESS";
      response = responseAskId();
      stateAfter = STATES.WAIT_ID_DMS;
      stateData = {};
    } else if (["3", "interesado"].includes(normalizedText)) {
      action = "INTERESADO";
      result = "IN_PROGRESS";
      response = responseAskInteresadoId();
      stateAfter = STATES.INT_WAIT_ID_DMS;
      stateData = {};
    } else {
      switch (stateBefore) {
        case STATES.WAIT_EPIN: {
          action = "CONSULTAR_EPIN";

          if (!isDigits(rawText)) {
            result = "INVALID";
            response = {
              text: "El EPIN debe contener solo números. Intente nuevamente.",
              suggested: ["menu"],
              actions: []
            };
            stateAfter = STATES.WAIT_EPIN;
            break;
          }

          const epin = await epinModel.findByEpin(rawText);
          if (!epin) {
            result = "NOT_FOUND";
            response = {
              text: "NO ES EPIN.\nPuede intentar nuevamente, volver a menu o registrar Interesado.",
              suggested: ["3", "menu"],
              actions: []
            };
            stateAfter = STATES.WAIT_EPIN;
            break;
          }

          pdvId = epin.pdv_id || null;
          epinId = epin.epin_id || null;
          result = "FOUND";
          response = formatFicha(epin, { includeEpinBanner: true });
          stateAfter = STATES.MENU;
          stateData = {};
          break;
        }

        case STATES.WAIT_ID_DMS: {
          action = "CONSULTAR_ID_DMS";

          if (!isDigits(rawText)) {
            result = "INVALID";
            response = {
              text: "El ID DMS debe contener solo números. Intente nuevamente.",
              suggested: ["menu"],
              actions: []
            };
            stateAfter = STATES.WAIT_ID_DMS;
            break;
          }

          const pdv = await pdvModel.findByIdDms(rawText);
          if (!pdv) {
            result = "NOT_FOUND";
            response = {
              text: "NO EXISTE ID.\nPuede intentar nuevamente o volver a menu.",
              suggested: ["menu"],
              actions: []
            };
            stateAfter = STATES.WAIT_ID_DMS;
            break;
          }

          pdvId = pdv.pdv_id || null;
          epinId = pdv.epin_id || null;
          result = "FOUND";
          response = formatFicha(pdv, { includeEpinBanner: false });
          stateAfter = STATES.MENU;
          stateData = {};
          break;
        }

        case STATES.INT_WAIT_ID_DMS: {
          action = "INTERESADO";
        
          if (!isDigits(rawText)) {
            result = "INVALID";
            response = {
              text: "El ID DMS debe contener solo números. Intente nuevamente.",
              suggested: ["menu"],
              actions: []
            };
            stateAfter = STATES.INT_WAIT_ID_DMS;
            break;
          }
        
          const pdv = await pdvModel.findByIdDms(rawText);
        
          if (!pdv) {
            result = "NOT_FOUND";
            response = {
              text: "No existe ID. Ingrese un ID válido.",
              suggested: ["menu"],
              actions: []
            };
            stateAfter = STATES.INT_WAIT_ID_DMS;
            break;
          }
        
          pdvId = pdv.pdv_id || null;
          epinId = pdv.epin_id || null;
          result = "IN_PROGRESS";
        
          response = {
            text:
              "PDV encontrado.\n" +
              `ID DMS: ${pdv.id_dms || "N/D"}\n` +
              `Nombre PDV: ${pdv.nombre_pdv || "N/D"}\n` +
              "Ingrese un número de teléfono de referencia.",
            suggested: ["menu"],
            actions: []
          };
        
          stateAfter = STATES.INT_WAIT_TELEFONO;
        
          stateData = {
            pdv_id: pdv.pdv_id,
            epin_id: pdv.epin_id,
            id_dms: pdv.id_dms,
            epin: pdv.epin,
        
            nombre_pdv: pdv.nombre_pdv,
            propietario: pdv.propietario,
            direccion: pdv.direccion,
            departamento: pdv.departamento,
            municipio: pdv.municipio,
            lat: pdv.lat,
            lon: pdv.lon,
        
            categoria: pdv.categoria,
            circuito: pdv.circuito,
            distribuidor: pdv.distribuidor,
            estado_pdv: pdv.estado_pdv,
            mi_tienda: pdv.mi_tienda,
            otros_epin: pdv.otros_epin || null
          };
        
          break;
        }

        case STATES.INT_WAIT_TELEFONO: {
          action = "INTERESADO";
        
          if (!isPhone(rawText)) {
            result = "INVALID";
            response = {
              text: "El número de teléfono no es válido. Ingrese solo números.",
              suggested: ["menu"],
              actions: []
            };
            stateAfter = STATES.INT_WAIT_TELEFONO;
            break;
          }
        
          const finalData = {
            ...stateData,
            telefono: rawText
          };
        
          const createResult = await interesadoModel.createInteresado({
            channel,
            created_by_user_channel_id: userId,
            created_by_name: userName,
            created_by_web_user_id: event.webUserId || null,
        
            input_type: "ID_DMS",
            input_value: finalData.id_dms,
        
            pdv_id: finalData.pdv_id,
            epin_id: finalData.epin_id,
        
            id_dms: finalData.id_dms,
            epin_reportado: finalData.epin || null,
            telefono: finalData.telefono,
        
            nombre_pdv: finalData.nombre_pdv,
            propietario: finalData.propietario,
            direccion: finalData.direccion,
            departamento: finalData.departamento,
            municipio: finalData.municipio,
            lat: finalData.lat,
            lon: finalData.lon,
        
            data_json: {
              conversationId,
              payload,
              createdByRole: event.userRole || null,
              createdByRegion: event.userRegion || null,
              categoria: finalData.categoria || null,
              circuito: finalData.circuito || null,
              distribuidor: finalData.distribuidor || null,
              estado_pdv: finalData.estado_pdv || null,
              mi_tienda: finalData.mi_tienda ?? null,
              otros_epin: finalData.otros_epin || null
            }
          });
        
          result = "SUCCESS";
        
          response = {
            text:
              "Solicitud registrada.\n" +
              `Solicitud ID: ${createResult.interesadoId}\n` +
              `ID DMS: ${finalData.id_dms || "N/D"}\n` +
              `EPIN: ${finalData.epin || "N/D"}\n` +
              `Nombre PDV: ${finalData.nombre_pdv || "N/D"}\n` +
              `Propietario: ${finalData.propietario || "N/D"}\n` +
              `Teléfono: ${finalData.telefono || "N/D"}\n` +
              `Dirección: ${finalData.direccion || "N/D"}\n` +
              `Departamento: ${finalData.departamento || "N/D"}\n` +
              `Municipio: ${finalData.municipio || "N/D"}\n` +
              `Latitud: ${finalData.lat || "N/D"}\n` +
              `Longitud: ${finalData.lon || "N/D"}`,
            suggested: ["1", "2", "3", "menu"],
            actions: []
          };
        
          pdvId = finalData.pdv_id || null;
          epinId = finalData.epin_id || null;
        
          stateAfter = STATES.MENU;
          stateData = {};
        
          break;
        }

        case STATES.INT_WAIT_NOMBRE_PDV: {
          action = "INTERESADO";

          if (!rawText) {
            result = "INVALID";
            response = {
              text: "El nombre del punto de venta es obligatorio.",
              suggested: ["menu"],
              actions: []
            };
            stateAfter = STATES.INT_WAIT_NOMBRE_PDV;
            break;
          }

          result = "IN_PROGRESS";
          response = {
            text: "Ingrese el nombre del propietario.",
            suggested: ["menu"],
            actions: []
          };
          stateAfter = STATES.INT_WAIT_PROPIETARIO;
          stateData = { ...stateData, nombre_pdv: rawText };
          break;
        }

        case STATES.INT_WAIT_PROPIETARIO: {
          action = "INTERESADO";

          if (!rawText) {
            result = "INVALID";
            response = {
              text: "El nombre del propietario es obligatorio.",
              suggested: ["menu"],
              actions: []
            };
            stateAfter = STATES.INT_WAIT_PROPIETARIO;
            break;
          }

          result = "IN_PROGRESS";
          response = {
            text: "Ingrese la dirección.",
            suggested: ["menu"],
            actions: []
          };
          stateAfter = STATES.INT_WAIT_DIRECCION;
          stateData = { ...stateData, propietario: rawText };
          break;
        }

        case STATES.INT_WAIT_DIRECCION: {
          action = "INTERESADO";

          if (!rawText) {
            result = "INVALID";
            response = {
              text: "La dirección es obligatoria.",
              suggested: ["menu"],
              actions: []
            };
            stateAfter = STATES.INT_WAIT_DIRECCION;
            break;
          }

          result = "IN_PROGRESS";
          response = {
            text: "Ingrese el departamento.",
            suggested: ["menu"],
            actions: []
          };
          stateAfter = STATES.INT_WAIT_DEPARTAMENTO;
          stateData = { ...stateData, direccion: rawText };
          break;
        }

        case STATES.INT_WAIT_DEPARTAMENTO: {
          action = "INTERESADO";

          if (!rawText) {
            result = "INVALID";
            response = {
              text: "El departamento es obligatorio.",
              suggested: ["menu"],
              actions: []
            };
            stateAfter = STATES.INT_WAIT_DEPARTAMENTO;
            break;
          }

          result = "IN_PROGRESS";
          response = {
            text: "Ingrese el municipio.",
            suggested: ["menu"],
            actions: []
          };
          stateAfter = STATES.INT_WAIT_MUNICIPIO;
          stateData = { ...stateData, departamento: rawText };
          break;
        }

        case STATES.INT_WAIT_MUNICIPIO: {
          action = "INTERESADO";

          if (!rawText) {
            result = "INVALID";
            response = {
              text: "El municipio es obligatorio.",
              suggested: ["menu"],
              actions: []
            };
            stateAfter = STATES.INT_WAIT_MUNICIPIO;
            break;
          }

          result = "IN_PROGRESS";
          response = {
            text: "Ingrese la latitud en formato decimal.",
            suggested: ["menu"],
            actions: []
          };
          stateAfter = STATES.INT_WAIT_LAT;
          stateData = { ...stateData, municipio: rawText };
          break;
        }

        case STATES.INT_WAIT_LAT: {
          action = "INTERESADO";

          if (!isDecimal(rawText)) {
            result = "INVALID";
            response = {
              text: "La latitud debe ser numérica en formato decimal.",
              suggested: ["menu"],
              actions: []
            };
            stateAfter = STATES.INT_WAIT_LAT;
            break;
          }

          const lat = parseNumber(rawText);
          if (lat === null || lat < -90 || lat > 90) {
            result = "INVALID";
            response = {
              text: "La latitud debe estar en el rango -90 a 90.",
              suggested: ["menu"],
              actions: []
            };
            stateAfter = STATES.INT_WAIT_LAT;
            break;
          }

          result = "IN_PROGRESS";
          response = {
            text: "Ingrese la longitud en formato decimal.",
            suggested: ["menu"],
            actions: []
          };
          stateAfter = STATES.INT_WAIT_LON;
          stateData = { ...stateData, lat };
          break;
        }

        case STATES.INT_WAIT_LON: {
          action = "INTERESADO";

          if (!isDecimal(rawText)) {
            result = "INVALID";
            response = {
              text: "La longitud debe ser numérica en formato decimal.",
              suggested: ["menu"],
              actions: []
            };
            stateAfter = STATES.INT_WAIT_LON;
            break;
          }

          const lon = parseNumber(rawText);
          if (lon === null || lon < -180 || lon > 180) {
            result = "INVALID";
            response = {
              text: "La longitud debe estar en el rango -180 a 180.",
              suggested: ["menu"],
              actions: []
            };
            stateAfter = STATES.INT_WAIT_LON;
            break;
          }

          const finalData = {
            ...stateData,
            lon
          };

          const createResult = await interesadoModel.createInteresado({
            channel,
            created_by_user_channel_id: userId,
            created_by_name: userName,
            created_by_web_user_id: event.webUserId || null,
            input_type: "ID_DMS",
            input_value: finalData.id_dms,
            pdv_id: finalData.pdv_id,
            epin_id: finalData.epin_id,
            id_dms: finalData.id_dms,
            epin_reportado: finalData.epin || null,
            telefono: finalData.telefono,
            nombre_pdv: finalData.nombre_pdv,
            propietario: finalData.propietario,
            direccion: finalData.direccion,
            departamento: finalData.departamento,
            municipio: finalData.municipio,
            lat: finalData.lat,
            lon: finalData.lon,
            data_json: {
              conversationId,
              payload,
              createdByRole: event.userRole || null,
              createdByRegion: event.userRegion || null
            }
          });

          result = "SUCCESS";
          response = {
            text:
              "Solicitud registrada.\n" +
              `Solicitud ID: ${createResult.interesadoId}\n` +
              `ID DMS: ${finalData.id_dms || "N/D"}\n` +
              `EPIN: ${finalData.epin || "N/D"}\n` +
              `Nombre PDV: ${finalData.nombre_pdv || "N/D"}\n` +
              `Propietario: ${finalData.propietario || "N/D"}\n` +
              `Teléfono: ${finalData.telefono || "N/D"}\n` +
              `Dirección: ${finalData.direccion || "N/D"}\n` +
              `Departamento: ${finalData.departamento || "N/D"}\n` +
              `Municipio: ${finalData.municipio || "N/D"}\n` +
              `Latitud: ${finalData.lat}\n` +
              `Longitud: ${finalData.lon}`,
            suggested: ["1", "2", "3", "menu"],
            actions: []
          };

          pdvId = finalData.pdv_id || null;
          epinId = finalData.epin_id || null;
          stateAfter = STATES.MENU;
          stateData = {};
          break;
        }

        default: {
          action = "MENU";
          response = menuResponse();
          stateAfter = STATES.MENU;
          stateData = {};
          break;
        }
      }
    }

    await conversationStateModel.upsertState({
      channel,
      user_channel_id: userId,
      conversation_id: conversationId,
      state: stateAfter,
      data_json: stateData
    });
  } catch (error) {
    console.error("ERROR REAL DEL BOT:", {
      message: error.message,
      code: error.code,
      sqlMessage: error.sqlMessage,
      sql: error.sql,
      stack: error.stack
    });
    action = action || "SYSTEM";
    result = "ERROR";
    response = {
      text: "Ocurrió un error procesando la solicitud. Escriba menu para continuar.",
      suggested: ["menu"],
      actions: []
    };
    stateAfter = STATES.MENU;

    try {
      await conversationStateModel.upsertState({
        channel,
        user_channel_id: userId,
        conversation_id: conversationId,
        state: STATES.MENU,
        data_json: {}
      });
    } catch (_) {
      // noop
    }
  }

  try {
    await chatInteractionModel.createInteraction({
      channel,
      user_channel_id: userId,
      user_name: userName,
      conversation_id: conversationId,
      action,
      input_value: rawText || null,
      normalized_input: normalizedText || null,
      result,
      output_text: response.text,
      state_before: stateBefore,
      state_after: stateAfter,
      pdv_id: pdvId,
      epin_id: epinId,
      metadata_json: {
        payload,
        suggested: response.suggested || []
      }
    });
  } catch (_) {
    // noop
  }

  return {
    ...response,
    meta: {
      channel,
      state: stateAfter
    }
  };
}

module.exports = {
  processMessage,
  STATES
};