const mailService = require("./mailService");

function toText(value) {
  return value == null ? "" : String(value).trim();
}

function normalizeKey(value) {
  return toText(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function safe(value, fallback = "N/D") {
  const text = toText(value);
  return text || fallback;
}

function escapeHtml(value) {
  return safe(value, "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function formatDateGt(date = new Date()) {
  return new Intl.DateTimeFormat("es-GT", {
    timeZone: "America/Guatemala",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  })
    .format(date)
    .replace(",", "");
}

function getSupervisorEmailByRegion(region) {
  const normalizedRegion = normalizeKey(region);

  const regionMap = {
    central: process.env.DMS_SUPERVISOR_CENTRAL,
    oriente: process.env.DMS_SUPERVISOR_ORIENTE,
    occidente: process.env.DMS_SUPERVISOR_OCCIDENTE
  };

  return regionMap[normalizedRegion] || null;
}

function buildCasoPuntualEmail({ casoPuntualId, caso, createdByName, createdAt }) {
  const areaResponsable = safe(caso.area_responsable_texto, "Sin área responsable");
  const descripcion = safe(caso.descripcion, "Sin descripción registrada.");
  const fechaRegistro = formatDateGt(createdAt || new Date());

  const subject = `Nuevo caso puntual registrado - ${areaResponsable}`;

  const text =
`Se ha registrado un nuevo caso puntual en SmartTrack.

Información del PDV:

ID DMS: ${safe(caso.id_dms)}
Nombre PDV: ${safe(caso.nombre_pdv)}
Departamento: ${safe(caso.departamento)}
Municipio: ${safe(caso.municipio)}
Dirección: ${safe(caso.direccion)}
Distribuidor: ${safe(caso.distribuidor)}

Información del caso:

ID de caso: ${casoPuntualId}
Tipo de caso: ${safe(caso.tipo_caso_nombre)}
Área responsable: ${areaResponsable}
Descripción: ${descripcion}

Contacto de referencia:

Nombre: ${safe(caso.contacto_referencia)}
Teléfono: ${safe(caso.telefono_referencia)}

Registrado por:

Ejecutivo: ${safe(createdByName)}
Fecha de registro: ${fechaRegistro}

Puede revisar el detalle completo desde el módulo de Gestiones en SmartTrack.`;

  const html = `
    <div style="font-family: Arial, sans-serif; color: #222; line-height: 1.5;">
      <p>Se ha registrado un nuevo caso puntual en <strong>SmartTrack</strong>.</p>

      <h3>Información del PDV:</h3>
      <p>
        <strong>ID DMS:</strong> ${escapeHtml(caso.id_dms)}<br>
        <strong>Nombre PDV:</strong> ${escapeHtml(caso.nombre_pdv)}<br>
        <strong>Departamento:</strong> ${escapeHtml(caso.departamento)}<br>
        <strong>Municipio:</strong> ${escapeHtml(caso.municipio)}<br>
        <strong>Dirección:</strong> ${escapeHtml(caso.direccion)}<br>
        <strong>Distribuidor:</strong> ${escapeHtml(caso.distribuidor)}
      </p>

      <h3>Información del caso:</h3>
      <p>
        <strong>ID de caso:</strong> ${escapeHtml(casoPuntualId)}<br>
        <strong>Tipo de caso:</strong> ${escapeHtml(caso.tipo_caso_nombre)}<br>
        <strong>Área responsable:</strong> ${escapeHtml(areaResponsable)}<br>
        <strong>Descripción:</strong> ${escapeHtml(descripcion)}
      </p>

      <h3>Contacto de referencia:</h3>
      <p>
        <strong>Nombre:</strong> ${escapeHtml(caso.contacto_referencia)}<br>
        <strong>Teléfono:</strong> ${escapeHtml(caso.telefono_referencia)}
      </p>

      <h3>Registrado por:</h3>
      <p>
        <strong>Ejecutivo:</strong> ${escapeHtml(createdByName)}<br>
        <strong>Fecha de registro:</strong> ${escapeHtml(fechaRegistro)}
      </p>

      <p>Puede revisar el detalle completo desde el módulo de Gestiones en SmartTrack.</p>
    </div>
  `;

  return {
    subject,
    text,
    html
  };
}

async function notifyCasoPuntualCreated({ casoPuntualId, caso, createdByName, createdByRegion }) {
  const supervisorEmail = getSupervisorEmailByRegion(createdByRegion);
  const adminEmail = process.env.CASOS_CC_ADMIN || null;

  if (!supervisorEmail && !adminEmail) {
    console.warn("[CASO_PUNTUAL_EMAIL_SKIP] No hay supervisor ni admin configurado para enviar correo.", {
      casoPuntualId,
      createdByRegion
    });

    return {
      ok: false,
      skipped: true,
      reason: "NO_RECIPIENTS"
    };
  }

  const to = supervisorEmail || adminEmail;
  const cc = supervisorEmail && adminEmail ? adminEmail : null;

  if (!supervisorEmail) {
    console.warn("[CASO_PUNTUAL_EMAIL_WARN] No se encontró supervisor DMS para la región. Se enviará solo al admin.", {
      casoPuntualId,
      createdByRegion
    });
  }

  const email = buildCasoPuntualEmail({
    casoPuntualId,
    caso,
    createdByName,
    createdAt: new Date()
  });

  const info = await mailService.sendMail({
    to,
    cc,
    subject: email.subject,
    text: email.text,
    html: email.html
  });

  console.log("[CASO_PUNTUAL_EMAIL_SENT]", {
    casoPuntualId,
    to,
    cc,
    messageId: info.messageId
  });

  return {
    ok: true,
    messageId: info.messageId
  };
}

module.exports = {
  notifyCasoPuntualCreated
};