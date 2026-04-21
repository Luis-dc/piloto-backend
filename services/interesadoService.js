const ExcelJS = require("exceljs");
const interesadoModel = require("../models/interesadoModel");

function resolvePeriodo(lastCreatedAt) {
  const baseDate = lastCreatedAt ? new Date(lastCreatedAt) : new Date();

  return {
    defaultYear: baseDate.getFullYear(),
    defaultMonth: baseDate.getMonth() + 1
  };
}

async function getPeriodos() {
  const [lastCreatedAt, yearsFromDb] = await Promise.all([
    interesadoModel.getUltimoPeriodoConInteresados(),
    interesadoModel.getAniosDisponibles()
  ]);

  const { defaultYear, defaultMonth } = resolvePeriodo(lastCreatedAt);

  const years = yearsFromDb.length
    ? Array.from(new Set([defaultYear, ...yearsFromDb])).sort((a, b) => b - a)
    : [defaultYear];

  return {
    defaultYear,
    defaultMonth,
    years
  };
}

async function getResumenER(user, filters = {}) {
  if (!["SUPERVISOR", "ADMIN"].includes(user.role)) {
    throw new Error("Rol no autorizado");
  }

  const periodos = await getPeriodos();
  const year = Number(filters.year) || periodos.defaultYear;
  const month = Number(filters.month) || periodos.defaultMonth;

  const items = await interesadoModel.getResumenPorEr({
    user,
    year,
    month
  });

  return {
    periodos,
    selectedYear: year,
    selectedMonth: month,
    items
  };
}

function mapRowsForExport(rows = []) {
  return rows.map((item) => ({
    interesado_id: item.interesado_id,
    fecha: item.created_at,
    canal: item.channel,
    er: item.er_name || item.created_by_name || "N/D",
    correo_er: item.er_email || "N/D",
    region: item.region || "N/D",
    id_dms: item.id_dms || "",
    epin: item.epin_reportado || "",
    nombre_pdv: item.nombre_pdv || "",
    propietario: item.propietario || "",
    telefono: item.telefono || "",
    direccion: item.direccion || "",
    departamento: item.departamento || "",
    municipio: item.municipio || "",
    lat: item.lat ?? "",
    lon: item.lon ?? "",
    exportado: item.exported_at ? "SI" : "NO"
  }));
}

function escapeCsv(value) {
  if (value === null || value === undefined) return "";
  const str = String(value);
  if (str.includes('"') || str.includes(",") || str.includes("\n")) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function buildCsv(rows = []) {
  const headers = [
    "ID",
    "Fecha",
    "Canal",
    "ER",
    "Correo ER",
    "Region",
    "ID DMS",
    "EPIN",
    "Nombre PDV",
    "Propietario",
    "Telefono",
    "Direccion",
    "Departamento",
    "Municipio",
    "Latitud",
    "Longitud",
    "Exportado"
  ];

  const lines = [headers.join(",")];

  rows.forEach((row) => {
    lines.push(
      [
        row.interesado_id,
        row.fecha,
        row.canal,
        row.er,
        row.correo_er,
        row.region,
        row.id_dms,
        row.epin,
        row.nombre_pdv,
        row.propietario,
        row.telefono,
        row.direccion,
        row.departamento,
        row.municipio,
        row.lat,
        row.lon,
        row.exportado
      ]
        .map(escapeCsv)
        .join(",")
    );
  });

  return lines.join("\n");
}

async function buildXlsx(rows = []) {
  const workbook = new ExcelJS.Workbook();
  const worksheet = workbook.addWorksheet("Interesados");

  worksheet.columns = [
    { header: "ID", key: "interesado_id", width: 12 },
    { header: "Fecha", key: "fecha", width: 24 },
    { header: "Canal", key: "canal", width: 12 },
    { header: "ER", key: "er", width: 24 },
    { header: "Correo ER", key: "correo_er", width: 28 },
    { header: "Región", key: "region", width: 18 },
    { header: "ID DMS", key: "id_dms", width: 16 },
    { header: "EPIN", key: "epin", width: 16 },
    { header: "Nombre PDV", key: "nombre_pdv", width: 28 },
    { header: "Propietario", key: "propietario", width: 24 },
    { header: "Teléfono", key: "telefono", width: 16 },
    { header: "Dirección", key: "direccion", width: 32 },
    { header: "Departamento", key: "departamento", width: 18 },
    { header: "Municipio", key: "municipio", width: 18 },
    { header: "Latitud", key: "lat", width: 14 },
    { header: "Longitud", key: "lon", width: 14 },
    { header: "Exportado", key: "exportado", width: 14 }
  ];

  rows.forEach((row) => worksheet.addRow(row));

  worksheet.getRow(1).font = { bold: true };
  worksheet.views = [{ state: "frozen", ySplit: 1 }];

  return workbook.xlsx.writeBuffer();
}

async function exportInteresados(user, filters = {}, format = "csv") {
  if (!["SUPERVISOR", "ADMIN"].includes(user.role)) {
    throw new Error("Rol no autorizado");
  }

  const periodos = await getPeriodos();

  const exportFilters = {
    createdByWebUserId: filters.createdByWebUserId || null,
    year: Number(filters.year) || periodos.defaultYear,
    month: Number(filters.month) || periodos.defaultMonth
  };

  const rawRows = await interesadoModel.findForExport(user, exportFilters);
  const rows = mapRowsForExport(rawRows);
  const ids = rawRows.map((item) => item.interesado_id);

  await interesadoModel.markExported(
    ids,
    user.uid,
    exportFilters.createdByWebUserId
      ? `Exportación por ejecutivo ${exportFilters.month}/${exportFilters.year}`
      : `Exportación general ${exportFilters.month}/${exportFilters.year}`
  );

  if (format === "xlsx") {
    const buffer = await buildXlsx(rows);
    return {
      contentType:
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      extension: "xlsx",
      buffer
    };
  }

  const csv = buildCsv(rows);
  return {
    contentType: "text/csv; charset=utf-8",
    extension: "csv",
    buffer: Buffer.from(csv, "utf-8")
  };
}

module.exports = {
  getPeriodos,
  getResumenER,
  exportInteresados
};