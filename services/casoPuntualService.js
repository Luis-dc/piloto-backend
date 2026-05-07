const ExcelJS = require("exceljs");
const casoPuntualModel = require("../models/casoPuntualModel");

function resolvePeriodo(lastCreatedAt) {
  const baseDate = lastCreatedAt ? new Date(lastCreatedAt) : new Date();

  return {
    defaultYear: baseDate.getFullYear(),
    defaultMonth: baseDate.getMonth() + 1
  };
}

async function getPeriodos() {
  const [lastCreatedAt, yearsFromDb] = await Promise.all([
    casoPuntualModel.getUltimoPeriodoConCasos(),
    casoPuntualModel.getAniosDisponibles()
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

  const items = await casoPuntualModel.getResumenPorEr({
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
    caso_puntual_id: item.caso_puntual_id,
    fecha: item.created_at,
    canal: item.channel,
    er: item.er_name || item.created_by_name || "N/D",
    correo_er: item.er_email || "N/D",
    region: item.region || "N/D",

    id_dms: item.id_dms || "",
    epin: item.epin_reportado || "",
    otros_epin: item.otros_epin || "",

    nombre_pdv: item.nombre_pdv || "",
    propietario: item.propietario || "",
    direccion: item.direccion || "",
    departamento: item.departamento || "",
    municipio: item.municipio || "",
    circuito: item.circuito || "",
    distribuidor: item.distribuidor || "",
    categoria: item.categoria || "",

    estado_pdv: item.estado_pdv || "",
    estado_epin: item.estado_epin || "",
    mi_tienda:
      item.mi_tienda === 1 ? "SI" : item.mi_tienda === 0 ? "NO" : "",

    tipo_caso: item.tipo_caso_nombre || "",
    area_responsable: item.area_responsable_texto || "",
    descripcion: item.descripcion || "",

    contacto_referencia: item.contacto_referencia || "",
    telefono_referencia: item.telefono_referencia || "",

    lat: item.lat ?? "",
    lon: item.lon ?? "",

    exportado: item.exported_at ? "SI" : "NO"
  }));
}

async function buildXlsx(rows = []) {
  const workbook = new ExcelJS.Workbook();
  const worksheet = workbook.addWorksheet("Casos Puntuales");

  worksheet.columns = [
    { header: "ID", key: "caso_puntual_id", width: 12 },
    { header: "Fecha", key: "fecha", width: 24 },
    { header: "Canal", key: "canal", width: 12 },
    { header: "ER", key: "er", width: 24 },
    { header: "Correo ER", key: "correo_er", width: 28 },
    { header: "Region", key: "region", width: 18 },

    { header: "ID DMS", key: "id_dms", width: 16 },
    { header: "EPIN", key: "epin", width: 16 },
    { header: "Otros EPIN", key: "otros_epin", width: 26 },

    { header: "Nombre PDV", key: "nombre_pdv", width: 28 },
    { header: "Propietario", key: "propietario", width: 24 },
    { header: "Direccion", key: "direccion", width: 32 },
    { header: "Departamento", key: "departamento", width: 18 },
    { header: "Municipio", key: "municipio", width: 18 },
    { header: "Circuito", key: "circuito", width: 18 },
    { header: "Distribuidor", key: "distribuidor", width: 22 },
    { header: "Categoria", key: "categoria", width: 20 },

    { header: "Estado PDV", key: "estado_pdv", width: 16 },
    { header: "Estado EPIN", key: "estado_epin", width: 16 },
    { header: "Mi Tienda", key: "mi_tienda", width: 14 },

    { header: "Tipo Caso", key: "tipo_caso", width: 28 },
    { header: "Area Responsable", key: "area_responsable", width: 28 },
    { header: "Descripcion", key: "descripcion", width: 40 },

    { header: "Contacto Referencia", key: "contacto_referencia", width: 26 },
    { header: "Telefono Referencia", key: "telefono_referencia", width: 20 },

    { header: "Latitud", key: "lat", width: 14 },
    { header: "Longitud", key: "lon", width: 14 },
    { header: "Exportado", key: "exportado", width: 14 }
  ];

  rows.forEach((row) => worksheet.addRow(row));

  worksheet.getRow(1).font = { bold: true };
  worksheet.views = [{ state: "frozen", ySplit: 1 }];

  return workbook.xlsx.writeBuffer();
}

async function exportCasosPuntuales(user, filters = {}) {
  if (!["SUPERVISOR", "ADMIN"].includes(user.role)) {
    throw new Error("Rol no autorizado");
  }

  const periodos = await getPeriodos();

  const exportFilters = {
    createdByWebUserId: filters.createdByWebUserId || null,
    year: Number(filters.year) || periodos.defaultYear,
    month: Number(filters.month) || periodos.defaultMonth
  };

  const rawRows = await casoPuntualModel.findForExport(user, exportFilters);
  const rows = mapRowsForExport(rawRows);
  const ids = rawRows.map((item) => item.caso_puntual_id);

  await casoPuntualModel.markExported(
    ids,
    user.uid,
    exportFilters.createdByWebUserId
      ? `Exportacion por ejecutivo ${exportFilters.month}/${exportFilters.year}`
      : `Exportacion general ${exportFilters.month}/${exportFilters.year}`
  );

  const buffer = await buildXlsx(rows);

  return {
    contentType:
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    extension: "xlsx",
    buffer
  };
}

module.exports = {
  getPeriodos,
  getResumenER,
  exportCasosPuntuales
};