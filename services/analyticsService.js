const { getPool } = require("../db/pool");
const ExcelJS = require("exceljs");

async function getLatestDoneBatch() {
  const pool = getPool();

  const [rows] = await pool.query(
    `
    SELECT batch_id, as_of_date, created_at
    FROM import_batch
    WHERE status = 'done'
    ORDER BY as_of_date DESC, batch_id DESC
    LIMIT 1
    `
  );

  return rows[0] || null;
}

async function getEpinSummary() {
  const pool = getPool();
  const latestBatch = await getLatestDoneBatch();

  if (!latestBatch) {
    throw new Error("No hay cortes disponibles");
  }

  const [[row]] = await pool.query(
    `
    SELECT
      COUNT(*) AS total_epins,
      SUM(CASE WHEN estado_epin = 'ACTIVO' THEN 1 ELSE 0 END) AS activos,
      SUM(CASE WHEN estado_epin = 'BLOQUEADO' THEN 1 ELSE 0 END) AS bloqueados
    FROM epin_snapshot
    WHERE batch_id = ?
    `,
    [latestBatch.batch_id]
  );

  return {
    batchId: latestBatch.batch_id,
    asOfDate: latestBatch.as_of_date,
    totalEpins: Number(row?.total_epins || 0),
    activos: Number(row?.activos || 0),
    bloqueados: Number(row?.bloqueados || 0)
  };
}

async function getEpinRecencyDistribution() {
  const pool = getPool();
  const latestBatch = await getLatestDoneBatch();

  if (!latestBatch) {
    throw new Error("No hay cortes disponibles");
  }

  const [[row]] = await pool.query(
    `
    SELECT
      SUM(CASE WHEN days_since BETWEEN 0 AND 7 THEN 1 ELSE 0 END) AS bucket_0_7,
      SUM(CASE WHEN days_since BETWEEN 8 AND 30 THEN 1 ELSE 0 END) AS bucket_8_30,
      SUM(CASE WHEN days_since BETWEEN 31 AND 60 THEN 1 ELSE 0 END) AS bucket_31_60,
      SUM(CASE WHEN days_since >= 61 THEN 1 ELSE 0 END) AS bucket_61_plus
    FROM (
      SELECT
        e.epin_id,
        COALESCE(TIMESTAMPDIFF(DAY, ib_last.as_of_date, ?), 9999) AS days_since
      FROM epin e
      LEFT JOIN import_batch ib_last
        ON ib_last.batch_id = e.last_seen_batch_id
      WHERE e.activo = 1
    ) t
    `,
    [latestBatch.as_of_date]
  );

  return {
    batchId: latestBatch.batch_id,
    asOfDate: latestBatch.as_of_date,
    buckets: [
      { label: "0-7 días", value: Number(row?.bucket_0_7 || 0) },
      { label: "8-30 días", value: Number(row?.bucket_8_30 || 0) },
      { label: "31-60 días", value: Number(row?.bucket_31_60 || 0) },
      { label: "61+ días", value: Number(row?.bucket_61_plus || 0) }
    ]
  };
}

async function getEpinSegments(groupBy = "departamento") {
    const pool = getPool();
    const latestBatch = await getLatestDoneBatch();
  
    if (!latestBatch) {
      throw new Error("No hay cortes disponibles");
    }
  
    const groupMap = {
      departamento: "p.departamento",
      distribuidor: "p.distribuidor",
      categoria: "p.categoria"
    };
  
    const groupColumn = groupMap[groupBy] || groupMap.departamento;
  
    const [rows] = await pool.query(
      `
      SELECT
        COALESCE(${groupColumn}, 'Sin dato') AS segment,
        COUNT(*) AS total_epins,
        SUM(CASE WHEN s.estado_epin = 'ACTIVO' THEN 1 ELSE 0 END) AS activos,
        SUM(CASE WHEN s.estado_epin = 'BLOQUEADO' THEN 1 ELSE 0 END) AS bloqueados,
        ROUND(
          (SUM(CASE WHEN s.estado_epin = 'BLOQUEADO' THEN 1 ELSE 0 END) / COUNT(*)) * 100,
          2
        ) AS pct_bloqueado
      FROM epin_snapshot s
      LEFT JOIN pdv p ON p.pdv_id = s.pdv_id
      WHERE s.batch_id = ?
      GROUP BY COALESCE(${groupColumn}, 'Sin dato')
      ORDER BY total_epins DESC, segment ASC
      `,
      [latestBatch.batch_id]
    );
  
    return {
      batchId: latestBatch.batch_id,
      asOfDate: latestBatch.as_of_date,
      groupBy,
      items: rows.map((row) => ({
        segment: row.segment,
        totalEpins: Number(row.total_epins || 0),
        activos: Number(row.activos || 0),
        bloqueados: Number(row.bloqueados || 0),
        pctBloqueado: Number(row.pct_bloqueado || 0)
      }))
    };
  }
  

  async function exportBlockedInactivePdvsExcel(groupBy = "departamento") {
    const pool = getPool();
    const latestBatch = await getLatestDoneBatch();
  
    if (!latestBatch) {
      throw new Error("No hay cortes disponibles");
    }
  
    const groupMap = {
      departamento: "p.departamento",
      distribuidor: "p.distribuidor",
      categoria: "p.categoria"
    };
  
    const selectedGroupBy = groupMap[groupBy] ? groupBy : "departamento";
    const groupColumn = groupMap[selectedGroupBy];
  
    const [rows] = await pool.query(
      `
      SELECT
        COALESCE(${groupColumn}, 'Sin dato') AS segmento,
        p.id_dms,
        p.nombre_pdv,
        p.propietario,
        p.departamento,
        p.municipio,
        p.direccion,
        p.distribuidor,
        p.categoria,
        s.epin,
        s.estado_epin,
        s.saldo_epin
      FROM epin_snapshot s
      LEFT JOIN pdv p
        ON p.pdv_id = s.pdv_id
      WHERE s.batch_id = ?
        AND s.estado_epin IN ('BLOQUEADO')
      ORDER BY
        COALESCE(${groupColumn}, 'Sin dato') ASC,
        p.nombre_pdv ASC,
        s.epin ASC
      `,
      [latestBatch.batch_id]
    );
  
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet("Segmentación EPIN");
  
    worksheet.columns = [
      { header: "Segmento", key: "segmento", width: 24 },
      { header: "ID DMS", key: "id_dms", width: 16 },
      { header: "Nombre PDV", key: "nombre_pdv", width: 28 },
      { header: "Propietario", key: "propietario", width: 24 },
      { header: "Departamento", key: "departamento", width: 18 },
      { header: "Municipio", key: "municipio", width: 18 },
      { header: "Dirección", key: "direccion", width: 34 },
      { header: "Distribuidor", key: "distribuidor", width: 18 },
      { header: "Tipo PDV", key: "categoria", width: 18 },
      { header: "EPIN", key: "epin", width: 14 },
      { header: "Estado EPIN", key: "estado_epin", width: 16 },
      { header: "Saldo EPIN", key: "saldo_epin", width: 14 }
    ];
  
    worksheet.getRow(1).font = { bold: true };
    worksheet.views = [{ state: "frozen", ySplit: 1 }];
    worksheet.autoFilter = {
      from: "A1",
      to: "L1"
    };
  
    rows.forEach((row) => {
      worksheet.addRow({
        segmento: row.segmento,
        id_dms: row.id_dms,
        nombre_pdv: row.nombre_pdv,
        propietario: row.propietario,
        departamento: row.departamento,
        municipio: row.municipio,
        direccion: row.direccion,
        distribuidor: row.distribuidor,
        categoria: row.categoria,
        epin: row.epin,
        estado_epin: row.estado_epin,
        saldo_epin: row.saldo_epin
      });
    });
  
    const buffer = await workbook.xlsx.writeBuffer();
  
    return {
      fileName: `segmentacion_epin_${selectedGroupBy}_${String(latestBatch.as_of_date).slice(0, 10)}.xlsx`,
      content: buffer
    };
  }


  /* Tenndencias */

  async function getEpinTrendSeries(limit = 12) {
    const pool = getPool();
    const safeLimit = Math.min(Math.max(Number(limit) || 12, 1), 24);
  
    const [rows] = await pool.query(
      `
      SELECT
        ib.batch_id,
        ib.as_of_date,
        SUM(CASE WHEN s.estado_epin = 'ACTIVO' THEN 1 ELSE 0 END) AS activos,
        SUM(CASE WHEN s.estado_epin = 'BLOQUEADO' THEN 1 ELSE 0 END) AS bloqueados,
        SUM(CASE WHEN s.estado_epin IN ('ACTIVO', 'BLOQUEADO') THEN 1 ELSE 0 END) AS total_relevante
      FROM import_batch ib
      INNER JOIN epin_snapshot s
        ON s.batch_id = ib.batch_id
      WHERE ib.status = 'done'
      GROUP BY ib.batch_id, ib.as_of_date
      ORDER BY ib.as_of_date DESC, ib.batch_id DESC
      LIMIT ?
      `,
      [safeLimit]
    );
  
    const items = rows
      .map((row) => ({
        batchId: row.batch_id,
        asOfDate: row.as_of_date,
        label: String(row.as_of_date).slice(0, 10),
        activos: Number(row.activos || 0),
        bloqueados: Number(row.bloqueados || 0),
        totalRelevante: Number(row.total_relevante || 0)
      }))
      .reverse();
  
    return {
      totalCuts: items.length,
      items
    };
  }
  
  async function getEpinTrendComparison() {
    const series = await getEpinTrendSeries(2);
    const items = series.items || [];
  
    if (!items.length) {
      throw new Error("No hay cortes disponibles");
    }
  
    const current = items[items.length - 1];
    const previous = items.length > 1 ? items[items.length - 2] : null;
  
    if (!previous) {
      return {
        current,
        previous: null,
        comparison: {
          activosDelta: 0,
          bloqueadosDelta: 0,
          activosPct: 0,
          bloqueadosPct: 0
        }
      };
    }
  
    const activosDelta = current.activos - previous.activos;
    const bloqueadosDelta = current.bloqueados - previous.bloqueados;
  
    const activosPct =
      previous.activos > 0 ? Number(((activosDelta / previous.activos) * 100).toFixed(2)) : 0;
  
    const bloqueadosPct =
      previous.bloqueados > 0
        ? Number(((bloqueadosDelta / previous.bloqueados) * 100).toFixed(2))
        : 0;
  
    return {
      current,
      previous,
      comparison: {
        activosDelta,
        bloqueadosDelta,
        activosPct,
        bloqueadosPct
      }
    };
  }

module.exports = {
  getLatestDoneBatch,
  getEpinSummary,
  getEpinRecencyDistribution,
  getEpinSegments,
  exportBlockedInactivePdvsExcel,
  getEpinTrendSeries,
  getEpinTrendComparison
};