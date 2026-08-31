const { getPool } = require("../db/pool");
const ExcelJS = require("exceljs");

async function getAvailableEpinCuts() {
  const pool = getPool();

  const [rows] = await pool.query(
    `
    SELECT
      s.batch_id,
      s.previous_batch_id,
      ib.as_of_date
    FROM epin_batch_summary s
    INNER JOIN import_batch ib
      ON ib.batch_id = s.batch_id
    WHERE ib.status = 'done'
    ORDER BY
      ib.as_of_date DESC,
      s.batch_id DESC
    `
  );

  return rows.map((row) => ({
    batchId: Number(row.batch_id),
    previousBatchId:
      row.previous_batch_id === null
        ? null
        : Number(row.previous_batch_id),
    asOfDate: row.as_of_date
  }));
}


async function getEpinAnalysisSummary(batchId = null) {
  const pool = getPool();

  let selectedBatchId = batchId
    ? Number(batchId)
    : null;

  if (!selectedBatchId) {
    const [latestRows] = await pool.query(
        `
        SELECT
          s.batch_id
        FROM epin_batch_summary s
        INNER JOIN import_batch ib
          ON ib.batch_id = s.batch_id
        WHERE ib.status = 'done'
        ORDER BY
          ib.as_of_date DESC,
          s.batch_id DESC
        LIMIT 1
        `
      );

    if (!latestRows.length) {
      throw new Error(
        "No hay cortes EPIN disponibles"
      );
    }

    selectedBatchId =
      Number(latestRows[0].batch_id);
  }

  const [rows] = await pool.query(
    `
    SELECT
      s.batch_id,
      s.previous_batch_id,
      s.total_activos,
      s.total_bloqueados,
      s.total_reactivados,
      s.total_nuevos,
      ib.as_of_date
    FROM epin_batch_summary s
    INNER JOIN import_batch ib
      ON ib.batch_id = s.batch_id
    WHERE s.batch_id = ?
      AND ib.status = 'done'
    LIMIT 1
    `,
    [selectedBatchId]
  );

  if (!rows.length) {
    throw new Error(
      `El batch ${selectedBatchId} no tiene análisis EPIN`
    );
  }

  const row = rows[0];

  return {
    batchId: Number(row.batch_id),

    previousBatchId:
      row.previous_batch_id === null
        ? null
        : Number(row.previous_batch_id),

    asOfDate: row.as_of_date,

    isBase:
      row.previous_batch_id === null,

    activos:
      Number(row.total_activos || 0),

    bloqueados:
      Number(row.total_bloqueados || 0),

    reactivados:
      Number(row.total_reactivados || 0),

    nuevos:
      Number(row.total_nuevos || 0)
  };
}

async function getEpinEventPreview(
    batchId,
    eventType,
    limit = 20
  ) {
    const pool = getPool();
  
    const allowedTypes = [
      "BLOQUEADO",
      "REACTIVADO",
      "NUEVO"
    ];
  
    const normalizedType =
      String(eventType || "").toUpperCase();
  
    if (!allowedTypes.includes(normalizedType)) {
      throw new Error(
        `Tipo de evento no válido: ${eventType}`
      );
    }
  
    const selectedBatchId = Number(batchId);
  
    if (!selectedBatchId) {
      throw new Error("Batch inválido");
    }
  
    const safeLimit = Math.min(
      Math.max(Number(limit) || 20, 1),
      20
    );
  
    const [summaryRows] = await pool.query(
      `
      SELECT
        batch_id,
        previous_batch_id
      FROM epin_batch_summary
      WHERE batch_id = ?
      LIMIT 1
      `,
      [selectedBatchId]
    );
  
    if (!summaryRows.length) {
      throw new Error(
        `El batch ${selectedBatchId} no tiene análisis EPIN`
      );
    }
  
    const previousBatchId =
      summaryRows[0].previous_batch_id === null
        ? null
        : Number(
            summaryRows[0].previous_batch_id
          );
  
    const [[countRow]] = await pool.query(
      `
      SELECT COUNT(*) AS total
      FROM epin_event
      WHERE batch_id = ?
        AND event_type = ?
      `,
      [
        selectedBatchId,
        normalizedType
      ]
    );
  
    const [rows] = await pool.query(
      `
      SELECT
        ev.event_id,
        ev.event_type,
        e.epin,
        p.id_dms,
        p.nombre_pdv,
        p.propietario,
        p.departamento,
        p.municipio,
        p.distribuidor,
        p.categoria
  
      FROM epin_event ev
  
      INNER JOIN epin e
        ON e.epin_id = ev.epin_id
  
      LEFT JOIN epin_snapshot snap
        ON snap.batch_id = ev.batch_id
        AND snap.epin_id = ev.epin_id
  
      LEFT JOIN pdv p
        ON p.pdv_id = COALESCE(
          snap.pdv_id,
          e.pdv_id
        )
  
      WHERE ev.batch_id = ?
        AND ev.event_type = ?
  
      ORDER BY e.epin ASC
      LIMIT ?
      `,
      [
        selectedBatchId,
        normalizedType,
        safeLimit
      ]
    );
  
    return {
      batchId: selectedBatchId,
      previousBatchId,
      eventType: normalizedType,
      total: Number(countRow?.total || 0),
  
      items: rows.map((row) => ({
        eventId: Number(row.event_id),
        eventType: row.event_type,
        epin: row.epin,
        idDms: row.id_dms,
        nombrePdv: row.nombre_pdv,
        propietario: row.propietario,
        departamento: row.departamento,
        municipio: row.municipio,
        distribuidor: row.distribuidor,
        categoria: row.categoria
      }))
    };
}

async function exportEpinEventsExcel(
    batchId,
    eventType
  ) {
    const pool = getPool();
  
    const allowedTypes = [
      "BLOQUEADO",
      "REACTIVADO",
      "NUEVO"
    ];
  
    const normalizedType =
      String(eventType || "").toUpperCase();
  
    if (!allowedTypes.includes(normalizedType)) {
      throw new Error(
        `Tipo de evento no válido: ${eventType}`
      );
    }
  
    const selectedBatchId = Number(batchId);
  
    if (!selectedBatchId) {
      throw new Error("Batch inválido");
    }
  
    const [summaryRows] = await pool.query(
      `
      SELECT
        s.batch_id,
        s.previous_batch_id,
        ib.as_of_date
      FROM epin_batch_summary s
      INNER JOIN import_batch ib
        ON ib.batch_id = s.batch_id
      WHERE s.batch_id = ?
        AND ib.status = 'done'
      LIMIT 1
      `,
      [selectedBatchId]
    );
  
    if (!summaryRows.length) {
      throw new Error(
        `El batch ${selectedBatchId} no tiene análisis EPIN`
      );
    }
  
    const summary = summaryRows[0];
  
    const previousBatchId =
      summary.previous_batch_id === null
        ? null
        : Number(summary.previous_batch_id);
  
    const [rows] = await pool.query(
      `
      SELECT
        ev.event_type,
        e.epin,
  
        p.id_dms,
        p.nombre_pdv,
        p.propietario,
        p.departamento,
        p.municipio,
        p.distribuidor,
        p.categoria
  
      FROM epin_event ev
  
      INNER JOIN epin e
        ON e.epin_id = ev.epin_id
  
      LEFT JOIN epin_snapshot snap
        ON snap.batch_id = ev.batch_id
        AND snap.epin_id = ev.epin_id
  
      LEFT JOIN pdv p
        ON p.pdv_id = COALESCE(
          snap.pdv_id,
          e.pdv_id
        )
  
      WHERE ev.batch_id = ?
        AND ev.event_type = ?
  
      ORDER BY e.epin ASC
      `,
      [
        selectedBatchId,
        normalizedType
      ]
    );
  
    const workbook = new ExcelJS.Workbook();
  
    const worksheet =
      workbook.addWorksheet(normalizedType);
  
    worksheet.columns = [
      {
        header: "Evento",
        key: "evento",
        width: 16
      },
      {
        header: "EPIN",
        key: "epin",
        width: 16
      },
      {
        header: "ID DMS",
        key: "id_dms",
        width: 16
      },
      {
        header: "Nombre PDV",
        key: "nombre_pdv",
        width: 28
      },
      {
        header: "Propietario",
        key: "propietario",
        width: 28
      },
      {
        header: "Departamento",
        key: "departamento",
        width: 20
      },
      {
        header: "Municipio",
        key: "municipio",
        width: 22
      },
      {
        header: "Distribuidor",
        key: "distribuidor",
        width: 18
      },
      {
        header: "Categoría",
        key: "categoria",
        width: 24
      }
    ];
  
    worksheet.getRow(1).font = {
      bold: true
    };
  
    worksheet.views = [
      {
        state: "frozen",
        ySplit: 1
      }
    ];
  
    worksheet.autoFilter = {
      from: "A1",
      to: "J1"
    };
  
    rows.forEach((row) => {
      worksheet.addRow({
        evento: row.event_type,
        epin: row.epin,
        id_dms: row.id_dms,
        nombre_pdv: row.nombre_pdv,
        propietario: row.propietario,
        departamento: row.departamento,
        municipio: row.municipio,
        distribuidor: row.distribuidor,
        categoria: row.categoria,
      });
    });
  
    const buffer =
      await workbook.xlsx.writeBuffer();
  
    const dateLabel =
      summary.as_of_date
        ? new Date(summary.as_of_date)
            .toISOString()
            .slice(0, 10)
        : `batch_${selectedBatchId}`;
  
    return {
      fileName:
        `${normalizedType.toLowerCase()}_epin_${dateLabel}.xlsx`,
  
      content: buffer,
  
      total: rows.length
    };
  }


module.exports = {
  getAvailableEpinCuts,
  getEpinAnalysisSummary,
  getEpinEventPreview,
  exportEpinEventsExcel
};