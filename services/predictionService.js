const ExcelJS = require("exceljs");

const {
  getPool
} = require("../db/pool");


// =====================================================
// REGIONES
// =====================================================

const REGION_DEPARTMENTS = {
  Central: [
    "Guatemala",
    "Sacatepequez",
    "Chimaltenango",
    "Escuintla"
  ],

  Oriente: [
    "El Progreso",
    "Jalapa",
    "Jutiapa",
    "Santa Rosa",
    "Zacapa",
    "Chiquimula",
    "Izabal"
  ],

  Occidente: [
    "Alta Verapaz",
    "Baja Verapaz",
    "Quiche",
    "Huehuetenango",
    "Quetzaltenango",
    "San Marcos",
    "Totonicapan",
    "Solola",
    "Suchitepequez",
    "Retalhuleu",
    "Peten"
  ]
};


function normalizeText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase();
}


function getDepartmentsForRegion(region) {
  if (!region) {
    return [];
  }

  const normalizedRegion =
    normalizeText(region);

  const entry =
    Object.entries(
      REGION_DEPARTMENTS
    ).find(
      ([key]) =>
        normalizeText(key) ===
        normalizedRegion
    );

  return entry
    ? entry[1]
    : [];
}


// =====================================================
// FILTRO POR ROL
// =====================================================

function buildRegionFilter(
  authUser,
  departmentColumn
) {
  if (
    !authUser ||
    authUser.role === "ADMIN"
  ) {
    return {
      clause: "",
      params: []
    };
  }

  const departments =
    getDepartmentsForRegion(
      authUser.region
    );

  if (!departments.length) {
    return {
      clause: " AND 1 = 0 ",
      params: []
    };
  }

  const placeholders =
    departments
      .map(() => "?")
      .join(",");

  return {
    clause:
      ` AND ${departmentColumn} IN (${placeholders}) `,
    params: departments
  };
}


// =====================================================
// ULTIMO RUN OFICIAL COMPLETADO
// =====================================================

async function getLatestOfficialRun() {
  const pool = getPool();

  const [rows] =
    await pool.query(`
      SELECT
        mr.model_run_id,
        mr.model_version_id,
        mr.batch_id,
        mr.total_epin,
        mr.total_predictions,
        mr.started_at,
        mr.finished_at,
        mv.version_name,
        mv.algorithm
      FROM model_run mr
      INNER JOIN model_version mv
        ON mv.model_version_id =
           mr.model_version_id
      WHERE mr.run_type = 'OFICIAL'
        AND mr.status = 'COMPLETADO'
      ORDER BY
        mr.finished_at DESC,
        mr.model_run_id DESC
      LIMIT 1
    `);

  return rows[0] || null;
}


// =====================================================
// RESUMEN
// =====================================================

async function getPredictionSummary(
  authUser
) {
  const pool = getPool();

  const run =
    await getLatestOfficialRun();

  if (!run) {
    return {
      available: false,
      run: null,
      summary: {
        total: 0,
        verde: 0,
        amarillo: 0,
        rojo: 0,
        highRiskPercentage: 0
      }
    };
  }

  /*
   * El JOIN con stg_bdo se agregará
   * cuando apliquemos el alcance regional.
   *
   * ADMIN ya puede consultar
   * inmediatamente el universo completo.
   */

  const [rows] =
    await pool.query(`
      SELECT
        COUNT(*) AS total,

        SUM(
          CASE
            WHEN p.risk_level = 'VERDE'
            THEN 1
            ELSE 0
          END
        ) AS verde,

        SUM(
          CASE
            WHEN p.risk_level = 'AMARILLO'
            THEN 1
            ELSE 0
          END
        ) AS amarillo,

        SUM(
          CASE
            WHEN p.risk_level = 'ROJO'
            THEN 1
            ELSE 0
          END
        ) AS rojo

      FROM prediction p

      WHERE p.model_run_id = ?
    `, [
      run.model_run_id
    ]);

  const row =
    rows[0] || {};

  const total =
    Number(row.total || 0);

  const rojo =
    Number(row.rojo || 0);

  return {
    available: true,

    run,

    summary: {
      total,

      verde:
        Number(
          row.verde || 0
        ),

      amarillo:
        Number(
          row.amarillo || 0
        ),

      rojo,

      highRiskPercentage:
        total > 0
          ? Number(
              (
                rojo /
                total *
                100
              ).toFixed(2)
            )
          : 0
    }
  };
}

async function getPredictionTop(
    authUser,
    limit = 20
  ) {
    const pool = getPool();
  
    const run =
      await getLatestOfficialRun();
  
    if (!run) {
      return {
        available: false,
        run: null,
        items: []
      };
    }
  
    const safeLimit = Math.min(
      Math.max(
        Number(limit) || 20,
        1
      ),
      100
    );
  
    const regionFilter =
      buildRegionFilter(
        authUser,
        "b.departamento"
      );
  
    const [rows] =
      await pool.query(
        `
          SELECT
            p.prediction_id,
            p.epin,
            p.risk_score,
            p.risk_level,
  
            s.id_dms,
            s.saldo_epin,
            s.estado_epin,
            s.estado_pdv,
  
            b.nombre_pdv,
            b.departamento,
            b.municipio,
            b.direccion,
            b.propietario,
            b.distribuidor
  
          FROM prediction p
  
          INNER JOIN epin_snapshot s
            ON s.batch_id = ?
           AND s.epin = p.epin
  
          LEFT JOIN stg_bdo b
            ON b.batch_id = ?
           AND b.id_dms = s.id_dms
  
          WHERE p.model_run_id = ?
            ${regionFilter.clause}
  
          ORDER BY
            p.risk_score DESC,
            p.prediction_id ASC
  
          LIMIT ${safeLimit}
        `,
        [
          run.batch_id,
          run.batch_id,
          run.model_run_id,
          ...regionFilter.params
        ]
      );
  
    return {
      available: true,
      run,
      items: rows.map(
        (row) => ({
          predictionId:
            row.prediction_id,
  
          epin:
            row.epin,
  
          idDms:
            row.id_dms,
  
          riskScore:
            Number(
              row.risk_score
            ),
  
          riskPercentage:
            Number(
              (
                Number(
                  row.risk_score
                ) * 100
              ).toFixed(2)
            ),
  
          riskLevel:
            row.risk_level,
  
          saldo:
            row.saldo_epin !== null
              ? Number(
                  row.saldo_epin
                )
              : null,
  
          estadoEpin:
            row.estado_epin,
  
          estadoPdv:
            row.estado_pdv,
  
          nombrePdv:
            row.nombre_pdv,
  
          departamento:
            row.departamento,
  
          municipio:
            row.municipio,
  
          direccion:
            row.direccion,
  
          propietario:
            row.propietario,
  
          distribuidor:
            row.distribuidor
        })
      )
    };
  }

  async function exportPredictionsExcel(
    authUser
  ) {
    const pool = getPool();
  
    const run =
      await getLatestOfficialRun();
  
    if (!run) {
      const error =
        new Error(
          "No existe una predicción oficial completada disponible."
        );
  
      error.status = 404;
      throw error;
    }
  
    const regionFilter =
      buildRegionFilter(
        authUser,
        "b.departamento"
      );
  
    const [rows] =
      await pool.query(
        `
          SELECT
            p.epin,
            p.risk_score,
            p.risk_level,
  
            s.id_dms,
            s.saldo_epin,
            s.estado_epin,
            s.estado_pdv,
  
            b.nombre_pdv,
            b.departamento,
            b.municipio,
            b.direccion,
            b.propietario,
            b.distribuidor
  
          FROM prediction p
  
          INNER JOIN epin_snapshot s
            ON s.batch_id = ?
           AND s.epin = p.epin
  
          LEFT JOIN stg_bdo b
            ON b.batch_id = ?
           AND b.id_dms = s.id_dms
  
          WHERE p.model_run_id = ?
            ${regionFilter.clause}
  
          ORDER BY
            p.risk_score DESC,
            p.prediction_id ASC
        `,
        [
          run.batch_id,
          run.batch_id,
          run.model_run_id,
          ...regionFilter.params
        ]
      );
  
    const workbook =
      new ExcelJS.Workbook();
  
    const worksheet =
      workbook.addWorksheet(
        "Predicciones"
      );
  
    worksheet.columns = [
      {
        header: "EPIN",
        key: "epin",
        width: 15
      },
      {
        header: "ID DMS",
        key: "idDms",
        width: 15
      },
      {
        header: "Probabilidad",
        key: "riskPercentage",
        width: 15
      },
      {
        header: "Nivel de riesgo",
        key: "riskLevel",
        width: 16
      },
      {
        header: "Saldo",
        key: "saldo",
        width: 14
      },
      {
        header: "Estado EPIN",
        key: "estadoEpin",
        width: 16
      },
      {
        header: "Estado PDV",
        key: "estadoPdv",
        width: 16
      },
      {
        header: "PDV",
        key: "nombrePdv",
        width: 30
      },
      {
        header: "Departamento",
        key: "departamento",
        width: 18
      },
      {
        header: "Municipio",
        key: "municipio",
        width: 22
      },
      {
        header: "Distribuidor",
        key: "distribuidor",
        width: 24
      },
      {
        header: "Propietario",
        key: "propietario",
        width: 28
      },
      {
        header: "Dirección",
        key: "direccion",
        width: 40
      }
    ];
  
    for (const row of rows) {
      worksheet.addRow({
        epin:
          row.epin,
  
        idDms:
          row.id_dms,
  
        riskPercentage:
          Number(
            (
              Number(
                row.risk_score
              ) * 100
            ).toFixed(2)
          ),
  
        riskLevel:
          row.risk_level,
  
        saldo:
          row.saldo_epin !== null
            ? Number(
                row.saldo_epin
              )
            : null,
  
        estadoEpin:
          row.estado_epin,
  
        estadoPdv:
          row.estado_pdv,
  
        nombrePdv:
          row.nombre_pdv,
  
        departamento:
          row.departamento,
  
        municipio:
          row.municipio,
  
        distribuidor:
          row.distribuidor,
  
        propietario:
          row.propietario,
  
        direccion:
          row.direccion
      });
    }
  
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
      to: "M1"
    };
  
    const content =
      await workbook.xlsx.writeBuffer();
  
    return {
      fileName:
        `predicciones_batch_${run.batch_id}.xlsx`,
      content
    };
  }


module.exports = {
  getLatestOfficialRun,
  getPredictionSummary,
  getPredictionTop,
  exportPredictionsExcel,
  getDepartmentsForRegion,
  buildRegionFilter
};