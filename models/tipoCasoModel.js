const { getPool } = require("../db/pool");

function groupTiposConAreas(rows) {
  const map = new Map();

  for (const row of rows) {
    if (!map.has(row.tipo_caso_id)) {
      map.set(row.tipo_caso_id, {
        tipo_caso_id: row.tipo_caso_id,
        codigo: row.codigo,
        nombre: row.nombre,
        descripcion: row.descripcion,
        descripcion_obligatoria: row.descripcion_obligatoria,
        activo: row.activo,
        orden: row.orden,
        areas: []
      });
    }

    const item = map.get(row.tipo_caso_id);

    if (row.area_responsable_id) {
      item.areas.push({
        area_responsable_id: row.area_responsable_id,
        codigo: row.area_codigo,
        nombre: row.area_nombre,
        orden: row.area_orden
      });
    }
  }

  return Array.from(map.values()).map((item) => ({
    ...item,
    area_responsable_texto: item.areas.map((area) => area.nombre).join(" / "),
    areas_responsables_json: item.areas.map((area) => ({
      area_responsable_id: area.area_responsable_id,
      codigo: area.codigo,
      nombre: area.nombre
    }))
  }));
}

async function findActiveWithAreas() {
  const pool = getPool();

  const sql = `
    SELECT
      tc.tipo_caso_id,
      tc.codigo,
      tc.nombre,
      tc.descripcion,
      tc.descripcion_obligatoria,
      tc.activo,
      tc.orden,

      ar.area_responsable_id,
      ar.codigo AS area_codigo,
      ar.nombre AS area_nombre,
      tca.orden AS area_orden
    FROM tipo_caso tc
    LEFT JOIN tipo_caso_area tca
      ON tca.tipo_caso_id = tc.tipo_caso_id
      AND tca.activo = 1
    LEFT JOIN area_responsable ar
      ON ar.area_responsable_id = tca.area_responsable_id
      AND ar.activo = 1
    WHERE tc.activo = 1
    ORDER BY
      tc.orden ASC,
      tc.tipo_caso_id ASC,
      tca.orden ASC,
      ar.nombre ASC
  `;

  const [rows] = await pool.query(sql);
  return groupTiposConAreas(rows);
}

async function findActiveById(tipoCasoId) {
  const pool = getPool();

  const sql = `
    SELECT
      tc.tipo_caso_id,
      tc.codigo,
      tc.nombre,
      tc.descripcion,
      tc.descripcion_obligatoria,
      tc.activo,
      tc.orden,

      ar.area_responsable_id,
      ar.codigo AS area_codigo,
      ar.nombre AS area_nombre,
      tca.orden AS area_orden
    FROM tipo_caso tc
    LEFT JOIN tipo_caso_area tca
      ON tca.tipo_caso_id = tc.tipo_caso_id
      AND tca.activo = 1
    LEFT JOIN area_responsable ar
      ON ar.area_responsable_id = tca.area_responsable_id
      AND ar.activo = 1
    WHERE tc.activo = 1
      AND tc.tipo_caso_id = ?
    ORDER BY
      tca.orden ASC,
      ar.nombre ASC
  `;

  const [rows] = await pool.query(sql, [tipoCasoId]);
  const grouped = groupTiposConAreas(rows);

  return grouped[0] || null;
}

module.exports = {
  findActiveWithAreas,
  findActiveById
};