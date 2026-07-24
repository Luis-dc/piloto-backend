-- SMARTTRACK - reparación única de EPIN obsoletos
--
-- Objetivo:
--   Sincronizar las banderas operativas de epin con el último snapshot
--   cuyo import_batch terminó con status='done'.
--
-- Conserva last_seen_batch_id, last_seen_bdo_batch_id y
-- last_seen_2cnv_batch_id porque representan la última aparición histórica.
--
-- Recomendación: ejecutar primero en un respaldo o ambiente de pruebas.

START TRANSACTION;

SET @latest_done_batch_id := (
  SELECT batch_id
  FROM import_batch
  WHERE status = 'done'
  ORDER BY as_of_date DESC, batch_id DESC
  LIMIT 1
);

SET @latest_2cnv_count := (
  SELECT COUNT(*)
  FROM epin_snapshot
  WHERE batch_id = @latest_done_batch_id
    AND existe_en_2cnv = 1
);

SELECT
  @latest_done_batch_id AS latest_done_batch_id,
  ib.as_of_date,
  @latest_2cnv_count AS epins_confirmados_2cnv
FROM import_batch ib
WHERE ib.batch_id = @latest_done_batch_id;

-- Vista previa: EPIN marcados como vigentes que no están confirmados
-- por 2CNV en el último snapshot completado.
SELECT
  e.epin_id,
  e.epin,
  e.estado_epin,
  e.es_epin_actual,
  e.activo,
  e.last_seen_batch_id,
  e.last_seen_2cnv_batch_id
FROM epin e
LEFT JOIN epin_snapshot current_2cnv
  ON current_2cnv.batch_id = @latest_done_batch_id
 AND current_2cnv.epin = e.epin
 AND current_2cnv.existe_en_2cnv = 1
WHERE @latest_done_batch_id IS NOT NULL
  AND @latest_2cnv_count > 0
  AND current_2cnv.snapshot_id IS NULL
  AND (e.es_epin_actual = 1 OR e.activo = 1)
ORDER BY e.last_seen_2cnv_batch_id DESC, e.epin;

UPDATE epin e
LEFT JOIN epin_snapshot current_2cnv
  ON current_2cnv.batch_id = @latest_done_batch_id
 AND current_2cnv.epin = e.epin
 AND current_2cnv.existe_en_2cnv = 1
LEFT JOIN epin_snapshot current_bdo
  ON current_bdo.batch_id = @latest_done_batch_id
 AND current_bdo.epin = e.epin
 AND current_bdo.existe_en_bdo = 1
SET
  e.estado_epin = 'BAJA',
  e.es_epin_actual = 0,
  e.activo = 0,
  e.origen_ultimo_corte = CASE
    WHEN current_bdo.snapshot_id IS NOT NULL THEN 'BDO'
    ELSE 'DESCONOCIDO'
  END
WHERE @latest_done_batch_id IS NOT NULL
  AND @latest_2cnv_count > 0
  AND current_2cnv.snapshot_id IS NULL
  AND (
    e.estado_epin <> 'BAJA'
    OR e.es_epin_actual <> 0
    OR e.activo <> 0
    OR e.origen_ultimo_corte <> CASE
      WHEN current_bdo.snapshot_id IS NOT NULL THEN 'BDO'
      ELSE 'DESCONOCIDO'
    END
  );

SELECT ROW_COUNT() AS epins_corregidos;

COMMIT;

-- Verificación del caso reportado.
SELECT
  epin_id,
  epin,
  estado_epin,
  es_epin_actual,
  activo,
  origen_ultimo_corte,
  last_seen_batch_id,
  last_seen_bdo_batch_id,
  last_seen_2cnv_batch_id
FROM epin
WHERE epin = '40740109';
