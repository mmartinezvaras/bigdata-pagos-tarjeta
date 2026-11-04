-- =====================================================================================
-- 02 · Registrar lo nuevo de raw y reconstruir las tablas limpias
--      ./pagos hivef 02_limpiar.sql
-- Se puede repetir siempre que lleguen datos nuevos: recalcula todo desde raw.
-- =====================================================================================

-- 1) Registrar en el catálogo las carpetas dt=... que haya creado el archivador.
MSCK REPAIR TABLE pagos.pagos_raw;
SHOW PARTITIONS pagos.pagos_raw;

-- Mismo motivo que en 01_tablas.sql: el optimizador CBO falla con la vista.
SET hive.cbo.enable=false;
-- Join normal en vez de map join: el map join lanza un proceso Java aparte
-- (MapredLocalTask) que falla en este entorno local.
SET hive.auto.convert.join=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 2) Pagos válidos → Parquet, una partición por día (Madrid).
--    El orden del SELECT importa: se asigna por posición, y la partición va al final.
INSERT OVERWRITE TABLE pagos.pagos_limpios PARTITION (fecha)
SELECT tx_id, event_ts, ts_madrid, hora_madrid, card_id, customer_id, merchant_id,
       merchant_category, importe, channel, country, status, kafka_partition,
       kafka_offset, ingest_ts,
       fecha
FROM pagos.v_pagos_validados
WHERE regla IS NULL;

-- 3) Pagos rechazados → cuarentena, con el motivo.
INSERT OVERWRITE TABLE pagos.pagos_cuarentena
SELECT tx_id, regla, event_ts, card_id, merchant_id, amount_eur, channel, country, status,
       raw_value, kafka_partition, kafka_offset, ingest_ts
FROM pagos.v_pagos_validados
WHERE regla IS NOT NULL;

SHOW PARTITIONS pagos.pagos_limpios;
