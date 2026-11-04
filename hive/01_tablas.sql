-- =====================================================================================
-- 01 · Tablas de Hive del proyecto (se puede ejecutar varias veces sin problema)
--      ./pagos hivef 01_tablas.sql
--
-- Capas:
--   raw        /pagos/raw/card-payments      JSON tal cual lo archivó el consumidor
--   ref        /pagos/ref/comercios          catálogo de comercios (CSV)
--   curated    /pagos/curated/...            Parquet: pagos limpios y cuarentena
--
-- Todas son EXTERNAS (como en las clases 8 y 9): borrar la tabla no borra los ficheros.
-- =====================================================================================

CREATE DATABASE IF NOT EXISTS pagos;

-- -------------------------------------------------------------------------------------
-- RAW: lee directamente los ficheros JSON Lines que escribe el archivador.
-- Todo como STRING salvo los metadatos de Kafka: en raw no se interpreta nada, así un
-- importe mal escrito no se convierte en NULL en silencio al leerlo.
-- La partición dt (día de archivo) corresponde a las carpetas dt=AAAA-MM-DD.
-- -------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS pagos.pagos_raw (
  schema_version     INT,
  tx_id              STRING,
  event_ts           STRING,
  card_id            STRING,
  customer_id        STRING,
  merchant_id        STRING,
  merchant_category  STRING,
  amount_eur         STRING,
  channel            STRING,
  country            STRING,
  status             STRING,
  raw_value          STRING,     -- solo si el mensaje no era JSON válido
  kafka_topic        STRING,
  kafka_partition    INT,
  kafka_offset       BIGINT,
  ingest_ts          STRING,
  batch_id           STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/pagos/raw/card-payments'
TBLPROPERTIES ('external.table.purge'='false', 'discover.partitions'='false');

-- -------------------------------------------------------------------------------------
-- REF: catálogo de comercios (se sube con ./pagos subir-ref).
-- -------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS pagos.comercios (
  merchant_id  STRING,
  nombre       STRING,
  categoria    STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/pagos/ref/comercios'
TBLPROPERTIES ('external.table.purge'='false', 'skip.header.line.count'='1');

-- -------------------------------------------------------------------------------------
-- VISTA de limpieza: deduplica y valida. No guarda nada, es la receta que usan los
-- INSERT de 02_limpiar.sql.
--   1) Deduplicación: una sola fila por tx_id, la de menor (partición, offset). Si un
--      mensaje se archivó dos veces (relectura) o el origen lo envió dos veces
--      (duplicado), se queda la primera aparición.
--   2) Validación: regla = primera regla incumplida (R1..R5) o NULL si es válido.
--   3) Día y hora de negocio en hora de Madrid (event_ts está en UTC).
-- Un mensaje sin tx_id (no era JSON) se identifica por su partición y offset, para que no
-- se junte con otros sin tx_id al deduplicar.
-- -------------------------------------------------------------------------------------
-- El optimizador de Hive (CBO, basado en Calcite) falla con esta vista: AssertionError
-- Cannot add expression of different type to set. Se desactiva solo para estos scripts.
SET hive.cbo.enable=false;

CREATE OR REPLACE VIEW pagos.v_pagos_validados AS
SELECT
  u.tx_id,
  u.event_ts,
  CAST(from_utc_timestamp(u.ts_utc, 'Europe/Madrid') AS STRING)        AS ts_madrid,
  CAST(HOUR(from_utc_timestamp(u.ts_utc, 'Europe/Madrid')) AS INT)     AS hora_madrid,
  CAST(TO_DATE(from_utc_timestamp(u.ts_utc, 'Europe/Madrid')) AS STRING) AS fecha,
  u.card_id,
  u.customer_id,
  u.merchant_id,
  u.merchant_category,
  u.amount_eur,
  u.importe,
  u.channel,
  u.country,
  u.status,
  u.raw_value,
  u.kafka_partition,
  u.kafka_offset,
  u.ingest_ts,
  u.batch_id,
  CAST(CASE
    WHEN COALESCE(u.tx_id, '') = '' OR COALESCE(u.event_ts, '') = ''
      OR COALESCE(u.card_id, '') = '' OR COALESCE(u.customer_id, '') = ''
      OR COALESCE(u.merchant_id, '') = '' OR COALESCE(u.merchant_category, '') = ''
      OR COALESCE(u.amount_eur, '') = '' OR COALESCE(u.channel, '') = ''
      OR COALESCE(u.country, '') = '' OR COALESCE(u.status, '') = ''
      THEN 'R1_CAMPOS'
    WHEN u.importe IS NULL OR u.importe <= 0 OR u.importe > 10000
      THEN 'R2_IMPORTE'
    WHEN c.merchant_id IS NULL
      THEN 'R3_COMERCIO'
    WHEN u.ts_utc IS NULL OR u.ts_utc > u.ingest_utc
      THEN 'R4_FECHA'
    WHEN u.channel NOT IN ('FISICO', 'ONLINE')
      OR u.status NOT IN ('APROBADO', 'DENEGADO')
      OR u.merchant_category NOT IN ('ALIMENTACION', 'RESTAURACION', 'TRANSPORTE', 'OCIO',
                                     'ELECTRONICA', 'MODA', 'VIAJES', 'SALUD')
      OR NOT (u.country RLIKE '^[A-Z]{2}$')
      THEN 'R5_DOMINIOS'
    ELSE CAST(NULL AS STRING)
  END AS STRING) AS regla
FROM (
  SELECT
    n.*,
    CAST(n.amount_eur AS DECIMAL(10,2)) AS importe,
    CAST(regexp_replace(regexp_replace(n.event_ts,  'T', ' '), 'Z', '') AS TIMESTAMP) AS ts_utc,
    CAST(regexp_replace(regexp_replace(n.ingest_ts, 'T', ' '), 'Z', '') AS TIMESTAMP) AS ingest_utc
  FROM (
    SELECT
      r.*,
      ROW_NUMBER() OVER (
        PARTITION BY CAST(COALESCE(r.tx_id, CONCAT('sin_tx_', CAST(r.kafka_partition AS STRING),
                                                   '_', CAST(r.kafka_offset AS STRING))) AS STRING)
        ORDER BY r.kafka_partition, r.kafka_offset, r.ingest_ts
      ) AS aparicion
    FROM pagos.pagos_raw r
  ) n
  WHERE n.aparicion = 1
) u
LEFT JOIN pagos.comercios c ON u.merchant_id = c.merchant_id;

-- -------------------------------------------------------------------------------------
-- CURATED: pagos válidos, en Parquet y particionados por día de negocio (hora de Madrid).
-- El importe ya es DECIMAL(10,2): dinero exacto, sin errores de float.
-- -------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS pagos.pagos_limpios (
  tx_id              STRING,
  event_ts           STRING,          -- UTC, tal como llegó
  ts_madrid          STRING,          -- hora local de Madrid
  hora_madrid        INT,
  card_id            STRING,
  customer_id        STRING,
  merchant_id        STRING,
  merchant_category  STRING,
  importe            DECIMAL(10,2),
  channel            STRING,
  country            STRING,
  status             STRING,
  kafka_partition    INT,
  kafka_offset       BIGINT,
  ingest_ts          STRING
)
PARTITIONED BY (fecha STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/pagos/curated/pagos_limpios'
TBLPROPERTIES ('external.table.purge'='false', 'discover.partitions'='false',
               'parquet.compression'='SNAPPY');

-- -------------------------------------------------------------------------------------
-- CURATED: cuarentena. Los pagos rechazados con su motivo y su origen en Kafka.
-- -------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS pagos.pagos_cuarentena (
  tx_id              STRING,
  regla              STRING,
  event_ts           STRING,
  card_id            STRING,
  merchant_id        STRING,
  amount_eur         STRING,
  channel            STRING,
  country            STRING,
  status             STRING,
  raw_value          STRING,
  kafka_partition    INT,
  kafka_offset       BIGINT,
  ingest_ts          STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/pagos/curated/pagos_cuarentena'
TBLPROPERTIES ('external.table.purge'='false', 'parquet.compression'='SNAPPY');

SHOW TABLES IN pagos;
