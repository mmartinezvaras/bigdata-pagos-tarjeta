-- Control: recuentos de cada capa, para cuadrar con los resultados esperados.
-- Join normal (el map join falla en este entorno local, ver 02_limpiar.sql).
SET hive.auto.convert.join=false;
SELECT 'filas_raw' AS metrica, CAST(COUNT(*) AS STRING) AS valor FROM pagos.pagos_raw
UNION ALL
SELECT 'relecturas_consumidor', CAST(COUNT(*) - COUNT(DISTINCT kafka_partition, kafka_offset) AS STRING)
FROM pagos.pagos_raw
UNION ALL
SELECT 'pagos_distintos', CAST(COUNT(DISTINCT tx_id) AS STRING) FROM pagos.pagos_raw
UNION ALL
SELECT 'validos', CAST(COUNT(*) AS STRING) FROM pagos.pagos_limpios
UNION ALL
SELECT CONCAT('cuarentena_', regla), CAST(COUNT(*) AS STRING) FROM pagos.pagos_cuarentena GROUP BY regla
UNION ALL
SELECT 'importe_aprobado_deduplicado', CAST(SUM(importe) AS STRING)
FROM pagos.pagos_limpios WHERE status = 'APROBADO'
UNION ALL
SELECT 'importe_aprobado_sin_deduplicar', CAST(SUM(CAST(r.amount_eur AS DECIMAL(10,2))) AS STRING)
FROM pagos.pagos_raw r
JOIN (SELECT DISTINCT tx_id FROM pagos.pagos_limpios WHERE status = 'APROBADO') l ON r.tx_id = l.tx_id;
