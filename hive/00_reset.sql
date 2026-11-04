-- =====================================================================================
-- 00 · Sincronizar el catálogo de Hive después de vaciar HDFS (lo usa ./pagos reset-todo)
-- SYNC PARTITIONS quita del catálogo las particiones cuyas carpetas ya no existen.
-- =====================================================================================
MSCK REPAIR TABLE pagos.pagos_raw SYNC PARTITIONS;
MSCK REPAIR TABLE pagos.pagos_limpios SYNC PARTITIONS;
SHOW PARTITIONS pagos.pagos_raw;
SHOW PARTITIONS pagos.pagos_limpios;
