#!/usr/bin/env bash
# PRUEBA · Volumen
# 100.000 pagos (o los que se indiquen: ./pagos prueba volumen 50000) por todo el sistema.
# Se mide cuánto tarda cada parte y se comprueba que no falta nada y que Hive da lo esperado.
source "$(dirname "$0")/pruebas_comun.sh"
N=${1:-100000}
empezar_registro volumen
DATOS=data/volumen
mkdir -p "$DATOS"

paso "1. Partir de cero y generar $N pagos con su resultado esperado"
./pagos reset-todo
./pagos py simulator/simulador.py --seed 2026 --n-events "$N" \
  --out "$DATOS/eventos.jsonl" --manifest "$DATOS/manifiesto.json"
./pagos py tests/calcular_esperado.py "$DATOS/eventos.jsonl" --out "$DATOS/esperado" > /dev/null

paso "2. Archivador en marcha y productor a máxima velocidad"
./pagos archivador start
sleep 5
INICIO=$(date +%s)
./pagos py producer/productor.py "$DATOS/eventos.jsonl"

paso "3. Esperar a que el archivador lo guarde todo en HDFS"
esperar_lag_cero 900
SEG_ARCHIVO=$(( $(date +%s) - INICIO ))
nota "Kafka → HDFS completo en ${SEG_ARCHIVO} s"
./pagos lag
./pagos verificar
nota "Raw en HDFS (carpetas, ficheros, tamaño):"
./pagos hdfs dfs -count -h /pagos/raw/card-payments
./pagos archivador stop

INICIO=$(date +%s)
hive_y_comparar "$DATOS/esperado"
SEG_HIVE=$(( $(date +%s) - INICIO ))
nota "Tamaño raw (JSON) frente a limpios (Parquet + Snappy):"
./pagos hdfs dfs -du -s -h /pagos/raw/card-payments /pagos/curated/pagos_limpios

paso "RESULTADO DE LA PRUEBA DE VOLUMEN ($N pagos): $RESULTADO"
nota "Kafka → HDFS: ${SEG_ARCHIVO} s · Hive (limpieza + consultas): ${SEG_HIVE} s"
nota "Registro completo en $CARPETA/registro.txt"
[ "$RESULTADO" = "OK" ]
