#!/usr/bin/env bash
# DEMO de la defensa (≈ 4 minutos), con pausas para explicar cada paso.
#
# Antes del turno (tarda ~1 min):   ./pagos up && ./pagos ready && ./pagos reset-todo
# En el turno:                      ./pagos demo
#
# Recorrido: muestra → Kafka → archivador → HDFS → parada con mensajes pendientes →
# recuperación → Hive deduplica y valida → P1-P3 = resultados esperados.
# La muestra se publica DOS veces: la segunda tanda son 27 duplicados (mismos tx_id), así se
# ve a la vez la recuperación tras la parada y que los duplicados no cambian las métricas.
source "$(dirname "$0")/pruebas_comun.sh"
empezar_registro demo

pausa() {
  if [ -t 0 ]; then read -r -p $'\n   [Enter para seguir] ' _; fi
}

paso "1. Publicar la muestra de referencia: 27 pagos con clave card_id"
./pagos py producer/productor.py tests/muestra/eventos.jsonl
nota "Mensajes por partición en Kafka (partición:offset final):"
./pagos kafka kafka-get-offsets --topic card-payments
pausa

paso "2. El archivador lee del grupo archivador-hdfs, escribe en HDFS y DESPUÉS confirma"
./pagos archivador start
esperar_lag_cero 90
./pagos lag
nota "Ficheros en HDFS: partición y rango de offsets en el nombre"
./pagos raw
pausa

paso "3. PARADA: mato el archivador (SIGKILL) y llegan 27 mensajes más (la muestra reenviada)"
./pagos archivador kill
./pagos py producer/productor.py tests/muestra/eventos.jsonl
nota "Mensajes pendientes de archivar (LAG): $(lag_total)"
./pagos lag
pausa

paso "4. RECUPERACIÓN: el archivador vuelve y sigue desde su último commit"
./pagos archivador start
esperar_lag_cero 90
./pagos lag
./pagos verificar
./pagos archivador stop
pausa

hive_y_comparar tests/muestra/esperado

paso "RESULTADO DE LA DEMO: $RESULTADO"
nota "54 mensajes en raw → 24 pagos distintos (30 repetidos por tx_id) → 19 válidos + 5 en cuarentena"
nota "Registro completo en $CARPETA/registro.txt"
[ "$RESULTADO" = "OK" ]
