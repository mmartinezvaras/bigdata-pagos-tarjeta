#!/usr/bin/env bash
# PRUEBA · Relectura
# El archivador muere DESPUÉS de escribir un lote en HDFS y ANTES de confirmar el offset.
# Se espera: al volver, Kafka le devuelve ese lote y lo escribe otra vez (repetidos en raw),
# no falta ningún mensaje, y Hive, deduplicando por tx_id, da exactamente lo esperado.
source "$(dirname "$0")/pruebas_comun.sh"
empezar_registro relectura

paso "1. Partir de cero y publicar la muestra de referencia (27 mensajes)"
./pagos reset-todo
./pagos py producer/productor.py tests/muestra/eventos.jsonl

paso "2. Archivador con lotes de 5 que MUERE tras escribir el primer lote, sin commit"
./pagos py consumer/archivador.py --lote 5 --espera 1 --fallar-tras-escribir 1 \
  || nota "el archivador ha muerto, como queríamos"
nota "Offsets confirmados en Kafka (ninguno: murió antes del commit):"
./pagos lag || true
nota "Lo que llegó a HDFS antes de morir:"
./pagos raw
./pagos verificar || nota "faltan mensajes: es lo esperado, el archivador no ha terminado"

paso "3. Vuelve el archivador: Kafka le entrega desde el último commit (el principio)"
./pagos py consumer/archivador.py --salir-si-inactivo 30
./pagos lag
./pagos raw
nota "No debe faltar nada; los 'repetidos' son la relectura del lote 1:"
./pagos verificar

hive_y_comparar tests/muestra/esperado

paso "RESULTADO DE LA PRUEBA DE RELECTURA: $RESULTADO"
nota "Registro completo en $CARPETA/registro.txt"
[ "$RESULTADO" = "OK" ]
