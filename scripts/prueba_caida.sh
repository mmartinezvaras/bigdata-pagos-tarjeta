#!/usr/bin/env bash
# PRUEBA · Parada y recuperación
# Se mata el archivador en seco (SIGKILL) mientras el productor sigue enviando pagos.
# Se espera: el LAG crece mientras está muerto, al volver se pone al día (LAG = 0),
# no falta ningún mensaje y Hive da exactamente lo esperado.
source "$(dirname "$0")/pruebas_comun.sh"
empezar_registro caida
DATOS=data/caida
mkdir -p "$DATOS"

paso "1. Partir de cero y generar 3.000 pagos con su resultado esperado"
./pagos reset-todo
./pagos py simulator/simulador.py --seed 7 --n-events 3000 \
  --out "$DATOS/eventos.jsonl" --manifest "$DATOS/manifiesto.json"
./pagos py tests/calcular_esperado.py "$DATOS/eventos.jsonl" --out "$DATOS/esperado" > /dev/null

paso "2. Archivador en marcha y productor a 60 pagos/s (unos 50 s) en segundo plano"
./pagos archivador start
./pagos py producer/productor.py "$DATOS/eventos.jsonl" --rate 60 > "$CARPETA/productor.txt" 2>&1 &
PRODUCTOR=$!
sleep 15
nota "Con el archivador funcionando el LAG es pequeño: $(lag_total)"
./pagos lag

paso "3. MATAR el archivador en seco (SIGKILL) con el productor todavía enviando"
./pagos archivador kill
for i in 1 2 3; do
  sleep 8
  nota "LAG total: $(lag_total)  ← crece: llegan pagos y nadie los archiva"
done
./pagos lag

paso "4. Volver a arrancar el archivador"
./pagos archivador start
wait "$PRODUCTOR" || true
nota "El productor ha terminado:"
cat "$CARPETA/productor.txt"
esperar_lag_cero 180
./pagos lag

paso "5. ¿Se ha perdido algo?"
./pagos verificar
./pagos archivador stop
docker compose --profile pipeline logs --no-log-prefix archivador > "$CARPETA/archivador_logs.txt" 2>&1 || true
nota "Logs del archivador (antes y después de morir) en $CARPETA/archivador_logs.txt"

hive_y_comparar "$DATOS/esperado"

paso "RESULTADO DE LA PRUEBA DE CAÍDA: $RESULTADO"
nota "Registro completo en $CARPETA/registro.txt"
[ "$RESULTADO" = "OK" ]
