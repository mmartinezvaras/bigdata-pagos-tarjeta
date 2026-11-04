#!/usr/bin/env bash
# Funciones comunes de las pruebas de la fase 6. Las pruebas lo cargan con "source".
set -euo pipefail
cd "$(dirname "$0")/.."

# Todo lo que salga por pantalla se guarda también en evidencias/pruebas/<prueba>/registro.txt
empezar_registro() {
  CARPETA="evidencias/pruebas/$1"
  mkdir -p "$CARPETA"
  exec > >(tee "$CARPETA/registro.txt") 2>&1
  echo "Prueba '$1' · $(date '+%Y-%m-%d %H:%M:%S')"
}

paso() {
  echo
  echo "======================================================================"
  echo "== $*"
  echo "======================================================================"
}

nota() { echo ">> $*"; }

# Suma del LAG del archivador en todas las particiones ("?" si aún no hay offsets confirmados).
lag_total() {
  ./pagos lag 2>/dev/null | awk '
    $1 == "archivador-hdfs" { n++; if ($4 == "-" || $6 == "-") p = 1; else s += $6 }
    END { if (n == 0 || p) print "?"; else print s + 0 }'
}

# Espera hasta que el archivador haya archivado todo lo que hay en Kafka (LAG = 0).
esperar_lag_cero() {
  local max=${1:-180} inicio=$SECONDS l="?"
  while [ $((SECONDS - inicio)) -lt "$max" ]; do
    l=$(lag_total)
    if [ "$l" = "0" ]; then
      nota "LAG = 0 a los $((SECONDS - inicio)) s de espera: el archivador ha llegado al final del topic"
      return 0
    fi
    sleep 3
  done
  nota "AVISO: el LAG no llegó a 0 en ${max} s (último valor: ${l})"
  return 1
}

# Limpia en Hive, lanza P1-P3 y compara con los resultados esperados de $1.
hive_y_comparar() {
  paso "Hive: registrar raw, limpiar y lanzar P1-P3"
  ./pagos hivef 02_limpiar.sql
  ./pagos consultas "$CARPETA/hive"
  paso "Comparar Hive con lo esperado"
  if ./pagos py scripts/comparar_con_esperado.py --esperado "$1" --obtenido "$CARPETA/hive"; then
    RESULTADO="OK"
  else
    RESULTADO="FALLO"
  fi
}
