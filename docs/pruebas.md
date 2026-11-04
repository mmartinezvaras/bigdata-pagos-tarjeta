# Pruebas del sistema

Todas las pruebas se lanzan con un solo comando, parten de un sistema vacío
(`./pagos reset-todo`) y guardan su registro completo en `evidencias/pruebas/<prueba>/`.
Cada una termina comparando lo que da Hive con los resultados esperados, calculados de forma
independiente por `tests/calcular_esperado.py`.

| # | Prueba | Qué demuestra | Comando |
|---|---|---|---|
| T1 | Muestra de referencia | Hive da exactamente los resultados calculados a mano | (Fase 5) `./pagos consultas` + `comparar_con_esperado.py` |
| T2 | Dato incorrecto | Cada anomalía acaba en cuarentena con su regla y no entra en las métricas | incluida en T1 y T3 (`hive/cuarentena.tsv`) |
| T3 | Duplicado y relectura | Duplicados de origen y relecturas del consumidor no inflan las métricas | `./pagos prueba relectura` |
| T4 | Parada y recuperación | Si el archivador muere mientras llegan pagos, no se pierde ninguno | `./pagos prueba caida` |
| T5 | Volumen | El sistema procesa 100.000 pagos correctamente y cuánto tarda | `./pagos prueba volumen` |

## T1 · Muestra de referencia

27 mensajes con una anomalía de cada tipo, 3 duplicados y una tarjeta sospechosa
([tests/muestra](../tests/muestra/README.md)). Resultado: `HIVE DA EXACTAMENTE LO ESPERADO`
(`evidencias/fase5_comparacion.txt`).

## T2 · Dato incorrecto

Los 5 tipos de anomalía (R1-R5) aparecen en la muestra. `pagos_cuarentena` tiene exactamente
uno por regla, con su `tx_id` y su origen en Kafka (partición y offset), y ninguno aparece en
`pagos_limpios`: el control `validos` = 19 = 24 pagos distintos − 5 en cuarentena.

## T3 · Duplicado y relectura

1. Se publica la muestra y se arranca el archivador con lotes de 5 y la opción
   `--fallar-tras-escribir 1`: escribe el primer lote en HDFS y muere **sin hacer commit**.
2. Se vuelve a arrancar: Kafka le entrega otra vez desde el principio y escribe de nuevo ese
   lote.
3. `./pagos verificar` debe dar **0 faltan** y algún **repetido** (la relectura).
4. Hive debe dar exactamente lo esperado: la deduplicación por `tx_id` elimina tanto los 3
   duplicados de origen como las filas releídas. El control muestra `relecturas_consumidor`
   > 0 y el gasto sin deduplicar frente al deduplicado.

## T4 · Parada y recuperación

1. 3.000 pagos publicados a 60 por segundo (≈ 50 s), con el archivador funcionando.
2. A los 15 s se **mata** el archivador en seco (`docker compose kill`, SIGKILL). Durante
   ~25 s el LAG crece: llegan pagos y nadie los archiva.
3. Se vuelve a arrancar. Kafka le entrega desde su último commit y se pone al día (LAG = 0).
4. `./pagos verificar`: **0 faltan**. Hive da exactamente lo esperado.

## T5 · Volumen

100.000 pagos (102.000 mensajes con duplicados) publicados a máxima velocidad. Se mide el
tiempo de Kafka → HDFS y de Hive, se comprueba que no falta nada y que Hive da lo esperado.
También se compara el tamaño de raw (JSON) con el de limpios (Parquet + Snappy).

## Resultados

Ejecutadas el 05/11/2026 en un MacBook Air (Apple Silicon) con Docker Desktop.

| Prueba | Resultado | Datos clave |
|---|---|---|
| T1 · Muestra | ✅ OK | 27 mensajes → 24 pagos → 19 válidos + 5 cuarentena |
| T2 · Dato incorrecto | ✅ OK | 1 por regla R1-R5 |
| T3 · Relectura | ✅ OK | Murió tras escribir `p2[0-3]` + `p1[0]`: faltaban 22. Al volver: 0 faltan, 28 filas en raw (1 relectura en p1; el fichero de p2 se sustituyó por tener el mismo nombre). Hive = esperado |
| T4 · Caída | ✅ OK | 3.060 mensajes a 55/s. LAG con archivador vivo: 169 · muerto: 733 → 1.230 → 1.727 · al volver: 0 en 6 s. 0 faltan, 0 relecturas (murió entre lotes). Hive = esperado (P1 56 filas, P3 2 tarjetas, 18 por regla en cuarentena) |
| T5 · Volumen | ✅ OK | 102.000 mensajes. Productor: 0,7 s (≈140.000 msg/s). Kafka → HDFS: 139 s (≈730 msg/s), 0 faltan. Hive: 39 s. 97.000 válidos, 600 por regla, 80 tarjetas sospechosas = esperado. Raw JSON 42,1 MB → Parquet 4,1 MB |

## Lo que enseñan las cifras

- **El cuello de botella es el archivador** (≈730 msg/s), no Kafka (≈140.000 msg/s).
  Escribe en HDFS por WebHDFS, con varias peticiones HTTP por fichero. Kafka absorbe el pico
  al instante y el archivador lo vacía a su ritmo: para eso sirve tener una cola en medio.
- **Ficheros pequeños:** 102.000 mensajes en 234 ficheros de ≈180 KB, con bloques de 16 MB.
  Mejoras: lotes más grandes o un proceso de compactación periódico.
- **Parquet ocupa 10 veces menos** que el JSON de raw (columnar + Snappy + tipos binarios).
- **Replicación 2:** `hdfs dfs -du` muestra el doble de espacio real (84,2 MB) porque cada
  bloque está en los dos DataNodes.
