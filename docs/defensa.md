# Guía de defensa (12 minutos)

| Bloque | Tiempo | Qué hago |
|---|---|---|
| Contexto y arquitectura | 2 min | Presentación: caso, 3 preguntas, diagrama |
| Demostración | 4 min | `./pagos demo` |
| Preguntas y cambio en directo | 4 min | Respondo con las decisiones (D1-D25) y hago el cambio que pidan |
| Transición | 2 min | Dejo el entorno para el siguiente |

## Antes del turno (checklist)

- [ ] Docker Desktop abierto. `cd` a la carpeta del proyecto.
- [ ] `./pagos up && ./pagos ready` → los tres servicios listos.
- [ ] `./pagos reset-todo` → sistema vacío para la demo (≈ 1 min).
- [ ] Terminal con letra grande (⌘ +) y ventana a pantalla completa.
- [ ] Abiertos: la presentación, `docs/decisiones.md`, `docs/pruebas.md` y la carpeta `evidencias/`.
- [ ] DBeaver conectado a Hive (por si piden una consulta nueva).
- [ ] Vídeo de respaldo de la demo a mano (ver al final).

---

## 1. Contexto y arquitectura (2 min) · guion

> "Soy el equipo de datos de un banco. Cada pago con tarjeta es un evento: tarjeta, comercio,
> importe, país, canal y si se aprueba. Mi sistema responde tres preguntas, una por usuario:
> **negocio** quiere el gasto diario por categoría, **operaciones** quiere saber a qué horas
> se paga y cuánto es online, y **fraude** quiere las tarjetas que pagan en dos países el
> mismo día."
>
> "Los datos los genero yo con un simulador **con semilla**: misma semilla, mismos datos. Meto
> a propósito anomalías, duplicados y tarjetas sospechosas, y apunto lo que meto. Así sé de
> antemano qué tiene que salir."
>
> (diagrama) "El productor publica en Kafka con **la tarjeta como clave**, para que los pagos
> de cada tarjeta vayan a la misma partición y mantengan su orden. El archivador lee por
> lotes, **primero escribe en HDFS y después confirma el offset**: puede repetir, pero nunca
> pierde. Cada fila lleva su partición y offset. En Hive, una vista deduplica por `tx_id` y
> valida; los buenos van a Parquet particionado por día y los malos a cuarentena con su
> motivo. Al final un script compara Hive con un cálculo independiente en Python."

## 2. Demostración (4 min) · qué decir en cada pausa

`./pagos demo`. Avanza con Enter.

1. **Publicar la muestra.** "27 mensajes: 24 pagos y 3 duplicados. Mirad el reparto: cada
   tarjeta va siempre a la misma partición. El único que cambia entre ejecuciones es el pago
   sin `card_id`: sin clave, Kafka lo pone al azar."
2. **Archivador.** "`CURRENT-OFFSET` es lo que el grupo ha confirmado y `LOG-END-OFFSET` lo
   que hay en Kafka. LAG 0: está al día. El commit apunta al **siguiente** mensaje a leer. En
   los nombres de los ficheros se ve la partición y el rango de offsets."
3. **Parada.** "Lo mato en seco y reenvío la muestra. Kafka guarda los 27 mensajes aunque nadie
   los lea: el LAG es 27, son los pendientes."
4. **Recuperación.** "Al volver, Kafka le da los mensajes desde su último commit. LAG 0 y la
   verificación dice que no falta ninguno."
5. **Hive.** "Raw tiene 54 filas, pero son 24 pagos distintos: los 27 reenvíos son
   duplicados por `tx_id` y no cuentan. 19 válidos y 5 en cuarentena, uno por regla. Y el
   comparador dice que P1, P2 y P3 son exactamente los esperados."

Si hay tiempo, enseñar `tests/muestra/README.md`: el ticket de 17,975 € → 17,98 € y la
tarjeta que pagó en Reino Unido y en España con 41 minutos de diferencia.

---

## 3. Preguntas probables

| Pregunta | Respuesta corta | Ver |
|---|---|---|
| ¿Por qué `card_id` como clave? | Kafka solo ordena dentro de una partición. La clave decide la partición; necesito ordenados los pagos de cada tarjeta para el fraude. `tx_id` repartiría cada pago; `merchant_id` concentraría los comercios grandes | D12 |
| ¿Qué pasa si pongo más particiones? | Más paralelismo (hasta 1 consumidor por partición), pero cambia `hash(clave) mod N`: algunas tarjetas pasan a otra partición y se pierde su orden durante el cambio | D13 |
| ¿Qué es el offset? ¿Y el LAG? | Offset: posición de un mensaje en su partición. Commit: el offset que el grupo da por procesado. LAG = final del log − offset confirmado = mensajes pendientes | D15 |
| ¿Cuándo confirmas en Kafka? | Después de que el fichero del lote está escrito y renombrado en HDFS. Si muere entre medias, relee: repetidos sí, pérdidas no | D15, limitaciones |
| ¿Por qué no exactly-once? | Haría falta una transacción entre Kafka y HDFS, y HDFS no participa en las de Kafka. Prefiero at-least-once y deduplicar por `tx_id`, que es sencillo y demostrable | D15 |
| ¿Dónde reconoces los duplicados? | Los reintentos del productor los evita Kafka (idempotencia). Los del origen y las relecturas, en Hive, con `ROW_NUMBER()` por `tx_id`. En raw se aceptan a propósito | D14, D21 |
| ¿Y si el mismo `tx_id` llega con datos distintos? | Me quedo con la primera aparición (menor partición/offset). Es una limitación: el origen debe garantizar `tx_id` únicos | limitaciones 10 |
| ¿Por qué Parquet? | Columnar: para sumar importes por categoría solo lee esas columnas. Comprime: 42 MB de JSON → 4 MB. Tiene tipos (DECIMAL) | D22 |
| ¿Por qué particionas por fecha? | Las preguntas son por día: una consulta de un día solo lee su carpeta. Raw va por día de **llegada** (`dt`) y limpios por día de **pago** (`fecha`) | D17, D22 |
| ¿Por qué UTC? | Sin cambios de hora ni ambigüedades. Convierto a Madrid solo para el día y la hora de negocio | D3 |
| ¿Por qué DECIMAL? | Con float, 35,95/2 = 17,975 puede quedar en 17,97499… y redondear mal. Con dinero, exacto | D11 |
| ¿Cómo sabes que el resultado es correcto? | Muestra pequeña con resultados calculados a mano y un oráculo en Python, independiente de Kafka y Hive. Un script compara celda a celda | D10, D23 |
| ¿Qué pasa si cae HDFS? | La escritura falla, el archivador termina sin commit: no se pierde nada, pero hay que arrancarlo a mano | limitaciones 5 |
| ¿Qué pasa si cae Kafka? | Los mensajes están en disco (volumen). El productor reintenta y, si no puede, los cuenta como fallidos. Con 1 broker no hay alta disponibilidad | D7, D14 |
| ¿Y con dos archivadores? | El grupo reparte las particiones entre los dos y cada uno confirma las suyas. Un reequilibrio puede provocar relecturas, que se deduplican | limitaciones 8 |
| ¿Por qué una vista y cuarentena? | Una sola receta: cada pago acaba en limpios o en cuarentena, nunca en los dos ni en ninguno. Ningún dato desaparece en silencio | D5, D21 |
| ¿Qué te costó más? | Hive: un `;` en un comentario, el optimizador CBO y el map join. El mensaje de beeline no decía nada; la causa estaba en el log de HiveServer2 | D24 |
| ¿Qué harías en producción? | 3 brokers y replicación 3, varios archivadores, limpieza incremental, compactación, reinicio automático, alertas por LAG y fraude en tiempo real | limitaciones |

---

## 4. Cambios en directo (practicados)

Después de cualquier cambio de datos o SQL, el ciclo es:
`./pagos hivef 02_limpiar.sql` → `./pagos consultas` → `./pagos py scripts/comparar_con_esperado.py`.

### C1 · Una consulta nueva (la más probable)

*"Los 5 comercios con más gasto aprobado."* Crear `hive/consultas/p4_top_comercios.sql`:

```sql
SET hive.auto.convert.join=false;
SELECT l.merchant_id, c.nombre, COUNT(*) AS n_pagos, SUM(l.importe) AS importe_total_eur
FROM pagos.pagos_limpios l
JOIN pagos.comercios c ON l.merchant_id = c.merchant_id
WHERE l.status = 'APROBADO'
GROUP BY l.merchant_id, c.nombre
ORDER BY importe_total_eur DESC
LIMIT 5;
```

Ejecutar: `./pagos hivef consultas/p4_top_comercios.sql` (o pegarla en DBeaver). Variantes
rápidas: cambiar el `WHERE` (solo ONLINE), el `GROUP BY` (por país) o el `LIMIT`.

### C2 · Cambiar una pregunta existente

- P3 con 3 o más países: en `p3_tarjetas_sospechosas.sql`, `HAVING COUNT(DISTINCT country) >= 3`.
- P1 incluyendo denegados: quitar `WHERE status = 'APROBADO'`.
- P2 por día y hora: añadir `fecha` al `SELECT`, al `GROUP BY` y al `ORDER BY`.

Lanzar `./pagos consultas`. El comparador fallará porque el oráculo calcula la pregunta
original. **Explicarlo:** "es justo lo que tiene que hacer: si cambio la definición, cambio
también el cálculo esperado (`tests/calcular_esperado.py`)".

### C3 · Cambiar el tamaño del lote del archivador

```bash
./pagos reset-todo
./pagos py producer/productor.py tests/muestra/eventos.jsonl
./pagos py consumer/archivador.py --lote 5 --espera 1 --salir-si-inactivo 15
./pagos raw          # ahora hay muchos ficheros pequeños, de 5 mensajes como mucho
```

Comentar: lotes pequeños = menos retraso pero más ficheros pequeños en HDFS (D16).

### C4 · Cambiar el número de particiones

En `pagos`, cambiar `PARTITIONS=3` por `PARTITIONS=4`, y luego:

```bash
./pagos reset-todo
./pagos py producer/productor.py tests/muestra/eventos.jsonl   # reparto entre 4 particiones
./pagos kafka kafka-topics --describe --topic card-payments
```

Comentar: cada tarjeta sigue en una sola partición, pero no necesariamente en la misma que
antes (D13). Volver a 3 al terminar.

### C5 · Otra semilla o más volumen

```bash
./pagos py simulator/simulador.py --seed 99 --n-events 500 --out data/otra.jsonl
./pagos py tests/calcular_esperado.py data/otra.jsonl
```

Misma semilla → mismos datos; otra semilla → otros datos con la misma estructura (D10).

### C6 · Una regla nueva (más larga)

*"Rechazar los pagos de más de 2.000 €."* En `hive/01_tablas.sql`, en el `CASE` de la vista,
cambiar `u.importe > 10000` por `u.importe > 2000`. Luego `./pagos hivef 01_tablas.sql` y
`./pagos hivef 02_limpiar.sql`. Ver el cambio en `SELECT regla, COUNT(*) FROM
pagos.pagos_cuarentena GROUP BY regla;`. Para que el comparador vuelva a dar OK, aplicar el
mismo cambio en `regla_incumplida()` de `tests/calcular_esperado.py` y regenerar
`tests/muestra/esperado`. Deshacer al terminar.

---

## Plan B

- **Algo no arranca:** `./pagos ready` dice qué servicio falla. `docker compose logs --tail=50 <servicio>`.
- **Hive tarda o falla en directo:** enseñar `evidencias/fase5_comparacion.txt` y
  `evidencias/pruebas/demo/registro.txt` (resultado de una ejecución anterior).
- **Vídeo de respaldo:** grabar una ejecución completa de `./pagos demo` con la grabación de
  pantalla de macOS (⌘ ⇧ 5) y guardarla fuera del repositorio (no subir vídeos grandes).
