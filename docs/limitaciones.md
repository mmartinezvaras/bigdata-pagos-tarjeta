# Garantías y limitaciones

## Cuándo se guarda cada dato y cuándo se confirma el progreso

| Paso | Cuándo se considera "guardado" | Qué se confirma y cuándo | Si falla en medio |
|---|---|---|---|
| Productor → Kafka | Cuando el broker confirma que lo ha escrito (`acks=all`) | Nada que confirmar: el productor solo cuenta los mensajes confirmados | El reintento del productor no duplica (idempotencia). Si Kafka no responde, el mensaje cuenta como fallido y el productor termina con error |
| Kafka | En el log de la partición, en disco (volumen `kafka_data`) | — | Los mensajes se conservan aunque nadie los lea (por eso crece el LAG) |
| Archivador → HDFS | Cuando el fichero del lote está escrito **y renombrado** a su nombre final (`part-<p>-<ini>-<fin>.jsonl`) | **Después** de eso, commit del offset siguiente al último guardado, por partición | Muere antes del commit → al volver relee el lote: **repetidos sí, pérdidas no** (at-least-once). Muere a mitad de escribir → queda un `_....tmp` que Hive ignora |
| HDFS raw → Hive limpios | Cuando termina `02_limpiar.sql` (`INSERT OVERWRITE`) | — | Se repite el script: recalcula todo desde raw, así que no deja nada a medias ni duplicado |

**Dónde se reconocen las repeticiones:** en Hive, al deduplicar por `tx_id` en la vista
`v_pagos_validados`. Ahí desaparecen tanto los duplicados del origen (el mismo pago enviado
dos veces, distinto offset) como las relecturas del archivador (mismo partición y offset en dos
ficheros). En raw se **aceptan** las repeticiones a propósito: raw es la copia fiel de lo que
llegó.

## Limitaciones

**Infraestructura**
1. **Un solo broker de Kafka, replicación 1.** Si se pierde su disco, se pierden los mensajes
   no archivados. En producción: 3 brokers y replicación 3.
2. **Hive en modo local** (MapReduce local, metastore Derby embebido, un usuario). Sirve para
   el proyecto; no es un clúster.
3. **Dos ajustes de Hive** para este entorno: optimizador CBO y *map join* desactivados en los
   scripts de limpieza (decisión D24). No cambian los resultados; en un clúster real se
   investigarían en lugar de desactivarlos.
4. **Sin seguridad:** ni autenticación ni cifrado. Todo es local.

**Archivador**
5. **No se reinicia solo.** Si HDFS cae, la escritura falla, el archivador termina **sin
   hacer commit** (no se pierde nada) y hay que volver a arrancarlo a mano.
6. **Rendimiento limitado por WebHDFS** (≈730 mensajes/s en la prueba de volumen), con un solo
   consumidor. Se podrían usar hasta 3 consumidores (uno por partición) en el mismo grupo.
7. **Ficheros pequeños:** 102.000 mensajes generan 234 ficheros de ≈180 KB, con bloques de
   16 MB. Faltaría compactación periódica o lotes más grandes.
8. **Relecturas en un reequilibrio:** si se retiran particiones al archivador con un lote sin
   confirmar, el nuevo dueño lo relee. Sigue siendo at-least-once; Hive lo deduplica.

**Procesamiento**
9. **Hive no es tiempo real.** Los datos aparecen en las consultas cuando se ejecuta
   `02_limpiar.sql`, que además recalcula **todo** raw cada vez: con mucho histórico, sería
   lento. Faltaría procesar solo las particiones nuevas.
10. **La deduplicación depende de que `tx_id` sea único** y lo genere el origen. Dos pagos
    distintos con el mismo `tx_id` se contarían como uno.
11. **Particiones antiguas de limpios:** `INSERT OVERWRITE` dinámico solo reescribe las fechas
    que aparecen en la salida. Si desapareciera todo un día de raw, su partición seguiría en
    limpios hasta un `./pagos reset-todo`.
12. **Cambiar el número de particiones** del topic cambia a qué partición va cada tarjeta, y se
    pierde el orden por tarjeta durante el cambio.

**Datos simulados**
13. **Las tarjetas normales siempre pagan en su país**, también online. Por eso P3 no tiene
    falsos positivos; con datos reales (viajes, compras en webs extranjeras) habría que
    afinar la regla.
14. **Catálogo de comercios fijo** y sin país; las categorías del pago no se cruzan con las
    del catálogo.
15. **R4 (fecha futura)** se compara con el momento de archivo. Un terminal con el reloj
    adelantado unos minutos no se detectaría.

## Mejoras posibles

- Detección de fraude **en tiempo real**: un segundo consumidor que avise en segundos cuando
  una tarjeta paga en dos países, en lugar de esperar a la consulta P3.
- Limpieza **incremental** por partición `dt` y compactación de raw.
- Reinicio automático del archivador (`restart: on-failure`) y alertas por LAG alto.
- Un pequeño panel con las tres métricas.
