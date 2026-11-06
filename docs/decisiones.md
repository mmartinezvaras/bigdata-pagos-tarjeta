# Decisiones del proyecto (chuleta para la defensa)

Cada decisión sigue el mismo formato: **qué decidí**, **por qué**, y **cómo lo cuento** en la
defensa en una o dos frases. Las alternativas descartadas también van, porque es lo que suelen
preguntar.

---

## D1 · El caso: pagos con tarjeta de un banco

- **Qué:** simulo los pagos con tarjeta que autoriza un banco en distintos comercios.
- **Por qué:** cada pago tiene muchas dimensiones (tarjeta, cliente, comercio, categoría,
  país, canal, hora), así que salen preguntas de negocio variadas. Además, las pruebas
  obligatorias tienen sentido real: un pago duplicado infla el gasto y un importe negativo es
  un error claro.
- **Descartado:** cajeros (pocas preguntas posibles) y transferencias (simular saldos
  coherentes añade complejidad que no aporta).
- **Cómo lo cuento:** *"Soy el equipo de datos de un banco. Recibo los pagos con tarjeta en
  tiempo real y los guardo para que negocio y fraude puedan consultarlos."*

## D2 · Las tres preguntas de negocio

| # | Pregunta | Quién la usa |
|---|---|---|
| P1 | ¿Cuánto se gasta cada día por categoría de comercio? (nº pagos aprobados, importe total en EUR y ticket medio) | Marketing / negocio |
| P2 | ¿En qué franjas horarias se paga más, y qué parte es online? (hora local de Madrid) | Operaciones y producto digital |
| P3 | ¿Qué tarjetas han pagado en **dos o más países distintos el mismo día**? | Equipo de fraude |

- **Por qué estas tres:** P1 es una agregación simple, P2 obliga a tratar bien las zonas
  horarias y P3 es una consulta más interesante (agrupar por tarjeta y día y contar países
  distintos). Juntas cubren los tres usuarios del sistema.
- **Detalle de unidades:** solo cuentan los pagos `APROBADO` en P1 (un pago denegado no es
  gasto). P2 y P3 cuentan todos los intentos válidos: para fraude, un intento denegado en
  otro país ya es una señal.
- **Cómo lo cuento:** *"Una pregunta por cada tipo de usuario: negocio, operaciones y fraude."*

## D3 · Las horas siempre en UTC

- **Qué:** `event_ts` se guarda en UTC. Para el negocio (el "día" de P1 y P3 y la "hora" de
  P2) se convierte a hora de Madrid en las consultas.
- **Por qué:** UTC no tiene cambios de horario (verano/invierno) ni ambigüedades. Si guardara
  hora local, el día del cambio de hora habría horas repetidas o inexistentes.
- **Cómo lo cuento:** *"Guardo en UTC y convierto al presentar, como se hace en cualquier
  sistema serio."*

## D4 · `tx_id` como identificador único

- **Qué:** cada pago lleva un `tx_id` único generado por el simulador.
- **Por qué:** es lo que permite reconocer duplicados. Sin un identificador del negocio, dos
  pagos idénticos de 10 € en el mismo comercio serían indistinguibles de un duplicado.
- **Cómo lo cuento:** *"La deduplicación necesita un identificador del evento, no del mensaje."*

## D5 · Datos incorrectos a cuarentena, no se borran

- **Qué:** los eventos que incumplen una regla van a una tabla `pagos_cuarentena` con el
  motivo; no se tiran.
- **Por qué:** así se puede demostrar qué se ha rechazado y por qué, y se pueden corregir y
  reprocesar. Borrar sin dejar rastro hace imposible auditar.
- **Cómo lo cuento:** *"Ningún dato desaparece en silencio: o está en métricas o está en
  cuarentena con su motivo."*

## D6 · Todo el entorno en un único `compose.yaml`

- **Qué:** un solo fichero levanta HDFS (NameNode + 2 DataNodes), Kafka, Hive y un contenedor
  Python. Reutilizo la imagen de Hadoop del laboratorio y la configuración de Hive de la
  clase 08, cambiando las rutas a `/pagos`.
- **Por qué:** el enunciado pide que todo se reproduzca con Docker. Con un solo fichero, quien
  lo pruebe ejecuta `./pagos up` y tiene el sistema entero. Reutilizar lo de clase reduce
  riesgos: esas piezas ya funcionan en mi Mac.
- **2 DataNodes en vez de 3:** con replicación 2 sigue habiendo copia de cada bloque, y ahorro
  1 GB de RAM, que necesito para Kafka y Hive.
- **Cómo lo cuento:** *"Un comando arranca todo; las piezas de HDFS y Hive son las de clase."*

## D7 · Un broker de Kafka en modo KRaft

- **Qué:** un solo broker que hace también de controlador (sin ZooKeeper).
- **Por qué:** el enunciado dice que un broker local basta. KRaft es el modo actual de Kafka y
  evita levantar ZooKeeper. Con un broker el factor de replicación es 1: si el broker cae,
  los mensajes no se pierden (están en disco, en el volumen), pero no hay alta
  disponibilidad. Esto va a la lista de limitaciones.
- **Creación automática de topics desactivada:** el topic `card-payments` se crea a mano con
  3 particiones (`./pagos init`). Así un error de escritura en el nombre del topic no crea
  uno nuevo en silencio.
- **Cómo lo cuento:** *"Un broker basta para el proyecto; en producción tendría 3 y
  replicación 3."*

## D8 · El código Python corre dentro de Docker, no en el Mac

- **Qué:** un contenedor `app` con Python y las librerías ejecuta el simulador, el productor y
  el consumidor. El código está montado desde la carpeta del proyecto.
- **Por qué:** dentro de la red de Docker los nombres `kafka`, `namenode` o `dn1` se resuelven
  solos. Desde el Mac, HDFS redirige las escrituras a `dn1:9864`, un nombre que el Mac no
  conoce, y fallaría. Además, así nadie tiene que instalar Python ni librerías para probarlo.
- **Librerías:** `confluent-kafka` (cliente oficial de Confluent, basado en librdkafka) y
  `hdfs` (escribe en HDFS por WebHDFS, la API HTTP del NameNode).
- **Cómo lo cuento:** *"El código vive en la misma red que los servicios; el Mac solo edita
  ficheros."*

## D9 · El simulador escribe un fichero; no publica en Kafka

- **Qué:** `simulator/simulador.py` genera los pagos en JSON Lines (un JSON por línea). El
  productor, aparte, lee ese fichero y lo publica.
- **Por qué:** así los mismos eventos sirven como muestra de referencia y como entrada del
  sistema, y puedo calcular el resultado esperado sin depender de Kafka. Además, cada pieza
  hace una sola cosa y se prueba por separado.
- **Cómo lo cuento:** *"Separé generar datos de transportarlos: el mismo fichero lo uso para
  saber qué debe salir y para alimentar Kafka."*

## D10 · Verdad conocida: semilla, manifiesto y oráculo

- **Qué:** el simulador es reproducible (`--seed`) y guarda un manifiesto con todo lo que
  inyectó. `tests/calcular_esperado.py` calcula P1-P3 en Python puro, aplicando el contrato
  sin Kafka, HDFS ni Hive. `tests/test_simulador.py` comprueba que ese cálculo encuentra
  exactamente lo inyectado.
- **Por qué:** para decir que Hive "está bien" necesito saber de antemano qué debe dar. Si
  Hive y el cálculo independiente coinciden con los mismos eventos, el sistema funciona.
- **Muestra de referencia:** `tests/muestra/` tiene 27 mensajes con una anomalía de cada
  tipo, 3 duplicados y una tarjeta sospechosa, con los resultados comprobables a mano.
- **Cómo lo cuento:** *"Sé lo que tiene que salir antes de ejecutar nada, y lo comparo."*

## D11 · El dinero en DECIMAL, nunca en float

- **Qué:** los importes se tratan como decimales exactos (`Decimal` en Python,
  `DECIMAL(10,2)` en Hive) y se redondean "hacia arriba en el 5".
- **Por qué:** en float, 35,95 / 2 = 17,975 puede guardarse como 17,97499… y redondear a
  17,97. En decimal da 17,98, que es lo correcto. En la muestra pasa justo esto.
- **Cómo lo cuento:** *"Con dinero no se usan floats: un céntimo mal en millones de pagos es
  mucho dinero."*

## D12 · La clave del mensaje es `card_id`

- **Qué:** cada pago se publica con `card_id` como clave. Kafka calcula un hash de la clave y
  así decide la partición, de modo que la misma tarjeta va **siempre a la misma partición**.
- **Por qué:** Kafka solo garantiza el orden **dentro de una partición**. Para fraude
  necesito ver los pagos de una tarjeta en el orden en que ocurrieron ("pagó en Madrid y 40
  minutos después en Londres"). Con `card_id` como clave, ese orden está garantizado.
- **Descartado:** `tx_id` (cada pago a una partición distinta, se pierde el orden por
  tarjeta); `merchant_id` (un supermercado grande concentraría muchos mensajes en una
  partición); sin clave (reparto al azar, sin orden por tarjeta).
- **Pagos sin `card_id`** (anomalía R1): van sin clave y Kafka los reparte al azar. No pasa
  nada, porque acabarán en cuarentena.
- **Cómo lo cuento:** *"La clave decide la partición, y la partición el orden. Lo que
  necesito ordenado son los pagos de cada tarjeta."*

## D13 · 3 particiones

- **Qué:** el topic `card-payments` tiene 3 particiones.
- **Por qué:** las particiones son la unidad de paralelismo: con 3, puedo tener hasta 3
  consumidores del mismo grupo leyendo a la vez, uno por partición. Para el volumen del
  proyecto sobra, y permite enseñar el reparto. Con pocas tarjetas (la muestra tiene 5) el
  reparto sale desigual; con muchas se equilibra.
- **Ojo:** si se aumentan las particiones después, cambia la partición de algunas tarjetas
  (el hash se reparte entre más particiones) y se pierde el orden durante el cambio.
- **Cómo lo cuento:** *"3 particiones = hasta 3 consumidores en paralelo."*

## D14 · Entrega segura: `acks=all` e idempotencia

- **Qué:** el productor espera la confirmación del broker (`acks=all`) y tiene activada la
  idempotencia.
- **Por qué:** con `acks=all` no se da un pago por enviado hasta que Kafka lo ha guardado.
  Con idempotencia, si un envío falla y se reintenta, Kafka reconoce la copia y no la guarda
  dos veces.
- **Pero siguen llegando duplicados:** los que mete el simulador son mensajes distintos con
  el mismo `tx_id` (como un terminal que reenvía), y Kafka no puede saber que son el mismo
  pago. Por eso la deduplicación de verdad se hace por `tx_id`, más adelante.
- **Cómo lo cuento:** *"Kafka evita los duplicados que crea él mismo; los que vienen del
  origen los quito yo por `tx_id`."*

## D15 · Commit del offset DESPUÉS de escribir en HDFS (at-least-once)

- **Qué:** el archivador desactiva el commit automático. Para cada lote: 1) escribe en HDFS,
  2) solo si HDFS confirma, hace commit en Kafka del offset siguiente al último guardado.
- **Por qué:** el commit es decirle a Kafka "ya he guardado hasta aquí". Si se dice antes de
  guardar y el proceso muere, esos mensajes se pierden para siempre. Haciéndolo después, si
  muere entre la escritura y el commit, al volver relee ese lote y lo escribe otra vez:
  puede haber repetidos, pero **nunca pérdidas**.
- **Las repeticiones:** se reconocen porque tienen la misma `(kafka_partition,
  kafka_offset)`, y se quitan en Hive al deduplicar por `tx_id`.
- **Descartado:** commit automático (Kafka confirma cada 5 s por su cuenta, aunque no se
  haya escrito nada: puede perder datos); exactly-once (necesitaría transacciones que
  abarquen Kafka y HDFS, y HDFS no participa en ellas).
- **Cómo lo cuento:** *"Primero guardo, luego confirmo. Prefiero un repetido, que sé quitar,
  a un pago perdido, que no sé recuperar."*

## D16 · Lotes por tamaño o por tiempo

- **Qué:** el archivador acumula hasta 500 mensajes o 5 segundos, lo que llegue antes, y
  escribe el lote de una vez.
- **Por qué:** HDFS está pensado para pocos ficheros grandes, no para miles de ficheros de
  una línea (cada fichero ocupa memoria en el NameNode). El límite de tiempo evita que, con
  poco tráfico, un pago se quede esperando mucho antes de archivarse.
- **Cómo lo cuento:** *"Lotes para no llenar HDFS de ficheros diminutos, con un máximo de
  5 segundos de retraso."*

## D17 · Organización en HDFS y nombres de fichero con offsets

- **Qué:** `/pagos/raw/card-payments/dt=AAAA-MM-DD/part-<partición>-<offset_ini>-<offset_fin>.jsonl`.
  `dt` es el día (UTC) en que se archivó. Cada fila lleva además `kafka_partition`,
  `kafka_offset`, `ingest_ts` y `batch_id`.
- **Por qué:** con solo mirar el nombre sé de qué partición y de qué rango de offsets viene
  cada fichero, y con las columnas puedo seguir cualquier pago hasta su mensaje en Kafka. La
  carpeta `dt=` es el formato que Hive entiende como partición.
- **Raw = tal cual llega:** aquí no se valida ni se corrige nada. Si mañana cambian las
  reglas, puedo reprocesar desde raw.
- **Cómo lo cuento:** *"Raw es la copia fiel de lo que llegó, con su origen apuntado."*

## D18 · Escritura atómica: fichero temporal + renombrado

- **Qué:** cada fichero se escribe primero como `_part-....jsonl.tmp` y después se renombra.
- **Por qué:** Hive ignora los ficheros que empiezan por `_`, y el renombrado en HDFS es
  atómico. Así nadie puede leer un fichero a medio escribir si el proceso muere a mitad.
- **Cómo lo cuento:** *"Un fichero en raw está entero o no está."*

## D19 · El archivador es un servicio de Docker aparte

- **Qué:** `./pagos archivador start|stop|kill|logs`. No arranca solo con `./pagos up`.
- **Por qué:** para las pruebas tengo que poder pararlo de forma ordenada (`stop` → termina
  el lote y sale) o matarlo en seco (`kill` → muere sin avisar) y volver a arrancarlo.
- **Cómo lo cuento:** *"Lo paro y lo mato a propósito para demostrar que no se pierde nada."*

## D20 · Tres capas en HDFS: raw, ref y curated

- **Qué:** `raw` (JSON tal cual llegó), `ref` (catálogo de comercios) y `curated` (Parquet
  limpio y cuarentena). Todas las tablas de Hive son **externas**, como en las clases 8 y 9.
- **Por qué:** raw es la fuente de verdad: si cambian las reglas, curated se reconstruye
  desde raw con `02_limpiar.sql`. Al ser tablas externas, borrar una tabla no borra los
  ficheros.
- **En raw todo es STRING** (menos los metadatos de Kafka): si el importe viene mal escrito,
  no se convierte en NULL al leerlo. La conversión a `DECIMAL` se hace al limpiar, donde un
  fallo va a cuarentena con su motivo.
- **Cómo lo cuento:** *"Raw guarda lo que llegó; curated, lo que vale. Curated siempre se
  puede rehacer desde raw."*

## D21 · Deduplicar y validar en una vista

- **Qué:** la vista `pagos.v_pagos_validados` contiene toda la lógica: 1) `ROW_NUMBER()`
  por `tx_id`, quedándose con la de menor `(kafka_partition, kafka_offset)`; 2) un `CASE`
  que devuelve la primera regla incumplida o NULL; 3) el paso de UTC a hora de Madrid.
  `02_limpiar.sql` vuelca los válidos a `pagos_limpios` y el resto a `pagos_cuarentena`.
- **Por qué:** la lógica está en un solo sitio, se puede consultar directamente para ver
  qué está pasando y las dos tablas salen de la misma regla, así que no puede pasar que un
  pago esté en las dos o en ninguna.
- **Primero deduplicar, después validar:** un pago incorrecto que llega dos veces aparece
  una sola vez en cuarentena.
- **Cómo lo cuento:** *"Una sola receta: cada pago acaba o en limpios o en cuarentena, nunca
  en los dos."*

## D22 · `pagos_limpios` en Parquet particionado por día

- **Qué:** Parquet con compresión Snappy, particionado por `fecha` (día de negocio en
  Madrid). La cuarentena también va en Parquet, sin particionar (es pequeña).
- **Por qué:** Parquet guarda por columnas: para "suma de importes por categoría" Hive solo
  lee esas columnas, no la fila entera. Particionar por fecha hace que una consulta de un
  día lea solo su carpeta (lo que vimos con `EXPLAIN` en la clase 9).
- **Partición por día de Madrid, no por día de archivo:** raw está partido por el día en que
  llegó (`dt`); curated, por el día en que se pagó, que es lo que pregunta negocio.
- **Cómo lo cuento:** *"Raw por día de llegada; curated por día de negocio, en columnas."*

## D23 · Comprobación automática contra lo esperado

- **Qué:** `./pagos consultas` guarda la salida de Hive en `evidencias/hive/` y
  `scripts/comparar_con_esperado.py` la compara, celda a celda y como decimales exactos, con
  los resultados del oráculo.
- **Por qué:** no basta con que las consultas "salgan": hay que demostrar que dan lo
  correcto. Así es repetible: cualquiera puede ejecutarlo y ver el mismo `OK`.
- **Cómo lo cuento:** *"No me fío de mirar la tabla a ojo: lo compara un script."*

## D24 · Tres ajustes de Hive en este entorno (problemas encontrados y resueltos)

Problemas reales que aparecieron al ejecutar la Fase 5 y cómo se resolvieron:

| Síntoma | Causa | Solución |
|---|---|---|
| Solo se creaban 2 de las 5 tablas, sin error claro | Un comentario `--` con `;` dentro: beeline corta las instrucciones por cada `;`, aunque esté en un comentario | Ningún `;` (ni comillas) dentro de comentarios en los `.sql` |
| `AssertionError: Cannot add expression of different type to set` al crear la vista | Fallo del optimizador CBO de Hive (Calcite) al deducir los tipos de las expresiones del `CASE` y del `COALESCE` | Tipos explícitos con `CAST(... AS STRING)` y `SET hive.cbo.enable=false` en los scripts de limpieza |
| `return code 1 from MapredLocalTask` en la limpieza | Hive convierte el `JOIN` con comercios en un *map join*, que lanza un proceso Java aparte que falla en este Docker local | `SET hive.auto.convert.join=false`: join normal |

- **Impacto:** ninguno en los resultados (lo demuestra la comparación con lo esperado). En
  rendimiento, con este volumen no se nota. En un clúster real se investigaría cada uno en
  vez de desactivarlo.
- **Para encontrar las causas:** `./pagos hivef` repite en modo detallado la orden que falla
  y busca en el log interno de Hive, porque beeline solo muestra un mensaje genérico.
- **Cómo lo cuento:** *"El mensaje de beeline no dice nada: la causa real está en el log de
  HiveServer2. Tres problemas, tres causas distintas, y la comparación con lo esperado
  demuestra que las soluciones no cambian los resultados."*

## D25 · Pruebas automatizadas y repetibles

- **Qué:** cada prueba es un script (`./pagos prueba relectura | caida | volumen`) que parte
  de cero (`./pagos reset-todo`), hace todos los pasos explicándolos, guarda el registro en
  `evidencias/pruebas/` y termina comparando Hive con lo esperado.
- **Por qué:** una prueba hecha a mano una vez no se puede repetir igual ni enseñar. Así
  cualquiera puede repetirla, y la evidencia es el registro completo, no una captura.
- **Tiempo de sesión de 10 s:** si el archivador muere sin despedirse, Kafka espera
  `session.timeout.ms` antes de darlo por muerto y dejar que otro recoja sus particiones. Lo
  bajé de 45 s a 10 s para que la recuperación sea rápida.
- **Ficheros a medias:** si muere mientras escribe, puede quedar un `_part-....tmp`. Hive y
  la verificación los ignoran (empiezan por `_`), así que no afectan a los resultados.
- **Cómo lo cuento:** *"Las pruebas son scripts: las repito cuando quiera y el resultado lo
  da una comparación, no mi opinión."*

## D26 · Un panel visual que es una ventana, no un atajo

- **Qué:** `./pagos panel` levanta una web (Streamlit, `localhost:8501`) con tres pestañas:
  *En directo* (mensajes y LAG por partición, gráfica del LAG, ficheros en HDFS, estado del
  archivador), *Respuestas de Hive* (P1-P3 en gráficos, cuarentena y si coinciden con lo
  esperado) y *Cómo funciona*. Botones para publicar, matar/arrancar el archivador, lanzar
  Hive y vaciar el sistema.
- **Por qué así:** el panel **no calcula nada por su cuenta**. Publica con el mismo
  `productor.py`, consulta Hive con los mismos `.sql` (ejecutados con beeline dentro del
  contenedor de Hive) y compara con el mismo oráculo y el mismo comparador. Si el panel
  sumara los importes él mismo, se saltaría el recorrido Kafka → HDFS → Hive que se evalúa.
- **Un observador para todos:** un hilo hace una foto del sistema cada 2 s y las pantallas
  solo la dibujan. Las acciones largas van en otro hilo, de una en una.
- **Socket de Docker:** para matar y arrancar el archivador, el panel monta
  `/var/run/docker.sock`. Quien controla ese socket controla Docker entero, así que solo es
  aceptable en local; por eso el panel tiene su propio perfil y no arranca con `./pagos up`.
- **Qué compara:** lo publicado **desde el panel** después de vaciar. Si se publica por otro
  lado, el panel lo avisa en lugar de dar un OK falso.
- **Aspecto** (`panel/estilo.py`), con los criterios de la skill `emil-design-eng` de Emil
  Kowalski: lo que se refresca cada 2 s no se anima (sin parpadeos); cifras con
  `tabular-nums` y tarjetas de tamaño fijo, para que nada se mueva al llegar datos; botones con
  `scale(0.97)` al pulsar (ease-out, < 160 ms) y objetivos de 44 px; el color nunca va solo,
  cada estado lleva su palabra. Misma tipografía y paleta que la presentación.
- **Cómo lo cuento:** *"El panel enseña el sistema; no lo sustituye. Las cifras que ves son
  las que responde Hive."*

---

Las garantías y limitaciones del sistema están en [limitaciones.md](limitaciones.md).
