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
  gasto). P2 cuenta todos los intentos.
- **Cómo lo cuento:** *"Una pregunta por cada tipo de usuario: negocio, operaciones y fraude."*

## D3 · Las horas siempre en UTC

- **Qué:** `event_ts` se guarda en UTC. Solo se convierte a hora de Madrid en la consulta P2.
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

---

*(Se irán añadiendo: clave de Kafka, organización en HDFS, commit de offsets,
formato Parquet, particionado en Hive…)*
