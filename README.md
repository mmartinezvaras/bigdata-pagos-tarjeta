# Pagos con tarjeta · Proyecto individual de Sistemas de Big Data

Sistema que simula los pagos con tarjeta de un banco y los lleva por todo el recorrido:

```
simulador → Kafka → consumidor de archivo → HDFS → Hive → consultas de negocio
```

## Estado

| Fase | Contenido | Estado |
|---|---|---|
| 1 | Caso, preguntas de negocio y contrato de datos | ✅ |
| 0 | Entorno Docker (Kafka, HDFS, Hive) | 🔧 pendiente de probar en el Mac |
| 2 | Simulador y muestra de referencia | ⏳ |
| 3 | Productor Kafka | ⏳ |
| 4 | Consumidor que archiva en HDFS | ⏳ |
| 5 | Tablas y consultas Hive | ⏳ |
| 6 | Pruebas y evidencias | ⏳ |
| 7 | README final, diagrama y presentación | ⏳ |

## Documentación

- [Contrato de datos](docs/contrato_datos.md): campos, tipos, unidades, reglas y anomalías.
- [Decisiones](docs/decisiones.md): qué se decidió y por qué.

## Estructura del repositorio

```
simulator/   generador de eventos (semilla y volumen configurables)
producer/    publicación en Kafka
consumer/    lectura de Kafka y archivo en HDFS
hive/        DDL de tablas y consultas de negocio
tests/       muestra de referencia y comprobaciones
evidencias/  salidas guardadas de las pruebas
scripts/     utilidades (prueba de humo del entorno)
docker/      imágenes y configuración de Hadoop, Hive y Python
docs/        contrato, decisiones, diagrama
```

## Arranque del entorno

Requisitos: Docker Desktop con al menos **6 GB de memoria** asignados (Settings → Resources).

Si tienes el laboratorio de clase arrancado, páralo antes (usa los mismos puertos):

    cd ../LABORATORIO_ALUMNOS_HDFS && docker compose stop && cd -

Desde la carpeta del proyecto:

    chmod +x pagos
    ./pagos up       # construye imágenes y arranca (la primera vez tarda varios minutos)
    ./pagos ready    # espera a HDFS, Kafka y Hive
    ./pagos init     # crea el topic card-payments (3 particiones) y /pagos en HDFS
    ./pagos check    # prueba de humo: Python escribe en HDFS y publica/lee en Kafka

Resultado esperado de `./pagos check`:

    [OK] HDFS: escrito, leído y borrado /tmp/smoke_xxxxxxxx.txt
    [OK] Kafka: mensaje publicado y leído (partición 0, offset N)
    Entorno listo: Python ↔ HDFS ↔ Kafka funcionan.

Interfaces web: NameNode en http://localhost:9870 · HiveServer2 en http://localhost:10002.

Para parar **sin perder datos**: `./pagos stop`. No uses `docker compose down -v`: borra los
volúmenes (HDFS, Kafka y el catálogo de Hive).

## Servicios

| Servicio | Imagen | Para qué |
|---|---|---|
| `namenode`, `dn1`, `dn2` | Hadoop 3.4.2 (imagen del laboratorio) | HDFS: almacenamiento de los eventos |
| `kafka` | `apache/kafka:3.9.1` (KRaft, 1 broker) | Transporte de los eventos |
| `hiveserver2` | `apache/hive:4.1.0` (imagen de la clase 08) | Tablas y consultas SQL |
| `app` | Python 3.12 + `confluent-kafka` + `hdfs` | Simulador, productor y consumidor |

La imagen de Hadoop y la configuración de Hive se reutilizan de los materiales de la asignatura
(laboratorio HDFS y clase 08), adaptando las rutas.
