"""Prueba de humo de la fase 0.

Comprueba, desde el contenedor `app`, que el código Python llega a las dos piezas que va a
usar el proyecto:
  1. HDFS: escribe un fichero pequeño por WebHDFS, lo lee y lo borra.
  2. Kafka: publica un mensaje en un topic de prueba y lo vuelve a leer.

No toca el topic ni las carpetas reales del proyecto.
"""
import os
import sys
import time
import uuid

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from hdfs import InsecureClient

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
HDFS_URL = os.environ.get("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.environ.get("HDFS_USER", "root")


def check_hdfs() -> None:
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    ruta = f"/tmp/smoke_{uuid.uuid4().hex[:8]}.txt"
    client.write(ruta, data="hola hdfs\n", encoding="utf-8")
    with client.read(ruta, encoding="utf-8") as f:
        leido = f.read()
    client.delete(ruta)
    assert leido == "hola hdfs\n", f"HDFS devolvió {leido!r}"
    print(f"[OK] HDFS: escrito, leído y borrado {ruta}")


def check_kafka() -> None:
    topic = "smoke-test"
    admin = AdminClient({"bootstrap.servers": KAFKA})
    if topic not in admin.list_topics(timeout=10).topics:
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result()

    marca = uuid.uuid4().hex
    p = Producer({"bootstrap.servers": KAFKA})
    p.produce(topic, key="k", value=marca)
    p.flush(10)

    c = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id": f"smoke-{marca[:8]}",
        "auto.offset.reset": "earliest",
    })
    c.subscribe([topic])
    fin = time.time() + 20
    try:
        while time.time() < fin:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            if msg.value().decode() == marca:
                print(f"[OK] Kafka: mensaje publicado y leído (partición {msg.partition()}, offset {msg.offset()})")
                return
    finally:
        c.close()
    raise AssertionError("Kafka: no se recibió el mensaje en 20 s")


if __name__ == "__main__":
    try:
        check_hdfs()
        check_kafka()
    except Exception as e:  # noqa: BLE001 - queremos un mensaje claro para cualquier fallo
        print(f"[FALLO] {e}", file=sys.stderr)
        sys.exit(1)
    print("Entorno listo: Python ↔ HDFS ↔ Kafka funcionan.")
