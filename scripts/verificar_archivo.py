"""Comprueba que TODO lo que hay en Kafka está archivado en HDFS (no se ha perdido nada).

Para cada partición compara:
  * los offsets que existen en Kafka (desde el primero hasta el último), con
  * los offsets que aparecen en los ficheros de /pagos/raw/card-payments.

Resultado por partición: cuántos faltan (pérdidas: debe ser 0) y cuántos están repetidos
(relecturas del consumidor: pueden ser > 0 tras una caída, y no es un error porque se quitan
en Hive). También cuenta los tx_id repetidos (duplicados que vienen del origen).

    ./pagos py scripts/verificar_archivo.py
"""
import json
import os
import sys
from collections import Counter

from confluent_kafka import Consumer, TopicPartition
from hdfs import InsecureClient

TOPIC = "card-payments"
BASE_HDFS = "/pagos/raw/card-payments"


def main() -> int:
    hdfs = InsecureClient(os.environ.get("HDFS_URL", "http://namenode:9870"),
                          user=os.environ.get("HDFS_USER", "root"))
    kafka = Consumer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
                      "group.id": "verificador-archivo", "enable.auto.commit": False})

    vistos = Counter()          # (partición, offset) -> veces que aparece en HDFS
    tx = Counter()
    n_ficheros = n_filas = 0
    if hdfs.status(BASE_HDFS, strict=False) is not None:
        for carpeta, _subcarpetas, ficheros in hdfs.walk(BASE_HDFS):
            for nombre in sorted(ficheros):
                if nombre.startswith(("_", ".")):
                    continue                     # temporales: Hive tampoco los lee
                n_ficheros += 1
                with hdfs.read(f"{carpeta}/{nombre}", encoding="utf-8") as f:
                    for linea in f:
                        if linea.strip():
                            r = json.loads(linea)
                            vistos[(r["kafka_partition"], r["kafka_offset"])] += 1
                            tx[r.get("tx_id")] += 1
                            n_filas += 1

    particiones = sorted(kafka.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions)
    print(f"HDFS: {n_ficheros} ficheros, {n_filas} filas en {BASE_HDFS}\n")
    print(f"{'partición':>9} | {'en Kafka':>8} | {'archivados':>10} | {'faltan':>6} | {'repetidos':>9}")
    total_faltan = 0
    for part in particiones:
        bajo, alto = kafka.get_watermark_offsets(TopicPartition(TOPIC, part), timeout=10)
        esperados = range(bajo, alto)
        faltan = sum(1 for o in esperados if vistos[(part, o)] == 0)
        repetidos = sum(vistos[(part, o)] - 1 for o in esperados if vistos[(part, o)] > 1)
        archivados = sum(1 for o in esperados if vistos[(part, o)] > 0)
        total_faltan += faltan
        print(f"{part:>9} | {len(esperados):>8} | {archivados:>10} | {faltan:>6} | {repetidos:>9}")
    kafka.close()

    dup_tx = sum(n - 1 for t, n in tx.items() if n > 1)
    print(f"\nFilas con tx_id repetido (duplicados de origen + relecturas): {dup_tx}")
    print("RESULTADO: " + ("OK, no falta ningún mensaje de Kafka en HDFS" if total_faltan == 0
                           else f"FALTAN {total_faltan} mensajes"))
    return 0 if total_faltan == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
