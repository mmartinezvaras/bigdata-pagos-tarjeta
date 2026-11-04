"""Productor: lee los pagos del simulador (JSON Lines) y los publica en Kafka.

Cada línea del fichero se publica tal cual como un mensaje del topic `card-payments`:
  * clave (key)  = card_id   -> todos los pagos de una tarjeta van a la MISMA partición,
                                así Kafka mantiene su orden (necesario para detectar fraude).
  * valor (value) = el JSON del pago, sin tocar.

Al terminar muestra cuántos mensajes han ido a cada partición y comprueba que ninguna tarjeta
se ha repartido entre dos particiones.

Ejemplos (desde la carpeta del proyecto):
    ./pagos py producer/productor.py tests/muestra/eventos.jsonl
    ./pagos py producer/productor.py data/eventos.jsonl --rate 50     # 50 pagos/segundo
    ./pagos py simulator/simulador.py --n-events 500 | ./pagos py producer/productor.py -
"""
import argparse
import json
import os
import sys
import time
from collections import Counter, defaultdict

from confluent_kafka import KafkaException, Producer

TOPIC = "card-payments"


def crear_productor(bootstrap: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap,
        # Un mensaje solo se da por entregado cuando el broker lo ha guardado
        # (con un solo broker, "all" equivale a "el broker lo tiene en disco").
        "acks": "all",
        # Si hay que reintentar un envío, Kafka descarta la copia repetida: el reintento
        # del PRODUCTOR no crea duplicados. (Los duplicados que mete el simulador sí llegan,
        # porque son mensajes distintos con el mismo tx_id: eso se resuelve más adelante.)
        "enable.idempotence": True,
        # Agrupa mensajes durante 20 ms antes de enviarlos: menos viajes de red.
        "linger.ms": 20,
        # Mismo algoritmo de reparto por clave que los clientes Java de Kafka (murmur2).
        "partitioner": "murmur2_random",
        "client.id": "productor-pagos",
    })


def main() -> int:
    p = argparse.ArgumentParser(description="Publica en Kafka los pagos de un fichero JSON Lines.")
    p.add_argument("fichero", help="fichero .jsonl del simulador ('-' = entrada estándar)")
    p.add_argument("--topic", default=TOPIC)
    p.add_argument("--rate", type=float, default=0,
                   help="pagos por segundo (0 = lo más rápido posible)")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    a = p.parse_args()

    productor = crear_productor(a.bootstrap)
    por_particion = Counter()
    particiones_de_tarjeta = defaultdict(set)
    sin_clave = 0
    fallos = []

    def al_entregar(err, msg):
        """Kafka llama a esta función cuando confirma (o rechaza) cada mensaje."""
        if err is not None:
            fallos.append(str(err))
            return
        por_particion[msg.partition()] += 1
        if msg.key() is not None:
            particiones_de_tarjeta[msg.key().decode()].add(msg.partition())

    entrada = sys.stdin if a.fichero == "-" else open(a.fichero, encoding="utf-8")
    pausa = 1.0 / a.rate if a.rate > 0 else 0
    inicio = time.time()
    enviados = 0
    try:
        for linea in entrada:
            linea = linea.strip()
            if not linea:
                continue
            card_id = json.loads(linea).get("card_id")      # puede ser None (anomalía R1)
            if card_id is None:
                sin_clave += 1
            while True:
                try:
                    productor.produce(a.topic, key=card_id, value=linea.encode("utf-8"),
                                      on_delivery=al_entregar)
                    break
                except BufferError:          # cola interna llena: esperar a que se vacíe
                    productor.poll(0.5)
            productor.poll(0)                # atiende las confirmaciones pendientes
            enviados += 1
            if enviados % 10000 == 0:
                print(f"[productor] {enviados} enviados...", file=sys.stderr)
            if pausa:
                time.sleep(pausa)
    except KeyboardInterrupt:
        print("\n[productor] interrumpido: enviando lo que queda en cola...", file=sys.stderr)
    finally:
        if entrada is not sys.stdin:
            entrada.close()
        pendientes = productor.flush(30)
    segundos = time.time() - inicio

    confirmados = sum(por_particion.values())
    print(f"[productor] {enviados} enviados, {confirmados} confirmados por Kafka, "
          f"{len(fallos)} fallidos, {pendientes} sin confirmar · {segundos:.1f} s "
          f"({confirmados / segundos:.0f} msg/s)")
    for part in sorted(por_particion):
        print(f"  partición {part}: {por_particion[part]} mensajes")
    repartidas = [t for t, ps in particiones_de_tarjeta.items() if len(ps) > 1]
    print(f"  {len(particiones_de_tarjeta)} tarjetas distintas · "
          f"repartidas entre varias particiones: {len(repartidas)} "
          f"({'OK, cada tarjeta en una sola partición' if not repartidas else repartidas[:5]})")
    if sin_clave:
        print(f"  {sin_clave} mensajes sin card_id (sin clave): Kafka los reparte al azar")
    if fallos:
        print(f"  primer error: {fallos[0]}", file=sys.stderr)
    return 0 if not fallos and not pendientes and confirmados == enviados else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KafkaException as e:
        print(f"[productor] error de Kafka: {e}", file=sys.stderr)
        sys.exit(1)
