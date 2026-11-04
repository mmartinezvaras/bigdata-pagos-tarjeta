"""Consumidor archivador: lee los pagos de Kafka y los guarda en HDFS sin perder ninguno.

Funcionamiento, en un bucle:
  1. Lee mensajes de las particiones que le tocan (grupo de consumidores `archivador-hdfs`).
  2. Los acumula en un LOTE hasta tener --lote mensajes o hasta que pasan --espera segundos.
  3. Escribe el lote en HDFS: un fichero por partición, con el rango de offsets en el nombre:
         /pagos/raw/card-payments/dt=AAAA-MM-DD/part-<partición>-<offset_ini>-<offset_fin>.jsonl
     A cada pago le añade de dónde viene: kafka_topic, kafka_partition, kafka_offset,
     ingest_ts (cuándo se archivó) y batch_id.
  4. SOLO DESPUÉS de que HDFS confirme la escritura, confirma los offsets en Kafka (commit).

Por el orden de 3 y 4, si el proceso muere entre medias, al volver Kafka le devuelve los
mismos mensajes y los escribe otra vez: puede haber filas repetidas en HDFS, pero NUNCA se
pierde un pago ("al menos una vez", at-least-once). Las repeticiones se quitan en Hive por
tx_id.

No valida los datos: archiva todo tal cual llega (zona "raw"). Validar es trabajo de Hive.

Normalmente se ejecuta como servicio de Docker (./pagos archivador start). A mano:
    ./pagos py consumer/archivador.py --salir-si-inactivo 15
Para la prueba de caída:
    ./pagos py consumer/archivador.py --fallar-tras-escribir 1   # muere sin hacer commit
"""
import argparse
import json
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from hdfs import InsecureClient

TOPIC = "card-payments"
GRUPO = "archivador-hdfs"
BASE_HDFS = "/pagos/raw/card-payments"


def log(texto: str) -> None:
    print(f"[archivador {datetime.now().strftime('%H:%M:%S')}] {texto}", flush=True)


def registro(msg, ingest_ts: str, batch_id: str) -> str:
    """El pago tal cual llegó, más los metadatos de origen (trazabilidad)."""
    try:
        evento = json.loads(msg.value())
        if not isinstance(evento, dict):
            raise ValueError("no es un objeto JSON")
    except (ValueError, TypeError):
        # Un mensaje que ni siquiera es JSON también se archiva: no se tira nada.
        evento = {"raw_value": (msg.value() or b"").decode("utf-8", "replace")}
    evento.update({
        "kafka_topic": msg.topic(),
        "kafka_partition": msg.partition(),
        "kafka_offset": msg.offset(),
        "ingest_ts": ingest_ts,
        "batch_id": batch_id,
    })
    return json.dumps(evento, ensure_ascii=False)


def escribir_atomico(hdfs: InsecureClient, carpeta: str, nombre: str, texto: str) -> str:
    """Escribe primero en un fichero temporal oculto y luego lo renombra.

    Hive ignora los ficheros que empiezan por "_", así que nunca verá un fichero a medias.
    El renombrado en HDFS es atómico: el fichero final aparece entero o no aparece.
    """
    hdfs.makedirs(carpeta)
    temporal = f"{carpeta}/_{nombre}.tmp"
    final = f"{carpeta}/{nombre}"
    hdfs.write(temporal, data=texto, encoding="utf-8", overwrite=True)
    if hdfs.status(final, strict=False) is not None:
        # Mismo nombre = misma partición y mismo rango de offsets = mismo contenido
        # (pasa si se relee exactamente el mismo lote). Se sustituye.
        hdfs.delete(final)
    hdfs.rename(temporal, final)
    return final


def main() -> int:
    p = argparse.ArgumentParser(description="Archiva en HDFS los pagos de Kafka.")
    p.add_argument("--topic", default=TOPIC)
    p.add_argument("--grupo", default=GRUPO, help="grupo de consumidores de Kafka")
    p.add_argument("--destino", default=BASE_HDFS, help="carpeta raw en HDFS")
    p.add_argument("--lote", type=int, default=500, help="máximo de mensajes por lote")
    p.add_argument("--espera", type=float, default=5.0,
                   help="segundos máximos que un lote espera a llenarse")
    p.add_argument("--salir-si-inactivo", type=float, default=0,
                   help="terminar tras N segundos sin mensajes nuevos (0 = no terminar nunca)")
    p.add_argument("--fallar-tras-escribir", type=int, default=0,
                   help="PRUEBA: morir justo después de escribir el lote N en HDFS, sin commit")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--hdfs-url", default=os.environ.get("HDFS_URL", "http://namenode:9870"))
    p.add_argument("--hdfs-user", default=os.environ.get("HDFS_USER", "root"))
    a = p.parse_args()

    hdfs = InsecureClient(a.hdfs_url, user=a.hdfs_user)
    consumidor = Consumer({
        "bootstrap.servers": a.bootstrap,
        "group.id": a.grupo,
        # Los offsets los confirmo YO, y solo después de escribir en HDFS.
        "enable.auto.commit": False,
        # Si el grupo no tiene offsets guardados (primera vez), empezar desde el principio.
        "auto.offset.reset": "earliest",
        "client.id": "archivador-hdfs",
    })

    def al_asignar(_c, particiones):
        log("particiones asignadas: " + ", ".join(str(tp.partition) for tp in particiones))

    def al_revocar(_c, particiones):
        log("particiones retiradas: " + ", ".join(str(tp.partition) for tp in particiones))

    consumidor.subscribe([a.topic], on_assign=al_asignar, on_revoke=al_revocar)

    parar = False

    def pedir_parada(signum, _frame):
        nonlocal parar
        log(f"señal {signal.Signals(signum).name}: termino el lote actual y salgo")
        parar = True

    signal.signal(signal.SIGTERM, pedir_parada)   # docker compose stop
    signal.signal(signal.SIGINT, pedir_parada)    # Ctrl-C

    lote, inicio_lote, n_lote = [], None, 0
    total_escritos, ultimo_mensaje = 0, time.time()

    def escribir_y_confirmar():
        nonlocal lote, inicio_lote, n_lote, total_escritos
        n_lote += 1
        ahora = datetime.now(timezone.utc)
        ingest_ts = ahora.strftime("%Y-%m-%dT%H:%M:%SZ")
        batch_id = f"{ahora.strftime('%Y%m%dT%H%M%S')}-{n_lote:05d}"
        carpeta = f"{a.destino}/dt={ahora.date().isoformat()}"

        por_particion = defaultdict(list)
        for m in lote:
            por_particion[m.partition()].append(m)

        # 1) Escribir en HDFS (un fichero por partición).
        resumen = []
        for part, mensajes in sorted(por_particion.items()):
            ini, fin = mensajes[0].offset(), mensajes[-1].offset()
            texto = "\n".join(registro(m, ingest_ts, batch_id) for m in mensajes) + "\n"
            escribir_atomico(hdfs, carpeta, f"part-{part}-{ini:010d}-{fin:010d}.jsonl", texto)
            resumen.append(f"p{part}[{ini}-{fin}]")

        if a.fallar_tras_escribir and n_lote == a.fallar_tras_escribir:
            log(f"PRUEBA: lote {n_lote} escrito en HDFS ({' '.join(resumen)}) y muero SIN commit")
            os._exit(1)

        # 2) Solo ahora, confirmar en Kafka: "he guardado hasta aquí". El offset que se
        #    confirma es el SIGUIENTE a leer (último + 1).
        consumidor.commit(offsets=[TopicPartition(a.topic, part, mensajes[-1].offset() + 1)
                                   for part, mensajes in por_particion.items()],
                          asynchronous=False)
        total_escritos += len(lote)
        log(f"lote {n_lote}: {len(lote)} pagos → {carpeta} {' '.join(resumen)} · commit OK "
            f"(total {total_escritos})")
        lote, inicio_lote = [], None

    log(f"leyendo '{a.topic}' como grupo '{a.grupo}' → HDFS {a.destino} "
        f"(lote {a.lote} mensajes / {a.espera:g} s)")
    try:
        while not parar:
            mensajes = consumidor.consume(num_messages=a.lote - len(lote), timeout=1.0)
            for m in mensajes:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        log(f"error de Kafka: {m.error()}")
                    continue
                lote.append(m)
            if mensajes:
                ultimo_mensaje = time.time()
            if lote and inicio_lote is None:
                inicio_lote = time.time()

            if lote and (len(lote) >= a.lote or time.time() - inicio_lote >= a.espera):
                escribir_y_confirmar()

            if (a.salir_si_inactivo and not lote
                    and time.time() - ultimo_mensaje >= a.salir_si_inactivo):
                log(f"{a.salir_si_inactivo:g} s sin mensajes nuevos: termino")
                break
        if lote:                       # parada ordenada: no dejar nada a medias
            escribir_y_confirmar()
    finally:
        consumidor.close()
    log(f"fin: {total_escritos} pagos archivados en {n_lote} lotes")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KafkaException as e:
        log(f"error de Kafka: {e}")
        sys.exit(1)
