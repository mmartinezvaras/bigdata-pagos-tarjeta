"""Lo que el panel sabe hacer con el sistema: observar Kafka, HDFS y Docker, y lanzar acciones.

El panel NO calcula las respuestas por su cuenta: reutiliza las mismas piezas que la terminal
(producer/productor.py, los .sql de hive/ con beeline, tests/calcular_esperado.py y
scripts/comparar_con_esperado.py). Solo las enseña de forma visual.

Dos ideas:
  * Un hilo "observador" hace una foto del sistema cada 2 s (offsets, LAG, ficheros...). La
    pantalla solo dibuja la última foto, así varias pestañas abiertas no multiplican consultas.
  * Las acciones largas (publicar, Hive, vaciar) corren en un hilo aparte, de una en una, y
    van contando por qué paso van.
"""
import os
import re
import subprocess
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path

import docker
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from hdfs import InsecureClient

PROYECTO = Path(os.environ.get("PROYECTO_DIR", "/proyecto"))
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
HDFS_URL = os.environ.get("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.environ.get("HDFS_USER", "root")
COMPOSE = os.environ.get("COMPOSE_PROJECT", "pagos")

TOPIC = "card-payments"
PARTICIONES = 3
GRUPO = "archivador-hdfs"
RAW = "/pagos/raw/card-payments"
CURATED = ["/pagos/curated/pagos_limpios", "/pagos/curated/pagos_cuarentena"]

DATOS = PROYECTO / "data" / "panel"
PUBLICADO = DATOS / "publicado.jsonl"          # todo lo publicado desde el último vaciado
HIVE_OUT = PROYECTO / "evidencias" / "panel" / "hive"
ESPERADO = DATOS / "esperado"
CONSULTAS = ["p1_gasto_dia_categoria", "p2_franja_horaria", "p3_tarjetas_sospechosas",
             "control", "cuarentena"]

RUIDO = re.compile(r"SLF4J|log4j|NativeCodeLoader|HADOOP_CONF_DIR|package scanning|"
                   r"packages. attribute|logging.apache.org|main INFO|main WARN")
FICHERO_RAW = re.compile(r"part-(\d+)-(\d+)-(\d+)\.jsonl$")


# ------------------------------------------------------------------------------------------
# Piezas sueltas
# ------------------------------------------------------------------------------------------
def sin_ruido(texto: str) -> str:
    return "\n".join(l for l in texto.splitlines() if l.strip() and not RUIDO.search(l))


def contenedor(servicio: str):
    """El contenedor de un servicio de compose (aunque esté parado), o None."""
    lista = docker.from_env().containers.list(all=True, filters={"label": [
        f"com.docker.compose.project={COMPOSE}", f"com.docker.compose.service={servicio}"]})
    return lista[0] if lista else None


def beeline(fichero: str) -> str:
    """Ejecuta hive/<fichero> con beeline dentro del contenedor de Hive, como ./pagos hivef."""
    c = contenedor("hiveserver2")
    if c is None or c.status != "running":
        raise RuntimeError("Hive no está en marcha (./pagos up)")
    r = c.exec_run(["beeline", "-u", "jdbc:hive2://localhost:10000/default", "-n", "root",
                    "--silent=true", "--showHeader=true", "--outputformat=tsv2",
                    "-f", f"/opt/sql/{fichero}"], demux=True)
    salida, errores = r.output
    salida = sin_ruido((salida or b"").decode("utf-8", "replace"))
    if r.exit_code != 0:
        detalle = sin_ruido((errores or b"").decode("utf-8", "replace"))
        lineas = [l for l in detalle.splitlines() if "Error" in l or "FAILED" in l]
        raise RuntimeError(f"Hive falló en {fichero}: " + (lineas[-1] if lineas else detalle[-300:]))
    return salida


def python(*args: str, timeout: int = 600) -> subprocess.CompletedProcess:
    """Ejecuta uno de los scripts del proyecto, igual que ./pagos py."""
    return subprocess.run(["python", *args], cwd=PROYECTO, capture_output=True, text=True,
                          timeout=timeout)


# ------------------------------------------------------------------------------------------
# El gestor: observador + acciones
# ------------------------------------------------------------------------------------------
class Gestor:
    def __init__(self) -> None:
        self.admin = AdminClient({"bootstrap.servers": KAFKA})
        self.consumidor = Consumer({"bootstrap.servers": KAFKA, "group.id": "panel-observador",
                                    "enable.auto.commit": False})
        self.hdfs = InsecureClient(HDFS_URL, user=HDFS_USER)
        self.cerrojo = threading.Lock()
        self.foto: dict = {}
        self.historial: deque = deque(maxlen=180)        # 6 minutos a una foto cada 2 s
        self.tarea: dict | None = None                    # la acción en curso
        self.ultimo: dict | None = None                   # el resultado de la última acción
        DATOS.mkdir(parents=True, exist_ok=True)
        threading.Thread(target=self._observar, daemon=True).start()

    # ---------------------------------------------------------------- observador
    def _observar(self) -> None:
        while True:
            try:
                foto = self._hacer_foto()
            except Exception as e:                      # nunca debe morir el observador
                foto = {"ts": time.time(), "error": str(e)}
            with self.cerrojo:
                self.foto = foto
                if foto.get("lag_total") is not None:
                    self.historial.append((datetime.now(), foto["lag_total"],
                                           sum(p["fin"] for p in foto["particiones"])))
            time.sleep(2)

    def _hacer_foto(self) -> dict:
        foto: dict = {"ts": time.time(), "avisos": []}

        # Kafka: mensajes por partición (offset final) y offsets confirmados por el grupo
        try:
            meta = self.admin.list_topics(TOPIC, timeout=5).topics.get(TOPIC)
            ids = sorted(meta.partitions) if meta and not meta.error else []
            confirmados = {}
            try:
                fut = self.admin.list_consumer_group_offsets(
                    [ConsumerGroupTopicPartitions(GRUPO, [TopicPartition(TOPIC, p) for p in ids])])
                for tp in fut[GRUPO].result(timeout=5).topic_partitions:
                    confirmados[tp.partition] = tp.offset if tp.offset >= 0 else None
            except Exception:
                pass                                    # el grupo aún no existe
            particiones = []
            for p in ids:
                _, fin = self.consumidor.get_watermark_offsets(TopicPartition(TOPIC, p),
                                                               timeout=5, cached=False)
                conf = confirmados.get(p)
                particiones.append({"p": p, "fin": fin, "confirmado": conf,
                                    "lag": None if conf is None else max(fin - conf, 0)})
            foto["kafka_ok"] = True
            foto["particiones"] = particiones
            lags = [x["lag"] for x in particiones]
            total = sum(x["fin"] for x in particiones)
            if particiones and all(l is not None for l in lags):
                foto["lag_total"] = sum(lags)
            elif total == 0:
                foto["lag_total"] = 0                   # nada en Kafka: nada pendiente
            else:
                foto["lag_total"] = None                # hay mensajes y aún no hay commits
        except Exception as e:
            foto.update(kafka_ok=False, particiones=[], lag_total=None)
            foto["avisos"].append(f"Kafka: {e}")

        try:
            desc = self.admin.describe_consumer_groups([GRUPO])[GRUPO].result(timeout=5)
            foto["grupo_estado"] = str(desc.state).split(".")[-1]
            foto["grupo_miembros"] = len(desc.members)
        except Exception:
            foto["grupo_estado"], foto["grupo_miembros"] = "—", 0

        # HDFS: ficheros en raw (los temporales "_..." no cuentan, como en Hive)
        try:
            ficheros = []
            if self.hdfs.status(RAW, strict=False) is not None:
                for carpeta, _, nombres in self.hdfs.walk(RAW, status=True):
                    for nombre, st in nombres:
                        if not nombre.startswith(("_", ".")):
                            ficheros.append((st["modificationTime"], carpeta[0], nombre, st["length"]))
            ficheros.sort(reverse=True)
            foto["hdfs_ok"] = True
            foto["hdfs_n"] = len(ficheros)
            foto["hdfs_bytes"] = sum(f[3] for f in ficheros)
            foto["hdfs_ultimos"] = [{"carpeta": c.rsplit("/", 1)[-1], "fichero": n, "bytes": b,
                                     "hora": datetime.fromtimestamp(t / 1000).strftime("%H:%M:%S")}
                                    for t, c, n, b in ficheros[:6]]
        except Exception as e:
            foto.update(hdfs_ok=False, hdfs_n=0, hdfs_bytes=0, hdfs_ultimos=[])
            foto["avisos"].append(f"HDFS: {e}")

        # Docker: ¿vive el archivador? ¿y Hive?
        try:
            a = contenedor("archivador")
            if a is None:
                foto["archivador"] = "no creado"
            else:
                exit_code = a.attrs.get("State", {}).get("ExitCode")
                foto["archivador"] = ("vivo" if a.status == "running" else
                                      "muerto (SIGKILL)" if exit_code == 137 else "parado")
                foto["archivador_logs"] = sin_ruido(a.logs(tail=8).decode("utf-8", "replace"))
            h = contenedor("hiveserver2")
            foto["hive_ok"] = h is not None and h.status == "running"
        except Exception as e:
            foto.update(archivador="desconocido", hive_ok=False)
            foto["avisos"].append(f"Docker: {e}")
        return foto

    def ultima_foto(self) -> tuple[dict, list]:
        with self.cerrojo:
            return dict(self.foto), list(self.historial)

    # ---------------------------------------------------------------- acciones rápidas
    def archivador(self, accion: str) -> str:
        c = contenedor("archivador")
        if c is None:
            return "El archivador no existe todavía: arranca el panel con ./pagos panel"
        if accion == "matar":
            if c.status != "running":
                return "El archivador ya estaba parado"
            c.kill()                                    # SIGKILL: muere sin avisar
            return "Archivador muerto en seco (SIGKILL)"
        if accion == "parar":
            c.stop(timeout=30)                          # SIGTERM: termina el lote y sale
            return "Archivador parado de forma ordenada"
        c.start()
        return "Archivador arrancado: sigue desde su último commit"

    # ---------------------------------------------------------------- acciones largas
    def ocupado(self) -> bool:
        return self.tarea is not None

    def lanzar(self, nombre: str, funcion, *args) -> bool:
        with self.cerrojo:
            if self.tarea is not None:
                return False
            self.tarea = {"nombre": nombre, "paso": "empezando…", "inicio": time.time()}

        def correr():
            try:
                mensaje = funcion(*args)
                resultado = {"ok": True, "mensaje": mensaje}
            except Exception as e:
                resultado = {"ok": False, "mensaje": str(e)}
            resultado.update(nombre=nombre, segundos=time.time() - self.tarea["inicio"])
            with self.cerrojo:
                self.ultimo, self.tarea = resultado, None

        threading.Thread(target=correr, daemon=True).start()
        return True

    def _paso(self, texto: str) -> None:
        with self.cerrojo:
            if self.tarea:
                self.tarea["paso"] = texto

    def _anotar_publicado(self, fichero: Path) -> None:
        with open(fichero, encoding="utf-8") as origen, open(PUBLICADO, "a", encoding="utf-8") as destino:
            for linea in origen:
                if linea.strip():
                    destino.write(linea.rstrip("\n") + "\n")

    def publicar(self, relativo: str, ritmo: float = 0) -> str:
        self._paso(f"publicando {relativo}…")
        args = ["producer/productor.py", relativo] + (["--rate", str(ritmo)] if ritmo else [])
        r = python(*args, timeout=3600)
        if r.returncode != 0:
            raise RuntimeError((r.stderr or r.stdout).strip()[-400:])
        self._anotar_publicado(PROYECTO / relativo)
        return r.stdout.strip().splitlines()[0] if r.stdout.strip() else "publicado"

    def generar_y_publicar(self, n: int, ritmo: float, semilla: int) -> str:
        relativo = f"data/panel/lote_s{semilla}_n{n}_{int(time.time())}.jsonl"
        self._paso(f"generando {n} pagos con semilla {semilla}…")
        r = python("simulator/simulador.py", "--seed", str(semilla), "--n-events", str(n),
                   "--out", relativo)
        if r.returncode != 0:
            raise RuntimeError(r.stderr.strip()[-400:])
        return self.publicar(relativo, ritmo)

    def responder(self) -> str:
        """Hive limpia y responde; luego se compara con lo esperado de lo publicado."""
        self._paso("Hive: registrar raw, deduplicar y validar (≈ 40 s)…")
        beeline("02_limpiar.sql")
        HIVE_OUT.mkdir(parents=True, exist_ok=True)
        for q in CONSULTAS:
            self._paso(f"Hive: consulta {q}…")
            (HIVE_OUT / f"{q}.tsv").write_text(beeline(f"consultas/{q}.sql") + "\n", encoding="utf-8")
        if not PUBLICADO.exists() or PUBLICADO.stat().st_size == 0:
            (HIVE_OUT.parent / "comparacion.txt").write_text(
                "Sin comparación: el panel no sabe qué se publicó (publica desde el panel "
                "después de vaciar el sistema).\n", encoding="utf-8")
            return "Respuestas de Hive listas (sin comparación)"
        self._paso("Calculando lo esperado con el oráculo en Python…")
        r = python("tests/calcular_esperado.py", str(PUBLICADO.relative_to(PROYECTO)),
                   "--out", str(ESPERADO.relative_to(PROYECTO)))
        if r.returncode != 0:
            raise RuntimeError(r.stderr.strip()[-400:])
        self._paso("Comparando Hive con lo esperado…")
        r = python("scripts/comparar_con_esperado.py", "--esperado", str(ESPERADO.relative_to(PROYECTO)),
                   "--obtenido", str(HIVE_OUT.relative_to(PROYECTO)))
        (HIVE_OUT.parent / "comparacion.txt").write_text(r.stdout, encoding="utf-8")
        return "Hive = esperado" if r.returncode == 0 else "Hive NO coincide con lo esperado"

    def vaciar(self) -> str:
        """Lo mismo que ./pagos reset-todo."""
        self._paso("parando el archivador…")
        c = contenedor("archivador")
        if c is not None and c.status == "running":
            c.stop(timeout=30)
        self._paso("borrando los offsets del archivador…")
        for _ in range(15):
            try:
                self.admin.delete_consumer_groups([GRUPO])[GRUPO].result(timeout=10)
                break
            except KafkaException as e:
                if "GROUP_ID_NOT_FOUND" in str(e) or "does not exist" in str(e):
                    break
                time.sleep(2)                           # aún tiene un miembro vivo
        self._paso("vaciando el topic…")
        try:
            self.admin.delete_topics([TOPIC])[TOPIC].result(timeout=30)
        except KafkaException:
            pass
        for _ in range(30):
            try:
                self.admin.create_topics([NewTopic(TOPIC, PARTICIONES, 1)])[TOPIC].result(timeout=10)
                break
            except KafkaException:
                time.sleep(1)                           # el borrado aún no ha terminado
        self._paso("vaciando HDFS raw y curated…")
        for ruta in [RAW] + CURATED:
            self.hdfs.delete(ruta, recursive=True)
            self.hdfs.makedirs(ruta)
        self._paso("sincronizando las particiones de Hive…")
        beeline("00_reset.sql")
        PUBLICADO.unlink(missing_ok=True)
        for f in HIVE_OUT.glob("*.tsv"):
            f.unlink()
        (HIVE_OUT.parent / "comparacion.txt").unlink(missing_ok=True)
        with self.cerrojo:
            self.historial.clear()
        return "Sistema vacío"
