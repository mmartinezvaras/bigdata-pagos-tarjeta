"""Simulador de pagos con tarjeta (contrato de datos v1, ver docs/contrato_datos.md).

Genera eventos de pago en formato JSON Lines (un JSON por línea), ordenados por el momento
del pago, como llegarían a un banco en tiempo real. NO publica en Kafka: eso lo hace el
productor (producer/productor.py), que lee este fichero. Separar las dos cosas permite usar
exactamente los mismos eventos como muestra de referencia y como entrada del sistema.

Es reproducible: con la misma semilla y los mismos parámetros genera exactamente los mismos
eventos, byte a byte.

Además de los pagos normales, inyecta a propósito:
  * anomalías (datos incorrectos), una de cada tipo de forma rotatoria;
  * duplicados (el mismo pago, con el mismo tx_id, enviado otra vez un poco más tarde);
  * tarjetas sospechosas (dos pagos el mismo día en países distintos).
Lo que inyecta lo apunta en un "manifiesto" JSON: es la verdad conocida con la que se
comprueba después que el sistema detecta exactamente eso.

Ejemplos:
    python simulator/simulador.py --seed 42 --n-events 1000 --out data/eventos.jsonl \
        --manifest data/manifiesto.json
    python simulator/simulador.py --seed 7 --n-events 50     # escribe en pantalla
"""
import argparse
import itertools
import json
import math
import random
import sys
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

try:  # funciona tanto ejecutado como script como importado desde los tests
    from catalogo import COMERCIOS, COMERCIO_INEXISTENTE
except ImportError:  # pragma: no cover
    from simulator.catalogo import COMERCIOS, COMERCIO_INEXISTENTE

SCHEMA_VERSION = 1
MADRID = ZoneInfo("Europe/Madrid")

# Orden de los campos en cada evento (el del contrato de datos).
CAMPOS = ["schema_version", "tx_id", "event_ts", "card_id", "customer_id", "merchant_id",
          "merchant_category", "amount_eur", "channel", "country", "status"]

# Por categoría: importe mínimo, importe típico (mediana) y máximo en EUR, y probabilidad de
# que el pago sea online.
PERFIL_CATEGORIA = {
    "ALIMENTACION": (3.00, 25.0, 180.00, 0.10),
    "RESTAURACION": (4.00, 18.0, 120.00, 0.15),
    "TRANSPORTE":   (1.50, 12.0, 90.00, 0.40),
    "OCIO":         (6.00, 30.0, 150.00, 0.45),
    "ELECTRONICA":  (15.00, 120.0, 1500.00, 0.60),
    "MODA":         (10.00, 55.0, 400.00, 0.45),
    "VIAJES":       (30.00, 220.0, 2500.00, 0.75),
    "SALUD":        (5.00, 22.0, 160.00, 0.20),
}

# Peso de cada hora del día (hora de Madrid, 0..23): de noche casi no se paga.
PESO_HORA = [1, 1, 0.5, 0.5, 0.5, 1, 2, 4, 6, 7, 8, 9, 10, 10, 9, 7, 7, 8, 9, 9, 8, 6, 4, 2]
_PESO_HORA_ACUM = list(itertools.accumulate(PESO_HORA))

# País de residencia de los titulares (donde pagan normalmente) y países "de viaje".
PAISES_RESIDENCIA = (["ES", "FR", "PT", "IT"], [90, 4, 3, 3])
PAISES_EXTRANJERO = ["FR", "PT", "IT", "DE", "GB", "US", "MA"]

PROB_APROBADO = 0.95

# Tipos de anomalía y la regla del contrato que debe detectarlos.
ANOMALIAS = {
    "IMPORTE_NO_POSITIVO": "R2_IMPORTE",
    "COMERCIO_INEXISTENTE": "R3_COMERCIO",
    "FECHA_FUTURA": "R4_FECHA",
    "CAMPO_VACIO": "R1_CAMPOS",
    "DOMINIO_INVALIDO": "R5_DOMINIOS",
}


# ----------------------------------------------------------------------------------------
# Piezas pequeñas
# ----------------------------------------------------------------------------------------
def generar_tarjetas(rng: random.Random, n_cards: int) -> list[dict]:
    """Crea las tarjetas. Un cliente tiene 1 tarjeta (75 %) o 2 (25 %)."""
    tarjetas, n_cliente = [], 0
    while len(tarjetas) < n_cards:
        n_cliente += 1
        pais = rng.choices(*PAISES_RESIDENCIA)[0]
        for _ in range(2 if rng.random() < 0.25 else 1):
            if len(tarjetas) == n_cards:
                break
            tarjetas.append({
                "card_id": f"CARD-{len(tarjetas) + 1:06d}",
                "customer_id": f"CUST-{n_cliente:05d}",
                "pais": pais,
                "actividad": rng.uniform(0.3, 3.0),   # unas tarjetas se usan más que otras
            })
    return tarjetas


def nuevo_tx_id(rng: random.Random, usados: set) -> str:
    while True:
        tx = f"TX-{rng.getrandbits(48):012x}"
        if tx not in usados:
            usados.add(tx)
            return tx


def momento(rng: random.Random, dia_local: date) -> datetime:
    """Un instante del día indicado (en hora de Madrid), devuelto en UTC."""
    hora = rng.choices(range(24), cum_weights=_PESO_HORA_ACUM)[0]
    local = datetime(dia_local.year, dia_local.month, dia_local.day,
                     hora, rng.randrange(60), rng.randrange(60), tzinfo=MADRID)
    return local.astimezone(timezone.utc)


def importe(rng: random.Random, categoria: str) -> float:
    minimo, tipico, maximo, _ = PERFIL_CATEGORIA[categoria]
    valor = rng.lognormvariate(math.log(tipico), 0.7)   # muchos pagos pequeños, pocos grandes
    return round(min(maximo, max(minimo, valor)), 2)


def pago(rng, usados, tarjeta, comercio, ts, pais, canal=None) -> dict:
    merchant_id, _, categoria = comercio
    if canal is None:
        canal = "ONLINE" if rng.random() < PERFIL_CATEGORIA[categoria][3] else "FISICO"
    return {
        "schema_version": SCHEMA_VERSION,
        "tx_id": nuevo_tx_id(rng, usados),
        "_ts": ts,                                   # se formatea al escribir
        "card_id": tarjeta["card_id"],
        "customer_id": tarjeta["customer_id"],
        "merchant_id": merchant_id,
        "merchant_category": categoria,
        "amount_eur": importe(rng, categoria),
        "channel": canal,
        "country": pais,
        "status": "APROBADO" if rng.random() < PROB_APROBADO else "DENEGADO",
        "_protegido": False,     # True = no se le puede inyectar anomalía
        "_anomalia": None,
    }


def a_json(evento: dict) -> str:
    salida = {k: evento[k] for k in CAMPOS if k != "event_ts"}
    salida["event_ts"] = evento["_ts"].strftime("%Y-%m-%dT%H:%M:%SZ")
    return json.dumps({k: salida[k] for k in CAMPOS}, ensure_ascii=False)


# ----------------------------------------------------------------------------------------
# Generación
# ----------------------------------------------------------------------------------------
def generar(seed: int, n_events: int, n_cards: int, start: date, days: int,
            anomaly_rate: float, dup_rate: float, n_suspicious: int):
    """Devuelve (lista de líneas JSON en orden de envío, manifiesto)."""
    if n_suspicious * 2 > n_events:
        raise ValueError("Hacen falta al menos 2 eventos por tarjeta sospechosa")
    if n_suspicious > n_cards:
        raise ValueError("No puede haber más tarjetas sospechosas que tarjetas")

    rng = random.Random(seed)
    tarjetas = generar_tarjetas(rng, n_cards)
    dias = [start + timedelta(days=d) for d in range(days)]
    usados: set = set()
    eventos: list[dict] = []

    # 1) Tarjetas sospechosas: un pago en su país y, el MISMO día (hora de Madrid), otro
    #    pago físico en un país extranjero entre 30 min y 3 h después. Es físicamente
    #    imposible, así que es lo que la pregunta P3 tiene que encontrar.
    sospechosas = []
    for t in rng.sample(tarjetas, n_suspicious):
        dia = rng.choice(dias)
        extranjero = rng.choice([p for p in PAISES_EXTRANJERO if p != t["pais"]])
        ts1 = momento(rng, dia)
        ts2 = ts1 + timedelta(minutes=rng.randint(30, 180))
        if ts2.astimezone(MADRID).date() != dia:       # si se pasa de medianoche, va antes
            ts2 = ts1 - timedelta(minutes=rng.randint(30, 180))
        for ts, pais, canal in ((ts1, t["pais"], None), (ts2, extranjero, "FISICO")):
            e = pago(rng, usados, t, rng.choice(COMERCIOS), ts, pais, canal)
            e["_protegido"] = True
            eventos.append(e)
        sospechosas.append({"card_id": t["card_id"], "fecha_local": dia.isoformat(),
                            "paises": sorted([t["pais"], extranjero])})

    # 2) Pagos normales: cada tarjeta paga siempre en su país de residencia.
    acum = list(itertools.accumulate(t["actividad"] for t in tarjetas))
    while len(eventos) < n_events:
        t = rng.choices(tarjetas, cum_weights=acum)[0]
        eventos.append(pago(rng, usados, t, rng.choice(COMERCIOS),
                            momento(rng, rng.choice(dias)), t["pais"]))

    # Orden de llegada = orden temporal (desempate por tx_id para que sea reproducible).
    eventos.sort(key=lambda e: (e["_ts"], e["tx_id"]))

    # 3) Anomalías: sobre eventos no protegidos, rotando los tipos para que salgan todos.
    candidatos = [i for i, e in enumerate(eventos) if not e["_protegido"]]
    n_anom = min(round(n_events * anomaly_rate), len(candidatos))
    tipos = list(ANOMALIAS)
    for k, i in enumerate(sorted(rng.sample(candidatos, n_anom))):
        e, tipo = eventos[i], tipos[k % len(tipos)]
        e["_anomalia"] = tipo
        if tipo == "IMPORTE_NO_POSITIVO":
            e["amount_eur"] = 0.0 if rng.random() < 0.3 else -e["amount_eur"]
        elif tipo == "COMERCIO_INEXISTENTE":
            e["merchant_id"] = COMERCIO_INEXISTENTE
        elif tipo == "FECHA_FUTURA":
            # Reloj del terminal mal puesto: el pago "dice" ser de dentro de 10 años, pero
            # llega en su sitio normal de la secuencia.
            e["_ts"] = e["_ts"] + timedelta(days=3650)
        elif tipo == "CAMPO_VACIO":
            e["card_id"] = None
        elif tipo == "DOMINIO_INVALIDO":
            e["channel"] = "TELEFONO"

    # 4) Duplicados: copias exactas (mismo tx_id) de pagos válidos, reenviadas entre 1 y 20
    #    posiciones más tarde, como haría un terminal que reintenta el envío.
    validos = [i for i, e in enumerate(eventos) if e["_anomalia"] is None]
    n_dup = min(round(n_events * dup_rate), len(validos))
    posiciones = [(float(i), i) for i in range(len(eventos))]
    duplicados = []
    for i in sorted(rng.sample(validos, n_dup)):
        destino = min(i + rng.randint(1, 20), len(eventos) - 1)
        posiciones.append((destino + 0.5, i))
        duplicados.append(eventos[i]["tx_id"])
    posiciones.sort()
    lineas = [a_json(eventos[i]) for _, i in posiciones]

    manifiesto = {
        "parametros": {"seed": seed, "n_events": n_events, "n_cards": n_cards,
                       "start": start.isoformat(), "days": days,
                       "anomaly_rate": anomaly_rate, "dup_rate": dup_rate,
                       "n_suspicious": n_suspicious},
        "n_pagos_distintos": n_events,
        "n_mensajes": len(lineas),
        "n_duplicados": n_dup,
        "anomalias_por_regla": {regla: sum(1 for e in eventos if e["_anomalia"] == tipo)
                                for tipo, regla in ANOMALIAS.items()},
        "anomalias": [{"tx_id": e["tx_id"], "tipo": e["_anomalia"],
                       "regla": ANOMALIAS[e["_anomalia"]]}
                      for e in eventos if e["_anomalia"]],
        "duplicados_tx_id": duplicados,
        "tarjetas_sospechosas": sorted(sospechosas, key=lambda s: s["card_id"]),
    }
    return lineas, manifiesto


# ----------------------------------------------------------------------------------------
# Línea de comandos
# ----------------------------------------------------------------------------------------
def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Simulador de pagos con tarjeta (JSON Lines).")
    p.add_argument("--seed", type=int, default=42, help="semilla (misma semilla = mismos datos)")
    p.add_argument("--n-events", type=int, default=1000,
                   help="nº de pagos distintos (los duplicados van aparte)")
    p.add_argument("--n-cards", type=int, default=None,
                   help="nº de tarjetas (por defecto, 1 por cada 25 pagos)")
    p.add_argument("--start", type=date.fromisoformat, default=date(2026, 10, 26),
                   help="primer día simulado (hora de Madrid), AAAA-MM-DD")
    p.add_argument("--days", type=int, default=7, help="nº de días simulados")
    p.add_argument("--anomaly-rate", type=float, default=0.03, help="fracción de pagos con anomalía")
    p.add_argument("--dup-rate", type=float, default=0.02, help="fracción de pagos que se duplican")
    p.add_argument("--n-suspicious", type=int, default=None,
                   help="nº de tarjetas sospechosas (por defecto, 1 de cada 50 tarjetas)")
    p.add_argument("--out", default="-", help="fichero de salida .jsonl ('-' = pantalla)")
    p.add_argument("--manifest", default=None, help="fichero JSON con lo inyectado (opcional)")
    a = p.parse_args(argv)

    n_cards = a.n_cards or max(4, a.n_events // 25)
    n_susp = a.n_suspicious if a.n_suspicious is not None else max(1, n_cards // 50)
    lineas, manifiesto = generar(a.seed, a.n_events, n_cards, a.start, a.days,
                                 a.anomaly_rate, a.dup_rate, n_susp)

    salida = sys.stdout if a.out == "-" else open(a.out, "w", encoding="utf-8")
    try:
        for linea in lineas:
            salida.write(linea + "\n")
    finally:
        if salida is not sys.stdout:
            salida.close()
    if a.manifest:
        with open(a.manifest, "w", encoding="utf-8") as f:
            json.dump(manifiesto, f, ensure_ascii=False, indent=2)

    print(f"[simulador] {manifiesto['n_mensajes']} mensajes = {a.n_events} pagos + "
          f"{manifiesto['n_duplicados']} duplicados · anomalías {manifiesto['anomalias_por_regla']} · "
          f"{len(manifiesto['tarjetas_sospechosas'])} tarjetas sospechosas", file=sys.stderr)


if __name__ == "__main__":
    main()
