"""Calcula los resultados ESPERADOS de las tres preguntas de negocio, en Python puro.

Es una implementación independiente del contrato de datos (docs/contrato_datos.md): no usa
Kafka, ni HDFS, ni Hive, ni el código del simulador. Lee el fichero JSON Lines tal cual y
aplica, en este orden:

  1. Deduplicación por tx_id (se queda la primera aparición).
  2. Reglas de validez R1..R5; un evento que incumple alguna va a cuarentena con la
     PRIMERA regla que incumple, en el orden R1, R2, R3, R4, R5.
  3. P1, P2 y P3 sobre los eventos válidos, con días y horas en hora de Madrid.

Sirve de "oráculo": si Hive da lo mismo que este script con los mismos eventos, Hive está
bien. Uso:

    python tests/calcular_esperado.py tests/muestra/eventos.jsonl \
        --catalogo simulator/comercios.csv --out tests/muestra/esperado
"""
import argparse
import csv
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from zoneinfo import ZoneInfo

MADRID = ZoneInfo("Europe/Madrid")

OBLIGATORIOS = ["tx_id", "event_ts", "card_id", "customer_id", "merchant_id",
                "merchant_category", "amount_eur", "channel", "country", "status"]
CANALES = {"FISICO", "ONLINE"}
ESTADOS = {"APROBADO", "DENEGADO"}
CATEGORIAS = {"ALIMENTACION", "RESTAURACION", "TRANSPORTE", "OCIO",
              "ELECTRONICA", "MODA", "VIAJES", "SALUD"}
PAIS = re.compile(r"^[A-Z]{2}$")
DOS = Decimal("0.01")


def leer_catalogo(ruta: str) -> set:
    with open(ruta, encoding="utf-8") as f:
        return {fila["merchant_id"] for fila in csv.DictReader(f)}


def parse_ts(texto: str) -> datetime:
    return datetime.strptime(texto, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def regla_incumplida(e: dict, comercios: set, ahora: datetime):
    """Devuelve el código de la primera regla que incumple el evento, o None si es válido."""
    if any(e.get(c) in (None, "") for c in OBLIGATORIOS):
        return "R1_CAMPOS"
    if not (Decimal("0") < Decimal(str(e["amount_eur"])) <= Decimal("10000")):
        return "R2_IMPORTE"
    if e["merchant_id"] not in comercios:
        return "R3_COMERCIO"
    if parse_ts(e["event_ts"]) > ahora:
        return "R4_FECHA"
    if (e["channel"] not in CANALES or e["status"] not in ESTADOS
            or e["merchant_category"] not in CATEGORIAS or not PAIS.match(e["country"])):
        return "R5_DOMINIOS"
    return None


def calcular(ruta_eventos: str, comercios: set, ahora: datetime | None = None) -> dict:
    ahora = ahora or datetime.now(timezone.utc)
    with open(ruta_eventos, encoding="utf-8") as f:
        mensajes = [json.loads(linea) for linea in f if linea.strip()]

    # 1) Deduplicación
    vistos, unicos = set(), []
    for e in mensajes:
        if e["tx_id"] in vistos:
            continue
        vistos.add(e["tx_id"])
        unicos.append(e)

    # 2) Validación
    validos, cuarentena = [], []
    for e in unicos:
        regla = regla_incumplida(e, comercios, ahora)
        if regla:
            cuarentena.append({"tx_id": e["tx_id"], "regla": regla})
        else:
            local = parse_ts(e["event_ts"]).astimezone(MADRID)
            validos.append({**e, "_fecha": local.date().isoformat(), "_hora": local.hour,
                            "_importe": Decimal(str(e["amount_eur"]))})

    # P1: por día (Madrid) y categoría, solo pagos APROBADOS
    p1 = defaultdict(lambda: [0, Decimal("0")])
    for e in validos:
        if e["status"] == "APROBADO":
            fila = p1[(e["_fecha"], e["merchant_category"])]
            fila[0] += 1
            fila[1] += e["_importe"]
    p1_filas = [{"fecha": f, "categoria": c, "n_pagos": n, "importe_total_eur": str(t.quantize(DOS)),
                 "ticket_medio_eur": str((t / n).quantize(DOS, ROUND_HALF_UP))}
                for (f, c), (n, t) in sorted(p1.items())]

    # P2: por hora del día (Madrid), todos los intentos válidos
    p2 = defaultdict(lambda: [0, 0])
    for e in validos:
        p2[e["_hora"]][0] += 1
        p2[e["_hora"]][1] += e["channel"] == "ONLINE"
    p2_filas = [{"hora": h, "n_pagos": n, "n_online": o,
                 "pct_online": str((Decimal(100 * o) / n).quantize(Decimal("0.1"), ROUND_HALF_UP))}
                for h, (n, o) in sorted(p2.items())]

    # P3: tarjetas con pagos en 2 o más países distintos el mismo día (Madrid)
    p3 = defaultdict(lambda: [set(), 0])
    for e in validos:
        p3[(e["card_id"], e["_fecha"])][0].add(e["country"])
        p3[(e["card_id"], e["_fecha"])][1] += 1
    p3_filas = [{"card_id": c, "fecha": f, "n_paises": len(ps), "paises": ",".join(sorted(ps)),
                 "n_pagos": n}
                for (c, f), (ps, n) in sorted(p3.items()) if len(ps) >= 2]

    # Para la prueba de duplicados: gasto aprobado contando los duplicados (lo que saldría
    # si NO se deduplicara) frente al real.
    validos_tx = {e["tx_id"] for e in validos}
    inflado = sum((Decimal(str(m["amount_eur"])) for m in mensajes
                   if m["tx_id"] in validos_tx and m["status"] == "APROBADO"), Decimal("0"))
    real = sum((e["_importe"] for e in validos if e["status"] == "APROBADO"), Decimal("0"))

    por_regla = defaultdict(int)
    for c in cuarentena:
        por_regla[c["regla"]] += 1

    resumen = {
        "n_mensajes": len(mensajes),
        "n_duplicados_descartados": len(mensajes) - len(unicos),
        "n_pagos_distintos": len(unicos),
        "n_validos": len(validos),
        "n_cuarentena": len(cuarentena),
        "cuarentena_por_regla": dict(sorted(por_regla.items())),
        "importe_aprobado_sin_deduplicar_eur": str(inflado.quantize(DOS)),
        "importe_aprobado_deduplicado_eur": str(real.quantize(DOS)),
    }
    return {"resumen": resumen, "p1": p1_filas, "p2": p2_filas, "p3": p3_filas,
            "cuarentena": cuarentena}


def escribir_csv(ruta: str, filas: list[dict], columnas: list[str]) -> None:
    with open(ruta, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columnas, lineterminator="\n")
        w.writeheader()
        w.writerows(filas)


def main() -> None:
    p = argparse.ArgumentParser(description="Resultados esperados P1-P3 (oráculo en Python).")
    p.add_argument("eventos", help="fichero .jsonl del simulador")
    p.add_argument("--catalogo", default="simulator/comercios.csv")
    p.add_argument("--out", default=None, help="carpeta donde guardar los CSV esperados")
    a = p.parse_args()

    r = calcular(a.eventos, leer_catalogo(a.catalogo))
    print(json.dumps(r["resumen"], ensure_ascii=False, indent=2))
    for nombre in ("p1", "p2", "p3"):
        print(f"\n== {nombre.upper()} ({len(r[nombre])} filas)")
        for fila in r[nombre][:30]:
            print("  " + " | ".join(str(v) for v in fila.values()))
        if len(r[nombre]) > 30:
            print(f"  ... ({len(r[nombre]) - 30} filas más)")

    if a.out:
        os.makedirs(a.out, exist_ok=True)
        escribir_csv(f"{a.out}/p1_gasto_dia_categoria.csv", r["p1"],
                     ["fecha", "categoria", "n_pagos", "importe_total_eur", "ticket_medio_eur"])
        escribir_csv(f"{a.out}/p2_franja_horaria.csv", r["p2"],
                     ["hora", "n_pagos", "n_online", "pct_online"])
        escribir_csv(f"{a.out}/p3_tarjetas_sospechosas.csv", r["p3"],
                     ["card_id", "fecha", "n_paises", "paises", "n_pagos"])
        escribir_csv(f"{a.out}/cuarentena.csv", r["cuarentena"], ["tx_id", "regla"])
        with open(f"{a.out}/resumen.json", "w", encoding="utf-8") as f:
            json.dump(r["resumen"], f, ensure_ascii=False, indent=2)
        print(f"\nGuardado en {a.out}/")


if __name__ == "__main__":
    main()
