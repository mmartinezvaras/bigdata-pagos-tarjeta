"""Comprobaciones del simulador. Se ejecutan sin librerías extra:

    python tests/test_simulador.py

1. Reproducibilidad: misma semilla -> mismos bytes; otra semilla -> datos distintos.
2. Recuentos: mensajes = pagos distintos + duplicados.
3. El oráculo (calcular_esperado.py) encuentra EXACTAMENTE lo que el simulador inyectó:
   las mismas anomalías por regla, los mismos duplicados y las mismas tarjetas sospechosas.
"""
import json
import os
import sys
import tempfile
from datetime import date

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(RAIZ, "simulator"), os.path.join(RAIZ, "tests")]

from calcular_esperado import calcular  # noqa: E402
from catalogo import IDS_COMERCIO  # noqa: E402
from simulador import generar  # noqa: E402

PARAMS = dict(n_events=5000, n_cards=200, start=date(2026, 10, 26), days=7,
              anomaly_rate=0.03, dup_rate=0.02, n_suspicious=6)


def comprobar(nombre, condicion, detalle=""):
    print(("[OK]    " if condicion else "[FALLO] ") + nombre + (f"  ({detalle})" if detalle else ""))
    return condicion


def main() -> int:
    ok = True
    a, man = generar(seed=1, **PARAMS)
    b, _ = generar(seed=1, **PARAMS)
    c, _ = generar(seed=2, **PARAMS)
    ok &= comprobar("misma semilla genera exactamente lo mismo", a == b)
    ok &= comprobar("otra semilla genera datos distintos", a != c)

    tx = [json.loads(x)["tx_id"] for x in a]
    ok &= comprobar("mensajes = pagos + duplicados",
                    len(a) == PARAMS["n_events"] + man["n_duplicados"],
                    f"{len(a)} = {PARAMS['n_events']} + {man['n_duplicados']}")
    ok &= comprobar("tx_id distintos = nº de pagos", len(set(tx)) == PARAMS["n_events"])

    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False, encoding="utf-8") as f:
        f.write("\n".join(a) + "\n")
        ruta = f.name
    try:
        r = calcular(ruta, IDS_COMERCIO)
    finally:
        os.unlink(ruta)

    res = r["resumen"]
    ok &= comprobar("duplicados detectados = inyectados",
                    res["n_duplicados_descartados"] == man["n_duplicados"],
                    f"{res['n_duplicados_descartados']} vs {man['n_duplicados']}")
    esperado = {k: v for k, v in man["anomalias_por_regla"].items() if v}
    ok &= comprobar("cuarentena por regla = anomalías inyectadas",
                    res["cuarentena_por_regla"] == esperado, str(res["cuarentena_por_regla"]))
    tx_anom = {x["tx_id"]: x["regla"] for x in man["anomalias"]}
    tx_cuar = {x["tx_id"]: x["regla"] for x in r["cuarentena"]}
    ok &= comprobar("cada anomalía cae en cuarentena con SU regla", tx_anom == tx_cuar)

    susp_esperadas = {(s["card_id"], s["fecha_local"]) for s in man["tarjetas_sospechosas"]}
    susp_halladas = {(x["card_id"], x["fecha"]) for x in r["p3"]}
    ok &= comprobar("P3 encuentra exactamente las tarjetas sospechosas",
                    susp_esperadas == susp_halladas, f"{len(susp_halladas)} tarjetas")

    inflado = float(res["importe_aprobado_sin_deduplicar_eur"])
    real = float(res["importe_aprobado_deduplicado_eur"])
    ok &= comprobar("los duplicados inflarían el gasto si no se deduplicara", inflado > real,
                    f"{inflado:.2f} > {real:.2f}")

    print("\nTODO CORRECTO" if ok else "\nHAY FALLOS")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
