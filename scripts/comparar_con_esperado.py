"""Compara lo que devuelve Hive con los resultados esperados (calculados por el oráculo).

Lee:
  * los esperados:  <esperado>/p1_*.csv, p2_*.csv, p3_*.csv y resumen.json
  * lo de Hive:     <obtenido>/p1_*.tsv, p2_*.tsv, p3_*.tsv y control.tsv
                    (los genera ./pagos consultas)

Los números se comparan como decimales exactos: 4.8 y 4.80 son iguales. El orden de las
filas no importa.

    ./pagos py scripts/comparar_con_esperado.py                     # muestra de referencia
    ./pagos py scripts/comparar_con_esperado.py --esperado data/esperado
"""
import argparse
import csv
import json
import sys
from decimal import Decimal, InvalidOperation

PREGUNTAS = [("p1_gasto_dia_categoria", 5), ("p2_franja_horaria", 4),
             ("p3_tarjetas_sospechosas", 5)]


def normalizar(celda: str):
    celda = celda.strip()
    try:
        return Decimal(celda)
    except InvalidOperation:
        return celda


def leer_esperado(ruta: str) -> list[tuple]:
    with open(ruta, encoding="utf-8") as f:
        filas = list(csv.reader(f))[1:]                     # sin cabecera
    return sorted((tuple(normalizar(c) for c in fila) for fila in filas), key=str)


def leer_hive(ruta: str, n_columnas: int) -> list[list[str]]:
    """Filas de la salida TSV de beeline: se quita la cabecera y cualquier línea extra."""
    with open(ruta, encoding="utf-8") as f:
        filas = [linea.rstrip("\n").split("\t") for linea in f if linea.strip()]
    filas = [fila for fila in filas if len(fila) == n_columnas]
    return filas[1:]                                         # la primera es la cabecera


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--esperado", default="tests/muestra/esperado")
    p.add_argument("--obtenido", default="evidencias/hive")
    a = p.parse_args()
    todo_ok = True

    for nombre, n_col in PREGUNTAS:
        esperado = leer_esperado(f"{a.esperado}/{nombre}.csv")
        obtenido = sorted((tuple(normalizar(c) for c in fila)
                           for fila in leer_hive(f"{a.obtenido}/{nombre}.tsv", n_col)), key=str)
        ok = esperado == obtenido
        todo_ok &= ok
        print(f"[{'OK' if ok else 'FALLO'}] {nombre}: {len(obtenido)} filas en Hive, "
              f"{len(esperado)} esperadas")
        if not ok:
            for fila in sorted(set(esperado) - set(obtenido), key=str):
                print(f"    falta en Hive : {fila}")
            for fila in sorted(set(obtenido) - set(esperado), key=str):
                print(f"    sobra en Hive : {fila}")

    with open(f"{a.esperado}/resumen.json", encoding="utf-8") as f:
        res = json.load(f)
    control = {fila[0]: fila[1] for fila in leer_hive(f"{a.obtenido}/control.tsv", 2)}
    comprobaciones = [
        ("pagos_distintos", res["n_pagos_distintos"]),
        ("validos", res["n_validos"]),
        ("importe_aprobado_deduplicado", res["importe_aprobado_deduplicado_eur"]),
    ] + [(f"cuarentena_{regla}", n) for regla, n in res["cuarentena_por_regla"].items()]
    for metrica, esperado in comprobaciones:
        obtenido = control.get(metrica)
        ok = obtenido is not None and normalizar(str(obtenido)) == normalizar(str(esperado))
        todo_ok &= ok
        print(f"[{'OK' if ok else 'FALLO'}] {metrica}: Hive {obtenido} · esperado {esperado}")
    print(f"[info] filas en raw: {control.get('filas_raw')} (mensajes enviados: "
          f"{res['n_mensajes']}) · relecturas del consumidor: {control.get('relecturas_consumidor')}")
    print(f"[info] gasto aprobado SIN deduplicar: {control.get('importe_aprobado_sin_deduplicar')} € "
          f"· deduplicado: {control.get('importe_aprobado_deduplicado')} €")

    print("\nHIVE DA EXACTAMENTE LO ESPERADO" if todo_ok else "\nHAY DIFERENCIAS")
    return 0 if todo_ok else 1


if __name__ == "__main__":
    sys.exit(main())
