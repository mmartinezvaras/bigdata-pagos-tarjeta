"""Catálogo fijo de comercios (datos de referencia).

Son 40 comercios ficticios, 5 por categoría. El catálogo NO depende de la semilla del
simulador: es un dato maestro que no cambia entre ejecuciones, igual que en un banco real el
catálogo de comercios no cambia cada vez que llega un pago.

Ejecutado como script, escribe el catálogo en CSV (se sube a HDFS en /pagos/ref/comercios):

    python simulator/catalogo.py simulator/comercios.csv
"""
import csv
import sys

CATEGORIAS = [
    "ALIMENTACION", "RESTAURACION", "TRANSPORTE", "OCIO",
    "ELECTRONICA", "MODA", "VIAJES", "SALUD",
]

_NOMBRES = {
    "ALIMENTACION": ["Supermercado Sol", "Frutería La Huerta", "Panadería Trigo",
                     "Mercado Central Puesto 12", "Ultramarinos Pepa"],
    "RESTAURACION": ["Bar El Rincón", "Restaurante Mar Azul", "Cafetería Plaza",
                     "Pizzería Vesubio", "Taberna Los Arcos"],
    "TRANSPORTE":   ["Gasolinera Ruta 6", "Radio Taxi Centro", "Parking Centro",
                     "Bus Interurbano Norte", "Alquiler Bici Urbana"],
    "OCIO":         ["Cines Estrella", "Bolera Norte", "Teatro del Puerto",
                     "Librería Páginas", "Escape Room Enigma"],
    "ELECTRONICA":  ["TecnoHogar", "Informática Byte", "Móviles Express",
                     "Electro Barrio", "Zona Juegos"],
    "MODA":         ["Moda Alba", "Zapatería Paso", "Boutique Lino",
                     "Deportes Meta", "Complementos Iris"],
    "VIAJES":       ["Viajes Horizonte", "Hotel Brisa", "Aerolínea Cielo",
                     "Alquiler Coches Ruta", "Hostal Camino"],
    "SALUD":        ["Farmacia Central", "Óptica Visión", "Clínica Dental Sonríe",
                     "Fisioterapia Movimiento", "Parafarmacia Salud"],
}

# Lista de comercios: (merchant_id, nombre, categoria), con ids MER-0001 ... MER-0040.
COMERCIOS = []
for _cat in CATEGORIAS:
    for _nombre in _NOMBRES[_cat]:
        COMERCIOS.append((f"MER-{len(COMERCIOS) + 1:04d}", _nombre, _cat))

IDS_COMERCIO = {c[0] for c in COMERCIOS}

# Comercio que NO existe en el catálogo: lo usa el simulador para la anomalía R3.
COMERCIO_INEXISTENTE = "MER-9999"


def escribir_csv(destino) -> None:
    w = csv.writer(destino, lineterminator="\n")
    w.writerow(["merchant_id", "nombre", "categoria"])
    w.writerows(COMERCIOS)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        with open(sys.argv[1], "w", encoding="utf-8", newline="") as f:
            escribir_csv(f)
    else:
        escribir_csv(sys.stdout)
