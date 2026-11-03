# Muestra de referencia

Muestra pequeña con resultados **conocidos de antemano**. Sirve para comprobar que lo que
devuelve Hive al final del recorrido es correcto: si Hive da estas mismas tablas, el sistema
calcula bien.

## Cómo se ha generado

```bash
python simulator/simulador.py --seed 42 --n-events 24 --n-cards 6 --days 2 \
    --anomaly-rate 0.21 --dup-rate 0.125 --n-suspicious 1 \
    --out tests/muestra/eventos.jsonl --manifest tests/muestra/manifiesto.json
python tests/calcular_esperado.py tests/muestra/eventos.jsonl \
    --catalogo simulator/comercios.csv --out tests/muestra/esperado
```

Con la semilla 42 y estos parámetros sale siempre exactamente esto. Los porcentajes de
anomalías y duplicados son mucho más altos que en una ejecución normal a propósito, para que
aparezca **una anomalía de cada tipo**, varios duplicados y una tarjeta sospechosa en solo
27 mensajes.

## Los 27 mensajes, en orden de llegada

Hora en **hora de Madrid** (en el fichero está en UTC, una hora menos).

| # | tx_id | Hora | Tarjeta | Comercio | Categoría | Importe € | Canal | País | Estado | Qué le pasa |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `TX-3fa71bf90e27` | 26/10 07:46 | CARD-000006 | MER-0004 | ALIMENTACION | -11.25 | FISICO | ES | APROBADO | 🚫 cuarentena `R2_IMPORTE` |
| 2 | `TX-e70666aa9385` | 26/10 09:31 | CARD-000001 | MER-0006 | RESTAURACION | 7.67 | FISICO | ES | DENEGADO | ✅ válido |
| 3 | `TX-bf3c434308bc` | 26/10 11:15 | CARD-000006 | MER-0030 | MODA | 148.63 | FISICO | ES | APROBADO | ✅ válido |
| 4 | `TX-8181369147eb` | 26/10 13:36 | CARD-000003 | MER-0005 | ALIMENTACION | 20.76 | ONLINE | ES | APROBADO | ✅ válido |
| 5 | `TX-fd51451b4cf3` | 26/10 14:15 | CARD-000004 | MER-0005 | ALIMENTACION | 15.19 | FISICO | ES | APROBADO | ✅ válido |
| 6 | `TX-995400257ad1` | 26/10 15:49 | CARD-000002 | MER-0024 | ELECTRONICA | 213.29 | ONLINE | ES | APROBADO | ✅ válido |
| 7 | `TX-f10c1eb0e38a` | 26/10 16:43 | CARD-000001 | MER-0012 | TRANSPORTE | 22.35 | ONLINE | ES | APROBADO | ✅ válido |
| 8 | `TX-eb5c80bacd64` | 26/10 18:03 | CARD-000003 | MER-0035 | VIAJES | 363.70 | ONLINE | ES | DENEGADO | ✅ válido |
| 9 | `TX-c40d885f6e66` | 26/10 18:52 | CARD-000006 | MER-0006 | RESTAURACION | 4.80 | FISICO | ES | APROBADO | ✅ válido |
| 10 | `TX-8ce2db20a56e` | 26/10 20:04 | CARD-000002 | MER-9999 | MODA | 13.73 | FISICO | ES | APROBADO | 🚫 cuarentena `R3_COMERCIO` |
| 11 | `TX-fd51451b4cf3` | 26/10 14:15 | CARD-000004 | MER-0005 | ALIMENTACION | 15.19 | FISICO | ES | APROBADO | 🔁 duplicado (se descarta) |
| 12 | `TX-a2bc7412b293` | 26/10 20:55 | CARD-000004 | MER-0019 | OCIO | 48.44 | ONLINE | ES | APROBADO | ✅ válido |
| 13 | `TX-474a3ceddf2d` | 26/10 20:55 | CARD-000003 | MER-0015 | TRANSPORTE | 15.54 | ONLINE | ES | APROBADO | ✅ válido |
| 14 | `TX-dc98e2acf72f` | 27/10 07:18 | CARD-000003 | MER-0008 | RESTAURACION | 14.65 | FISICO | ES | APROBADO | ✅ válido |
| 15 | `TX-28f4a0a04dc4` | 27/10 08:03 | CARD-000002 | MER-0009 | RESTAURACION | 33.03 | ONLINE | ES | APROBADO | ✅ válido |
| 16 | `TX-8dcd969b6662` | 27/10 08:14 | CARD-000003 | MER-0034 | VIAJES | 145.18 | ONLINE | ES | APROBADO | ✅ válido |
| 17 | `TX-18d01825bc54` | 27/10 10:30 | CARD-000006 | MER-0037 | SALUD | 29.57 | FISICO | ES | APROBADO | ✅ válido |
| 18 | `TX-371e27cd8130` | 27/10 11:55 | CARD-000002 | MER-0028 | MODA | 125.83 | ONLINE | ES | APROBADO | ✅ válido |
| 19 | `TX-e0599132b63e` | 24/10/**2036** 14:04 | CARD-000001 | MER-0003 | ALIMENTACION | 34.86 | FISICO | ES | APROBADO | 🚫 cuarentena `R4_FECHA` |
| 20 | `TX-d8f55be6128e` | 27/10 13:05 | CARD-000002 | MER-0007 | RESTAURACION | 13.95 | FISICO | GB | APROBADO | ✅ válido |
| 21 | `TX-7c96b7e99aca` | 27/10 13:46 | CARD-000002 | MER-0030 | MODA | 30.51 | FISICO | ES | DENEGADO | ✅ válido |
| 22 | `TX-43ff3d1a85dd` | 27/10 21:13 | CARD-000003 | MER-0027 | MODA | 37.57 | FISICO | ES | APROBADO | ✅ válido |
| 23 | `TX-474a3ceddf2d` | 26/10 20:55 | CARD-000003 | MER-0015 | TRANSPORTE | 15.54 | ONLINE | ES | APROBADO | 🔁 duplicado (se descarta) |
| 24 | `TX-deda1b3dbd5c` | 27/10 22:48 | **(vacío)** | MER-0001 | ALIMENTACION | 64.67 | FISICO | ES | APROBADO | 🚫 cuarentena `R1_CAMPOS` |
| 25 | `TX-ae841d53434b` | 27/10 22:55 | CARD-000003 | MER-0030 | MODA | 89.67 | TELEFONO | ES | APROBADO | 🚫 cuarentena `R5_DOMINIOS` |
| 26 | `TX-337eb09b2a5c` | 27/10 22:59 | CARD-000002 | MER-0034 | VIAJES | 336.68 | FISICO | ES | APROBADO | ✅ válido |
| 27 | `TX-43ff3d1a85dd` | 27/10 21:13 | CARD-000003 | MER-0027 | MODA | 37.57 | FISICO | ES | APROBADO | 🔁 duplicado (se descarta) |

**Resumen:** 27 mensajes → 3 duplicados descartados →
24 pagos distintos → 5 a cuarentena (uno por regla) →
**19 pagos válidos**.

## Resultados esperados

### P1 · Gasto por día y categoría (solo pagos APROBADOS)

| fecha | categoria | n_pagos | importe_total_eur | ticket_medio_eur |
|---|---|---|---|---|
| 2026-10-26 | ALIMENTACION | 2 | 35.95 | 17.98 |
| 2026-10-26 | ELECTRONICA | 1 | 213.29 | 213.29 |
| 2026-10-26 | MODA | 1 | 148.63 | 148.63 |
| 2026-10-26 | OCIO | 1 | 48.44 | 48.44 |
| 2026-10-26 | RESTAURACION | 1 | 4.80 | 4.80 |
| 2026-10-26 | TRANSPORTE | 2 | 37.89 | 18.95 |
| 2026-10-27 | MODA | 2 | 163.40 | 81.70 |
| 2026-10-27 | RESTAURACION | 3 | 61.63 | 20.54 |
| 2026-10-27 | SALUD | 1 | 29.57 | 29.57 |
| 2026-10-27 | VIAJES | 2 | 481.86 | 240.93 |

### P2 · Pagos por hora del día (hora de Madrid, todos los intentos válidos)

| hora | n_pagos | n_online | pct_online |
|---|---|---|---|
| 7 | 1 | 0 | 0.0 |
| 8 | 2 | 2 | 100.0 |
| 9 | 1 | 0 | 0.0 |
| 10 | 1 | 0 | 0.0 |
| 11 | 2 | 1 | 50.0 |
| 13 | 3 | 1 | 33.3 |
| 14 | 1 | 0 | 0.0 |
| 15 | 1 | 1 | 100.0 |
| 16 | 1 | 1 | 100.0 |
| 18 | 2 | 1 | 50.0 |
| 20 | 2 | 2 | 100.0 |
| 21 | 1 | 0 | 0.0 |
| 22 | 1 | 0 | 0.0 |

### P3 · Tarjetas con pagos en 2 o más países el mismo día

| card_id | fecha | n_paises | paises | n_pagos |
|---|---|---|---|---|
| CARD-000002 | 2026-10-27 | 2 | ES,GB | 5 |

### Cuarentena

| tx_id | regla |
|---|---|
| TX-3fa71bf90e27 | R2_IMPORTE |
| TX-8ce2db20a56e | R3_COMERCIO |
| TX-e0599132b63e | R4_FECHA |
| TX-deda1b3dbd5c | R1_CAMPOS |
| TX-ae841d53434b | R5_DOMINIOS |

## Comprobaciones a mano (para la defensa)

- **P1, 26/10, ALIMENTACION = 2 pagos, 35,95 €.** Cuentan el #4 (20,76) y el #5 (15,19).
  No cuentan el #1 (importe negativo, a cuarentena) ni el #11 (duplicado del #5).
  Ticket medio: 35,95 / 2 = 17,975 → **17,98** (redondeo hacia arriba en el 5).
- **P1, 26/10, TRANSPORTE = 2 pagos, 37,89 €.** #7 (22,35) + #13 (15,54). El #23 es el
  duplicado del #13. Ticket medio 18,945 → **18,95**.
- **P1, 27/10, RESTAURACION = 3 pagos, 61,63 €.** #14 (14,65) + #15 (33,03) + #20 (13,95).
  El #20 se hizo en Reino Unido, pero es un pago válido y aprobado, así que cuenta.
- **P1 no incluye el #2 ni el #8** porque son DENEGADOS: un pago denegado no es gasto.
- **P2, hora 20 = 2 pagos, los 2 online.** #12 y #13. El #10 (20:04) está en cuarentena por
  comercio inexistente y el #23 es duplicado.
- **P3 = CARD-000002 el 27/10.** Pagó en Reino Unido a las 13:05 (#20) y en España a las
  13:46 (#21). Son 41 minutos: imposible estar en los dos sitios. El #21 está DENEGADO, pero
  en P3 cuenta igual, porque para fraude un intento en otro país ya es una señal.
- **El #19 dice ser de octubre de 2036**: el reloj del terminal está mal. Llega en su orden
  normal, pero su fecha es futura, así que va a cuarentena por `R4_FECHA`.

## Prueba de duplicados con esta muestra

| | Importe aprobado |
|---|---|
| Sumando los 27 mensajes tal cual (sin deduplicar) | 1293.76 € |
| Deduplicando por `tx_id` | **1225.46 €** |

La diferencia es la suma de los tres pagos repetidos (15,19 + 15,54 + 37,57). Sin
deduplicar, el banco diría que sus clientes han gastado más de lo que realmente han gastado.
