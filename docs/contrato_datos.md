# Contrato de datos · Pagos con tarjeta

Versión del contrato: **1** (campo `schema_version` en cada evento).

## 1. Qué representa un evento

Cada evento es **un intento de pago con tarjeta** en un comercio, en el momento en que el banco
lo autoriza o lo deniega. Un evento = un pago. No hay eventos de "modificación": si un pago se
repite con el mismo `tx_id`, es un **duplicado**, no un pago nuevo.

## 2. Campos del evento (lo que publica el simulador en Kafka)

| Campo | Tipo | Unidad / formato | Obligatorio | Descripción |
|---|---|---|---|---|
| `schema_version` | int | — | sí | Versión del contrato. Ahora `1`. |
| `tx_id` | string | `TX-` + 12 hex | sí | Identificador **único** del pago. Es la base para detectar duplicados. |
| `event_ts` | string | ISO 8601 en **UTC**, p. ej. `2026-11-02T13:05:12Z` | sí | Momento del pago. |
| `card_id` | string | `CARD-000001` | sí | Tarjeta usada. **Es la clave del mensaje en Kafka.** |
| `customer_id` | string | `CUST-00001` | sí | Titular de la tarjeta. Una tarjeta pertenece a un solo cliente. |
| `merchant_id` | string | `MER-0001` | sí | Comercio. Debe existir en el catálogo de comercios. |
| `merchant_category` | string | enum (ver abajo) | sí | Categoría del comercio. |
| `amount_eur` | decimal(10,2) | **euros**, 2 decimales | sí | Importe del pago. |
| `channel` | string | `FISICO` \| `ONLINE` | sí | Pago en tienda física (TPV) o por internet. |
| `country` | string | ISO 3166-1 alfa-2 (`ES`, `FR`…) | sí | País donde se produce el pago. |
| `status` | string | `APROBADO` \| `DENEGADO` | sí | Resultado de la autorización. |

Categorías (`merchant_category`): `ALIMENTACION`, `RESTAURACION`, `TRANSPORTE`, `OCIO`,
`ELECTRONICA`, `MODA`, `VIAJES`, `SALUD`.

## 3. Metadatos que añade el consumidor al archivar (trazabilidad)

El simulador no los conoce: los añade el proceso de archivo al leer de Kafka.

| Campo | Descripción |
|---|---|
| `kafka_topic` | Topic de origen (`card-payments`). |
| `kafka_partition` | Partición de origen. |
| `kafka_offset` | Offset del mensaje dentro de su partición. |
| `ingest_ts` | Momento (UTC) en que el consumidor lo escribió en HDFS. |
| `batch_id` | Identificador del lote de escritura. |

Con `(kafka_partition, kafka_offset)` se puede localizar el mensaje exacto en Kafka. Si el
mismo `tx_id` aparece con offsets distintos, fue **publicado dos veces**; si aparece con el
mismo offset en dos lotes, fue **releído** por el consumidor.

## 4. Reglas de validez

Un evento que incumple alguna regla **no entra** en la tabla de métricas: va a la tabla de
cuarentena con el código de la regla incumplida. Si incumple varias, se anota **la primera**
en el orden R1, R2, R3, R4, R5.

| Código | Regla |
|---|---|
| `R1_CAMPOS` | Todos los campos obligatorios presentes y no vacíos. |
| `R2_IMPORTE` | `0 < amount_eur ≤ 10000`. |
| `R3_COMERCIO` | `merchant_id` existe en el catálogo de comercios. |
| `R4_FECHA` | `event_ts` no es posterior a `ingest_ts` (no hay pagos en el futuro). |
| `R5_DOMINIOS` | `channel`, `status`, `merchant_category` y `country` tienen valores válidos. |

**Duplicados:** no son un error de dato. Se conserva **una sola fila por `tx_id`** (la de
menor `(kafka_partition, kafka_offset)`) y se cuentan aparte para poder demostrarlo.

**Orden de tratamiento:** primero se deduplica por `tx_id` y después se validan las reglas.
Así un pago incorrecto que llega dos veces aparece una sola vez en cuarentena.

## 5. Anomalías que inyecta el simulador a propósito

| Anomalía | Cómo se genera | Regla que la detecta |
|---|---|---|
| Importe negativo o cero | `amount_eur ≤ 0` | `R2_IMPORTE` |
| Comercio inexistente | `merchant_id = MER-9999` | `R3_COMERCIO` |
| Fecha en el futuro | `event_ts` + 10 años (reloj del terminal mal puesto) | `R4_FECHA` |
| Campo vacío | `card_id` nulo | `R1_CAMPOS` |
| Valor no permitido | `channel = TELEFONO` | `R5_DOMINIOS` |
| Duplicado | se reenvía un pago ya publicado (mismo `tx_id`) | deduplicación por `tx_id` |

Los tipos de anomalía se reparten de forma rotatoria, así que con 5 o más anomalías aparece
al menos una de cada tipo. Solo se inyectan en pagos normales (nunca en los de las tarjetas
sospechosas), y los duplicados solo se hacen de pagos válidos.

Además, el simulador genera a propósito algunas **tarjetas con patrón sospechoso**: un pago
en su país y, el mismo día y entre 30 min y 3 h después, un pago físico en otro país. No son
datos incorrectos: son datos válidos que la pregunta 3 debe encontrar. El resto de tarjetas
pagan siempre en su país de residencia, así que no hay falsos positivos.

Todo lo inyectado (qué `tx_id` lleva cada anomalía, cuáles se duplicaron y qué tarjetas son
sospechosas) se guarda en un **manifiesto** JSON: es la verdad conocida contra la que se
comprueba el sistema.

## 6. Reproducibilidad

El simulador recibe `--seed` y `--n-events` (nº de pagos **distintos**; los duplicados van
aparte, así que se envían `n_events + duplicados` mensajes). Con la misma semilla y los
mismos parámetros genera **exactamente los mismos eventos**, así que los resultados esperados
se pueden calcular de antemano. Por defecto simula del 26/10/2026 al 01/11/2026.
