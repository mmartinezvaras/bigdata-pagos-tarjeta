-- Pagos en cuarentena: motivo y origen en Kafka (los 50 primeros, ordenados por regla).
SELECT regla, tx_id, card_id, merchant_id, amount_eur, channel, event_ts,
       kafka_partition, kafka_offset
FROM pagos.pagos_cuarentena
ORDER BY regla, tx_id
LIMIT 50;
