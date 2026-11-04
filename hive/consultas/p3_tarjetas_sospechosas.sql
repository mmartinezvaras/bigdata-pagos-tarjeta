-- P3 · ¿Qué tarjetas han pagado en 2 o más países distintos el mismo día? (fraude)
-- Cuenta también los intentos denegados: un intento en otro país ya es una señal.
SELECT card_id,
       fecha,
       COUNT(DISTINCT country)                         AS n_paises,
       concat_ws(',', sort_array(collect_set(country))) AS paises,
       COUNT(*)                                        AS n_pagos
FROM pagos.pagos_limpios
GROUP BY card_id, fecha
HAVING COUNT(DISTINCT country) >= 2
ORDER BY fecha, card_id;
