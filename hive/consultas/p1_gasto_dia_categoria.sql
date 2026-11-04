-- P1 · ¿Cuánto se gasta cada día en cada categoría de comercio?
-- Solo pagos APROBADOS (un pago denegado no es gasto). Día en hora de Madrid.
-- AVG sobre DECIMAL es exacto, ROUND redondea hacia arriba en el 5 (17,975 → 17,98).
SELECT fecha,
       merchant_category        AS categoria,
       COUNT(*)                 AS n_pagos,
       SUM(importe)             AS importe_total_eur,
       ROUND(AVG(importe), 2)   AS ticket_medio_eur
FROM pagos.pagos_limpios
WHERE status = 'APROBADO'
GROUP BY fecha, merchant_category
ORDER BY fecha, categoria;
