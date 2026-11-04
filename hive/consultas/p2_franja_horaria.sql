-- P2 · ¿A qué horas se paga más, y qué parte es online? (hora de Madrid)
-- Cuenta todos los intentos válidos, aprobados o denegados.
SELECT hora_madrid                                               AS hora,
       COUNT(*)                                                  AS n_pagos,
       SUM(CASE WHEN channel = 'ONLINE' THEN 1 ELSE 0 END)       AS n_online,
       ROUND(CAST(SUM(CASE WHEN channel = 'ONLINE' THEN 1 ELSE 0 END) AS DECIMAL(12,2))
             * 100 / COUNT(*), 1)                                AS pct_online
FROM pagos.pagos_limpios
GROUP BY hora_madrid
ORDER BY hora;
