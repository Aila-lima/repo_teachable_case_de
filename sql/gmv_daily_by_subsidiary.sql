-- ENTREGÁVEL 4 - GMV diário por subsidiary.
-- Uma tabela, sem joins, sem filtros: gmv_amount já é zero para compras que não
-- foram liberadas ou que foram canceladas/reembolsadas.
SELECT
    gmv_date            AS date,
    subsidiary,
    SUM(gmv_amount)     AS gmv,
    COUNT(*)            AS purchases
FROM vw_purchase_gmv_current
WHERE gmv_date IS NOT NULL
GROUP BY gmv_date, subsidiary
ORDER BY gmv_date, subsidiary;
