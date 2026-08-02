SELECT
    gmv_date            AS date,
    subsidiary,
    SUM(gmv_amount)     AS gmv,
    COUNT(*)            AS purchases
FROM vw_purchase_gmv_current
WHERE gmv_date IS NOT NULL
GROUP BY gmv_date, subsidiary
ORDER BY gmv_date, subsidiary;
