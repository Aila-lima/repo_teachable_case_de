-- A mesma métrica, como era conhecida num dia específico ({{as_of}}).
-- Rodar isto com duas datas diferentes é a primitiva de conciliação: o delta
-- entre elas é integralmente explicado por change_reason.
SELECT
    gmv_date            AS date,
    subsidiary,
    SUM(gmv_amount)     AS gmv,
    COUNT(*)            AS purchases
FROM (
    SELECT * EXCEPT (version_rank) FROM (
        SELECT h.*,
               ROW_NUMBER() OVER (PARTITION BY purchase_id ORDER BY version_number DESC) AS version_rank
        FROM fct_purchase_gmv_history h
        WHERE h.transaction_date <= DATE '{{as_of}}'
    ) WHERE version_rank = 1
)
WHERE gmv_date IS NOT NULL
GROUP BY gmv_date, subsidiary
ORDER BY gmv_date, subsidiary;
