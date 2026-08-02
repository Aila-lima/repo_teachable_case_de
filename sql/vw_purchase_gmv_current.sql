-- Estado vigente de cada compra.
-- Sem valid_to, sem flag is_current: a vigência é derivada na leitura, que é
-- precisamente o que mantém a tabela subjacente append-only.
CREATE OR REPLACE TEMP VIEW vw_purchase_gmv_current AS
SELECT * EXCEPT (version_rank)
FROM (
    SELECT
        h.*,
        ROW_NUMBER() OVER (
            PARTITION BY purchase_id
            ORDER BY version_number DESC
        ) AS version_rank
    FROM fct_purchase_gmv_history h
)
WHERE version_rank = 1;
