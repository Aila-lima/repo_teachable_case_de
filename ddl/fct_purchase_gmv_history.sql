CREATE TABLE fct_purchase_gmv_history (

    -- ---- Grão ------------------------------------------------------------
    purchase_id                 BIGINT          NOT NULL,
    version_number              INT             NOT NULL,   -- 1..N, monotônico por compra
    version_id                  STRING          NOT NULL,   -- sha256(purchase_id|version_number)
    version_type                STRING          NOT NULL,   -- INITIAL | LATE_ARRIVAL | RESTATEMENT

    -- ---- Tempo de negócio (valid time) -----------------------------------
    order_date                  DATE,
    release_date                DATE,                       -- pagamento capturado
    gmv_date                    DATE,                       -- = release_date (premissa A1)

    -- ---- Métrica ---------------------------------------------------------
    purchase_gross_value        DECIMAL(18,2),              -- valor monetário bruto
    gmv_amount                  DECIMAL(18,2)   NOT NULL,   -- 0 quando não elegível
    is_gmv_eligible             BOOLEAN         NOT NULL,
    gmv_ineligibility_reason    STRING,                     -- NOT_RELEASED | CANCELADA | REEMBOLSADA

    -- ---- Dimensões desnormalizadas (requisito 6: sem joins) --------------
    subsidiary                  STRING          NOT NULL,   -- 'UNKNOWN' até extra_info chegar
    purchase_status             STRING,
    buyer_id                    BIGINT,
    producer_id                 BIGINT,
    product_id                  BIGINT,
    item_quantity               INT,

    -- ---- Completude ------------------------------------------------------
    is_complete                 BOOLEAN         NOT NULL,
    missing_components          ARRAY<STRING>,              -- ex.: ['purchase_extra_info']

    -- ---- Linhagem e conciliação ------------------------------------------
    version_valid_from_ts       TIMESTAMP       NOT NULL,   -- evento de origem mais recente desta versão
    batch_id                    STRING          NOT NULL,
    processing_date             DATE            NOT NULL,   -- relógio real (difere em backfills)
    payload_hash                STRING          NOT NULL,   -- impressão digital do negócio; deduplica reenvios
    change_reason               ARRAY<STRING>,              -- ['RELEASE_DATE_CHANGED', ...]
    src_purchase_event_ts       TIMESTAMP,
    src_product_item_event_ts   TIMESTAMP,
    src_extra_info_event_ts     TIMESTAMP,
    inserted_at                 TIMESTAMP       NOT NULL,

    -- ---- Tempo de sistema / chave de partição ----------------------------
    transaction_date            DATE            NOT NULL
)
USING DELTA
PARTITIONED BY (transaction_date)
CLUSTER BY (purchase_id)
TBLPROPERTIES (
    'delta.appendOnly'                   = 'true',  -- garantido pelo engine, não por convenção
    'delta.autoOptimize.optimizeWrite'   = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 3650 days'
);
