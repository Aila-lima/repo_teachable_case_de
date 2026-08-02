CREATE TABLE "purchase" (
  "purchase_id" bigint,
  "buyer_id" bigint,
  "prod_item_id" bigint,
  "order_date" date,
  "release_date" date,
  "producer_id" bigint,
  "purchase_partition" bigint,
  "prod_item_partition" bigint,
  "purchase_total_value" float,
  "purchase_status" string,
  "transaction_datetime" datetime,
  "transaction_date" date
);

CREATE TABLE "order_transaction_cost_hist" (
  "purchase_id" bigint,
  "purchase_partition" bigint,
  "order_transaction_cost_vat_value" float,
  "order_transaction_cost_installment_value" float,
  "order_transaction_cost_date" date,
  "transaction_datetime" datetime,
  "transaction_date" date
);

CREATE TABLE "product_item" (
  "prod_item_id" bigint,
  "prod_item_partition" bigint,
  "product_id" bigint,
  "item_quantity" int,
  "purchase_value" float,
  "transaction_datetime" datetime,
  "transaction_date" date
);

CREATE TABLE "purchase_extra_info" (
  "purchase_id" bigint,
  "purchase_partition" bigint,
  "subsidiary" string,
  "transaction_datetime" datetime,
  "transaction_date" date
);

COMMENT ON COLUMN "purchase"."purchase_id" IS 'identificador da compra';

COMMENT ON COLUMN "purchase"."buyer_id" IS 'identificador do comprador';

COMMENT ON COLUMN "purchase"."prod_item_id" IS 'identificador do item de compra';

COMMENT ON COLUMN "purchase"."order_date" IS 'data do pedido de compra';

COMMENT ON COLUMN "purchase"."release_date" IS 'data de liberação da compra mediante a confirmação do pagamento';

COMMENT ON COLUMN "purchase"."producer_id" IS 'identificador do produtor';

COMMENT ON COLUMN "purchase"."purchase_partition" IS 'partição no lake para a compra';

COMMENT ON COLUMN "purchase"."prod_item_partition" IS 'partição no lake para o item de compra';

COMMENT ON COLUMN "purchase"."purchase_total_value" IS 'valor total da compra';

COMMENT ON COLUMN "purchase"."purchase_status" IS 'status da compra: INICIADA, APROVADA, CANCELADA, REEMBOLSADA';

COMMENT ON COLUMN "purchase"."transaction_datetime" IS 'momento de inserção do dado no lake';

COMMENT ON COLUMN "purchase"."transaction_date" IS 'data de inserção do dado no lake';

COMMENT ON COLUMN "order_transaction_cost_hist"."purchase_id" IS 'identificador da compra';

COMMENT ON COLUMN "order_transaction_cost_hist"."purchase_partition" IS 'partição no lake para a compra';

COMMENT ON COLUMN "order_transaction_cost_hist"."order_transaction_cost_vat_value" IS 'valor VAT referente a compra';

COMMENT ON COLUMN "order_transaction_cost_hist"."order_transaction_cost_installment_value" IS 'valor do parcelamento da compra';

COMMENT ON COLUMN "order_transaction_cost_hist"."order_transaction_cost_date" IS 'data da efetivação do parcelamento';

COMMENT ON COLUMN "order_transaction_cost_hist"."transaction_datetime" IS 'momento de inserção do dado no lake';

COMMENT ON COLUMN "order_transaction_cost_hist"."transaction_date" IS 'data de inserção do dado no lake';

COMMENT ON COLUMN "product_item"."prod_item_id" IS 'identificador do item de compra';

COMMENT ON COLUMN "product_item"."prod_item_partition" IS 'partição no lake para o item da compra';

COMMENT ON COLUMN "product_item"."product_id" IS 'identificador do produto';

COMMENT ON COLUMN "product_item"."item_quantity" IS 'quantidade comprada por item';

COMMENT ON COLUMN "product_item"."purchase_value" IS 'valor do item de compra';

COMMENT ON COLUMN "product_item"."transaction_datetime" IS 'momento de inserção do dado no lake';

COMMENT ON COLUMN "product_item"."transaction_date" IS 'data de inserção do dado no lake';

COMMENT ON COLUMN "purchase_extra_info"."purchase_id" IS 'identificador da compra';

COMMENT ON COLUMN "purchase_extra_info"."purchase_partition" IS 'partição no lake para a compra';

COMMENT ON COLUMN "purchase_extra_info"."subsidiary" IS 'Empresa que, embora controlada (dirigida) por outra, possui grande parte ou o total de suas ações';

COMMENT ON COLUMN "purchase_extra_info"."transaction_datetime" IS 'momento de inserção do dado no lake';

COMMENT ON COLUMN "purchase_extra_info"."transaction_date" IS 'data de inserção do dado no lake';

ALTER TABLE "product_item" ADD FOREIGN KEY ("prod_item_id") REFERENCES "purchase" ("prod_item_id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "product_item" ADD FOREIGN KEY ("prod_item_partition") REFERENCES "purchase" ("prod_item_partition") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "order_transaction_cost_hist" ADD FOREIGN KEY ("purchase_id") REFERENCES "purchase" ("purchase_id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "order_transaction_cost_hist" ADD FOREIGN KEY ("purchase_partition") REFERENCES "purchase" ("purchase_partition") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "purchase_extra_info" ADD FOREIGN KEY ("purchase_id") REFERENCES "purchase" ("purchase_id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "purchase_extra_info" ADD FOREIGN KEY ("purchase_partition") REFERENCES "purchase" ("purchase_partition") DEFERRABLE INITIALLY IMMEDIATE;
