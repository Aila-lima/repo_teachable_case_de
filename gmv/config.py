from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = Path(os.getenv("GMV_WAREHOUSE", str(PROJECT_ROOT / "warehouse")))

TABLE_FORMAT = os.getenv("GMV_TABLE_FORMAT", "parquet")

# --- Table names -----------------------------------------------------------
BRONZE_PURCHASE = "bronze_purchase"
BRONZE_PRODUCT_ITEM = "bronze_product_item"
BRONZE_EXTRA_INFO = "bronze_purchase_extra_info"

SILVER_PURCHASE = "silver_purchase_state"
SILVER_PRODUCT_ITEM = "silver_product_item_state"
SILVER_EXTRA_INFO = "silver_extra_info_state"

GOLD_HISTORY = "fct_purchase_gmv_history"

# --- Business rules --------------------------------------------------------
ITEM_JOIN_KEY = os.getenv("GMV_ITEM_JOIN_KEY", "purchase_id")  # purchase_id | prod_item_id

UNKNOWN_SUBSIDIARY = "UNKNOWN"
CANCELLED_STATUSES = ["CANCELADA", "REEMBOLSADA"]

PAYLOAD_FIELDS = [
    "order_date", "release_date", "gmv_date", "purchase_status",
    "buyer_id", "producer_id", "product_id", "item_quantity",
    "purchase_gross_value", "subsidiary", "is_gmv_eligible", "gmv_amount",
]

CHANGE_REASONS = {
    "release_date": "RELEASE_DATE_CHANGED",
    "order_date": "ORDER_DATE_CHANGED",
    "purchase_status": "STATUS_CHANGED",
    "purchase_gross_value": "VALUE_CORRECTED",
    "item_quantity": "QUANTITY_CORRECTED",
    "buyer_id": "BUYER_CHANGED",
    "producer_id": "PRODUCER_CHANGED",
    "product_id": "PRODUCT_CHANGED",
}
