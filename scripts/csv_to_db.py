import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     5432,
    "database": os.getenv("DB_NAME", "ecommerce_db"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASS", "password"),
}

CSV_DIR = "csvs"


TABLES = {
    "customers": {
        "file": "olist_customers_dataset.csv",
        "pk":   "customer_id",
        "ddl":  """
            CREATE TABLE IF NOT EXISTS customers (
                customer_id              VARCHAR(50) PRIMARY KEY,
                customer_unique_id       VARCHAR(50),
                customer_zip_code_prefix VARCHAR(10),
                customer_city            VARCHAR(100),
                customer_state           VARCHAR(5)
            )
        """,
    },
    "orders": {
        "file": "olist_orders_dataset.csv",
        "pk":   "order_id",
        "ddl":  """
            CREATE TABLE IF NOT EXISTS orders (
                order_id                       VARCHAR(50) PRIMARY KEY,
                customer_id                    VARCHAR(50),
                order_status                   VARCHAR(20),
                order_purchase_timestamp       TIMESTAMP,
                order_approved_at              TIMESTAMP,
                order_delivered_carrier_date   TIMESTAMP,
                order_delivered_customer_date  TIMESTAMP,
                order_estimated_delivery_date  TIMESTAMP
            )
        """,
        "timestamps": [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    },
    "order_items": {
        "file": "olist_order_items_dataset.csv",
        "pk":   "order_id, order_item_id",
        "ddl":  """
            CREATE TABLE IF NOT EXISTS order_items (
                order_id            VARCHAR(50),
                order_item_id       INTEGER,
                product_id          VARCHAR(50),
                seller_id           VARCHAR(50),
                shipping_limit_date TIMESTAMP,
                price               NUMERIC(10,2),
                freight_value       NUMERIC(10,2),
                PRIMARY KEY (order_id, order_item_id)
            )
        """,
        "timestamps": ["shipping_limit_date"],
    },
    "order_payments": {
        "file": "olist_order_payments_dataset.csv",
        "pk":   "order_id, payment_sequential",
        "ddl":  """
            CREATE TABLE IF NOT EXISTS order_payments (
                order_id             VARCHAR(50),
                payment_sequential   INTEGER,
                payment_type         VARCHAR(20),
                payment_installments INTEGER,
                payment_value        NUMERIC(10,2),
                PRIMARY KEY (order_id, payment_sequential)
            )
        """,
    },
    "order_reviews": {
        "file": "olist_order_reviews_dataset.csv",
        "pk":   "review_id",
        "ddl":  """
            CREATE TABLE IF NOT EXISTS order_reviews (
                review_id               VARCHAR(50) PRIMARY KEY,
                order_id                VARCHAR(50),
                review_score            INTEGER,
                review_comment_title    TEXT,
                review_comment_message  TEXT,
                review_creation_date    TIMESTAMP,
                review_answer_timestamp TIMESTAMP
            )
        """,
        "timestamps": ["review_creation_date", "review_answer_timestamp"],
    },
    "products": {
        "file": "olist_products_dataset.csv",
        "pk":   "product_id",
        "ddl":  """
            CREATE TABLE IF NOT EXISTS products (
                product_id                   VARCHAR(50) PRIMARY KEY,
                product_category_name        VARCHAR(100),
                product_name_lenght          INTEGER,
                product_description_lenght   INTEGER,
                product_photos_qty           INTEGER,
                product_weight_g             INTEGER,
                product_length_cm            INTEGER,
                product_height_cm            INTEGER,
                product_width_cm             INTEGER
            )
        """,
    },
    "sellers": {
        "file": "olist_sellers_dataset.csv",
        "pk":   "seller_id",
        "ddl":  """
            CREATE TABLE IF NOT EXISTS sellers (
                seller_id              VARCHAR(50) PRIMARY KEY,
                seller_zip_code_prefix VARCHAR(10),
                seller_city            VARCHAR(100),
                seller_state           VARCHAR(5)
            )
        """,
    },
    "geolocation": {
        "file": "olist_geolocation_dataset.csv",
        "pk":   "geolocation_zip_code_prefix",
        "ddl":  """
            CREATE TABLE IF NOT EXISTS geolocation (
                geolocation_zip_code_prefix VARCHAR(10) PRIMARY KEY,
                geolocation_lat             DOUBLE PRECISION,
                geolocation_lng             DOUBLE PRECISION,
                geolocation_city            VARCHAR(100),
                geolocation_state           VARCHAR(5)
            )
        """,
    },
    "category_translation": {
        "file": "product_category_name_translation.csv",
        "pk":   "product_category_name",
        "ddl":  """
            CREATE TABLE IF NOT EXISTS category_translation (
                product_category_name         VARCHAR(100) PRIMARY KEY,
                product_category_name_english VARCHAR(100)
            )
        """,
    },
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def clean_df(df: pd.DataFrame, timestamps: list = None) -> pd.DataFrame:
    df.columns = [c.strip().strip('"') for c in df.columns]
    df = df.replace("", None)
    df = df.where(pd.notnull(df), None)

    if timestamps:
        for col in timestamps:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                df[col] = df[col].astype(object).where(df[col].notna(), None)

    return df


def load_table(conn, table_name: str, config: dict):
    filepath = os.path.join(CSV_DIR, config["file"])

    if not os.path.exists(filepath):
        logger.warning(f"File not found, skipping: {filepath}")
        return

    logger.info(f"Loading {config['file']} -> {table_name}")

    df = pd.read_csv(filepath, dtype=str, keep_default_na=False)
    df = clean_df(df, config.get("timestamps"))

    # Remove duplicate rows based on PK columns
    pk_cols = [c.strip() for c in config["pk"].split(",")]
    before = len(df)
    df = df.drop_duplicates(subset=pk_cols)
    if len(df) < before:
        logger.info(f"  Dropped {before - len(df)} duplicate rows.")

    columns = list(df.columns)
    values  = [tuple(row) for row in df.itertuples(index=False, name=None)]

    conflict_cols   = config["pk"]
    non_pk_cols     = [c for c in columns if c not in pk_cols]
    update_clause   = ", ".join([f"{c} = EXCLUDED.{c}" for c in non_pk_cols])

    sql = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES %s
        ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clause}
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)

    conn.commit()
    logger.info(f"  Done. {len(df)} rows loaded into {table_name}.")


def main():
    logger.info("Connecting to PostgreSQL...")
    conn = get_connection()
    logger.info("Connected.")

    with conn.cursor() as cur:
        for table_name, config in TABLES.items():
            logger.info(f"Creating table if not exists: {table_name}")
            cur.execute(config["ddl"])
        conn.commit()

    logger.info("All tables ready. Starting CSV load...")

    for table_name, config in TABLES.items():
        try:
            load_table(conn, table_name, config)
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
            conn.rollback()

    conn.close()
    logger.info("All CSV files loaded successfully.")


if __name__ == "__main__":
    main()