import os
import logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, get_json_object, from_unixtime
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, LongType
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

DB_URL  = os.environ.get("DB_URL")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_PREFIX    = os.environ.get("TOPIC_PREFIX", "niwa")
CHECKPOINT_BASE = "/app/checkpoints"


def create_spark():
    return SparkSession.builder \
        .appName("OlistEcommerceStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.postgresql:postgresql:42.7.2") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()


def kafka_stream(spark, table_name):
    topic = f"{TOPIC_PREFIX}.public.{table_name}"
    logger.info(f"Subscribing to topic: {topic}")

    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 50000) \
        .load() \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(
            get_json_object(col("json_payload"), "$.payload").alias("payload")
        )


def micro_ts(c):
    return to_timestamp(from_unixtime(col(c).cast("long") / 1000000))


def get_pg_conn():
    return psycopg2.connect(
        host=DB_HOST, database=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


def upsert(batch_df, batch_id, temp_table, main_table, conflict_col, update_cols):
    if batch_df.count() == 0:
        return

    logger.info(f"[{main_table}] Batch {batch_id} - Rows: {batch_df.count()}")

    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", temp_table) \
            .option("user", DB_USER) \
            .option("password", DB_PASS) \
            .mode("overwrite") \
            .save()

        set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        sql = f"""
            INSERT INTO {main_table}
            SELECT * FROM {temp_table}
            ON CONFLICT ({conflict_col}) DO UPDATE SET {set_clause}
        """

        conn = get_pg_conn()
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        conn.close()

        logger.info(f"[{main_table}] Batch {batch_id} - Upserted successfully.")

    except Exception as e:
        logger.error(f"[{main_table}] Batch {batch_id} - Failed: {e}")
        raise


def refresh_gold_views(batch_id, triggered_by):
    logger.info(f"[GOLD] Refreshing views - triggered by: {triggered_by}, batch: {batch_id}")

    gold_views = [
        "gold_sales_monthly",
        "gold_sales_daily",
        "gold_customer_metrics",
        "gold_product_performance",
        "gold_seller_performance",
        "gold_delivery_analytics",
        "gold_payment_analytics",
    ]

    try:
        conn = get_pg_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            for view in gold_views:
                cur.execute(f"REFRESH MATERIALIZED VIEW {view}")
                logger.info(f"[GOLD] Refreshed: {view}")
        conn.close()
        logger.info(f"[GOLD] All views refreshed successfully.")
    except Exception as e:
        logger.error(f"[GOLD] Refresh failed (non-fatal): {e}")


def upsert_and_refresh_gold(batch_df, batch_id, temp_table, main_table,
                             conflict_col, update_cols):
    upsert(batch_df, batch_id, temp_table, main_table, conflict_col, update_cols)
    if batch_df.count() > 0:
        refresh_gold_views(batch_id, main_table)


customers_schema = StructType([
    StructField("customer_id",              StringType(), True),
    StructField("customer_unique_id",       StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city",            StringType(), True),
    StructField("customer_state",           StringType(), True),
])

orders_schema = StructType([
    StructField("order_id",                       StringType(), True),
    StructField("customer_id",                    StringType(), True),
    StructField("order_status",                   StringType(), True),
    StructField("order_purchase_timestamp",       LongType(),   True),
    StructField("order_approved_at",              LongType(),   True),
    StructField("order_delivered_carrier_date",   LongType(),   True),
    StructField("order_delivered_customer_date",  LongType(),   True),
    StructField("order_estimated_delivery_date",  LongType(),   True),
])

order_items_schema = StructType([
    StructField("order_id",            StringType(), True),
    StructField("order_item_id",       IntegerType(), True),
    StructField("product_id",          StringType(), True),
    StructField("seller_id",           StringType(), True),
    StructField("shipping_limit_date", LongType(),   True),
    StructField("price",               DoubleType(), True),
    StructField("freight_value",       DoubleType(), True),
])

payments_schema = StructType([
    StructField("order_id",             StringType(),  True),
    StructField("payment_sequential",   IntegerType(), True),
    StructField("payment_type",         StringType(),  True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value",        DoubleType(),  True),
])

reviews_schema = StructType([
    StructField("review_id",               StringType(),  True),
    StructField("order_id",               StringType(),  True),
    StructField("review_score",            IntegerType(), True),
    StructField("review_comment_title",    StringType(),  True),
    StructField("review_comment_message",  StringType(),  True),
    StructField("review_creation_date",    LongType(),    True),
    StructField("review_answer_timestamp", LongType(),    True),
])

products_schema = StructType([
    StructField("product_id",                 StringType(),  True),
    StructField("product_category_name",      StringType(),  True),
    StructField("product_name_lenght",        IntegerType(), True),
    StructField("product_description_lenght", IntegerType(), True),
    StructField("product_photos_qty",         IntegerType(), True),
    StructField("product_weight_g",           IntegerType(), True),
    StructField("product_length_cm",          IntegerType(), True),
    StructField("product_height_cm",          IntegerType(), True),
    StructField("product_width_cm",           IntegerType(), True),
])

sellers_schema = StructType([
    StructField("seller_id",              StringType(), True),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city",            StringType(), True),
    StructField("seller_state",           StringType(), True),
])


def process_customers(spark):
    df = kafka_stream(spark, "customers") \
        .select(from_json(col("payload"), customers_schema).alias("d")) \
        .select("d.*") \
        .filter(col("customer_id").isNotNull())

    return df.writeStream \
        .foreachBatch(lambda b, i: upsert(b, i,
            "silver_customers_temp", "silver_customers",
            "customer_id",
            ["customer_unique_id", "customer_zip_code_prefix",
             "customer_city", "customer_state"])) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/customers") \
        .trigger(processingTime="10 seconds") \
        .start()


def process_orders(spark):
    ts_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    df = kafka_stream(spark, "orders") \
        .select(from_json(col("payload"), orders_schema).alias("d")) \
        .select("d.*") \
        .filter(col("order_id").isNotNull())

    for c in ts_cols:
        df = df.withColumn(c, micro_ts(c))

    return df.writeStream \
        .foreachBatch(lambda b, i: upsert_and_refresh_gold(b, i,
            "silver_orders_temp", "silver_orders",
            "order_id",
            ["customer_id", "order_status", "order_purchase_timestamp",
             "order_approved_at", "order_delivered_carrier_date",
             "order_delivered_customer_date", "order_estimated_delivery_date"])) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/orders") \
        .trigger(processingTime="10 seconds") \
        .start()


def process_order_items(spark):
    df = kafka_stream(spark, "order_items") \
        .select(from_json(col("payload"), order_items_schema).alias("d")) \
        .select("d.*") \
        .filter(col("order_id").isNotNull()) \
        .withColumn("shipping_limit_date", micro_ts("shipping_limit_date"))

    return df.writeStream \
        .foreachBatch(lambda b, i: upsert(b, i,
            "silver_order_items_temp", "silver_order_items",
            "order_id, order_item_id",
            ["product_id", "seller_id", "shipping_limit_date",
             "price", "freight_value"])) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/order_items") \
        .trigger(processingTime="10 seconds") \
        .start()


def process_payments(spark):
    df = kafka_stream(spark, "order_payments") \
        .select(from_json(col("payload"), payments_schema).alias("d")) \
        .select("d.*") \
        .filter(col("order_id").isNotNull())

    return df.writeStream \
        .foreachBatch(lambda b, i: upsert_and_refresh_gold(b, i,
            "silver_order_payments_temp", "silver_order_payments",
            "order_id, payment_sequential",
            ["payment_type", "payment_installments", "payment_value"])) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/payments") \
        .trigger(processingTime="10 seconds") \
        .start()


def process_reviews(spark):
    df = kafka_stream(spark, "order_reviews") \
        .select(from_json(col("payload"), reviews_schema).alias("d")) \
        .select("d.*") \
        .filter(col("review_id").isNotNull()) \
        .withColumn("review_creation_date",    micro_ts("review_creation_date")) \
        .withColumn("review_answer_timestamp", micro_ts("review_answer_timestamp"))

    return df.writeStream \
        .foreachBatch(lambda b, i: upsert(b, i,
            "silver_order_reviews_temp", "silver_order_reviews",
            "review_id",
            ["order_id", "review_score", "review_comment_title",
             "review_comment_message", "review_creation_date",
             "review_answer_timestamp"])) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/reviews") \
        .trigger(processingTime="10 seconds") \
        .start()


def process_products(spark):
    df = kafka_stream(spark, "products") \
        .select(from_json(col("payload"), products_schema).alias("d")) \
        .select("d.*") \
        .filter(col("product_id").isNotNull())

    return df.writeStream \
        .foreachBatch(lambda b, i: upsert(b, i,
            "silver_products_temp", "silver_products",
            "product_id",
            ["product_category_name", "product_name_lenght",
             "product_description_lenght", "product_photos_qty",
             "product_weight_g", "product_length_cm",
             "product_height_cm", "product_width_cm"])) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/products") \
        .trigger(processingTime="10 seconds") \
        .start()


def process_sellers(spark):
    df = kafka_stream(spark, "sellers") \
        .select(from_json(col("payload"), sellers_schema).alias("d")) \
        .select("d.*") \
        .filter(col("seller_id").isNotNull())

    return df.writeStream \
        .foreachBatch(lambda b, i: upsert(b, i,
            "silver_sellers_temp", "silver_sellers",
            "seller_id",
            ["seller_zip_code_prefix", "seller_city", "seller_state"])) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/sellers") \
        .trigger(processingTime="10 seconds") \
        .start()

def main():
    logger.info("Starting OlistEcommerceStreaming application.")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    queries = [
        process_customers(spark),
        process_orders(spark),
        process_order_items(spark),
        process_payments(spark),
        process_reviews(spark),
        process_products(spark),
        process_sellers(spark),
    ]

    logger.info(f"All {len(queries)} streaming queries started.")

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()