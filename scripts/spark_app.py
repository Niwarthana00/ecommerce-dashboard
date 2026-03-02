import os
import time
import logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp,
    get_json_object, from_unixtime
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, LongType
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


DB_URL           = os.environ.get("DB_URL")
DB_USER          = os.environ.get("DB_USER")
DB_PASS          = os.environ.get("DB_PASS")
DB_HOST          = os.environ.get("DB_HOST")
DB_NAME          = os.environ.get("DB_NAME")
SSL_MODE         = os.environ.get("SSL_MODE", "disable")
KAFKA_BOOTSTRAP  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_PREFIX     = os.environ.get("TOPIC_PREFIX", "ecommerce_db").lower()
CHECKPOINT_BASE  = "/app/checkpoints"

TOPICS = [
    f"{TOPIC_PREFIX}.public.customers",
    f"{TOPIC_PREFIX}.public.orders",
    f"{TOPIC_PREFIX}.public.order_items",
    f"{TOPIC_PREFIX}.public.order_payments",
    f"{TOPIC_PREFIX}.public.order_reviews",
    f"{TOPIC_PREFIX}.public.products",
    f"{TOPIC_PREFIX}.public.sellers",
]


def wait_for_kafka_topics(bootstrap_servers, topics, max_retries=30, delay=10):
    try:
        from kafka import KafkaAdminClient
        from kafka.errors import NoBrokersAvailable
    except ImportError:
        logger.warning("kafka-python not installed. Falling back to 30s sleep.")
        time.sleep(30)
        return

    logger.info(f"Waiting for {len(topics)} Kafka topics to be ready...")
    logger.info(f"Expected topics: {topics}")

    for attempt in range(1, max_retries + 1):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                request_timeout_ms=5000,
                connections_max_idle_ms=10000,
            )
            existing_topics = admin.list_topics()
            admin.close()

            missing = [t for t in topics if t not in existing_topics]

            if not missing:
                logger.info(f"✅ All {len(topics)} topics ready! (attempt {attempt})")
                return
            else:
                logger.warning(
                    f"[{attempt}/{max_retries}] Still missing topics: {missing} "
                    f"— retrying in {delay}s..."
                )

        except NoBrokersAvailable:
            logger.warning(
                f"[{attempt}/{max_retries}] Kafka broker not reachable yet "
                f"— retrying in {delay}s..."
            )
        except Exception as e:
            logger.warning(
                f"[{attempt}/{max_retries}] Unexpected error: {e} "
                f"— retrying in {delay}s..."
            )

        time.sleep(delay)

    raise RuntimeError(
        f"Kafka topics not ready after {max_retries} retries ({max_retries * delay}s). "
        f"Aborting application."
    )


def create_spark():
    return SparkSession.builder \
        .appName("OlistEcommerceStreaming") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.7.2"
        ) \
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
        .option("kafka.metadata.max.age.ms", "30000") \
        .option("minPartitions", "1") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(
            get_json_object(col("json_payload"), "$.payload").alias("payload")
        )


def micro_ts(c):
    return to_timestamp(from_unixtime(col(c).cast("long") / 1000000))


def get_pg_conn():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        sslmode=SSL_MODE 
    )

def upsert(batch_df, batch_id, temp_table, main_table, conflict_col, update_cols):
    if batch_df.count() == 0:
        logger.info(f"[{main_table}] Batch {batch_id} - Empty batch, skipping.")
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
    logger.info(f"[GOLD] Refreshing views — triggered by: {triggered_by}, batch: {batch_id}")

    concurrent_views = [
        "gold_sales_monthly",
        "gold_sales_daily",
        "gold_customer_metrics",
        "gold_product_performance",
        "gold_seller_performance",
        "gold_delivery_analytics",
        "gold_payment_analytics",
        "gold_rfm_analysis",
        "gold_cohort_retention",
        "gold_customer_clv",
        "gold_seller_health_score",
        "gold_delivery_sla_breach",
        "gold_geographic_revenue",
        "gold_category_trends",
        "gold_payment_risk_signals",
        "gold_demand_seasonality",
        "gold_product_affinity",
        "gold_repeat_purchase_analysis",
        "gold_review_sentiment",
    ]

    non_concurrent_views = [
        "gold_executive_kpis",
    ]

    try:
        conn = get_pg_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            for view in concurrent_views:
                cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
                logger.info(f"[GOLD] Refreshed (concurrent): {view}")
            for view in non_concurrent_views:
                cur.execute(f"REFRESH MATERIALIZED VIEW {view}")
                logger.info(f"[GOLD] Refreshed (non-concurrent): {view}")
        conn.close()
        logger.info(f"[GOLD] All views refreshed successfully.")
    except Exception as e:
        logger.error(f"[GOLD] Refresh failed (non-fatal): {e}")


def upsert_and_refresh_gold(batch_df, batch_id, temp_table, main_table,
                             conflict_col, update_cols):
    if batch_df.count() == 0:
        return
    upsert(batch_df, batch_id, temp_table, main_table, conflict_col, update_cols)
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
    StructField("order_id",            StringType(),  True),
    StructField("order_item_id",       IntegerType(), True),
    StructField("product_id",          StringType(),  True),
    StructField("seller_id",           StringType(),  True),
    StructField("shipping_limit_date", LongType(),    True),
    StructField("price",               DoubleType(),  True),
    StructField("freight_value",       DoubleType(),  True),
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
    StructField("order_id",                StringType(),  True),
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
        .foreachBatch(lambda b, i: upsert_and_refresh_gold(
            b, i,
            "silver_customers_temp",
            "silver_customers",
            "customer_id",
            ["customer_unique_id", "customer_zip_code_prefix",
             "customer_city", "customer_state"]
        )) \
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
        .foreachBatch(lambda b, i: upsert_and_refresh_gold(
            b, i,
            "silver_orders_temp",
            "silver_orders",
            "order_id",
            ["customer_id", "order_status", "order_purchase_timestamp",
             "order_approved_at", "order_delivered_carrier_date",
             "order_delivered_customer_date", "order_estimated_delivery_date"]
        )) \
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
        .foreachBatch(lambda b, i: upsert_and_refresh_gold(
            b, i,
            "silver_order_items_temp",
            "silver_order_items",
            "order_id, order_item_id",
            ["product_id", "seller_id", "shipping_limit_date",
             "price", "freight_value"]
        )) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/order_items") \
        .trigger(processingTime="10 seconds") \
        .start()


def process_payments(spark):
    df = kafka_stream(spark, "order_payments") \
        .select(from_json(col("payload"), payments_schema).alias("d")) \
        .select("d.*") \
        .filter(col("order_id").isNotNull())

    return df.writeStream \
        .foreachBatch(lambda b, i: upsert_and_refresh_gold(
            b, i,
            "silver_order_payments_temp",
            "silver_order_payments",
            "order_id, payment_sequential",
            ["payment_type", "payment_installments", "payment_value"]
        )) \
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
        .foreachBatch(lambda b, i: upsert_and_refresh_gold(
            b, i,
            "silver_order_reviews_temp",
            "silver_order_reviews",
            "review_id",
            ["order_id", "review_score", "review_comment_title",
             "review_comment_message", "review_creation_date",
             "review_answer_timestamp"]
        )) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/reviews") \
        .trigger(processingTime="10 seconds") \
        .start()


def process_products(spark):
    df = kafka_stream(spark, "products") \
        .select(from_json(col("payload"), products_schema).alias("d")) \
        .select("d.*") \
        .filter(col("product_id").isNotNull())

    return df.writeStream \
        .foreachBatch(lambda b, i: upsert_and_refresh_gold(
            b, i,
            "silver_products_temp",
            "silver_products",
            "product_id",
            ["product_category_name", "product_name_lenght",
             "product_description_lenght", "product_photos_qty",
             "product_weight_g", "product_length_cm",
             "product_height_cm", "product_width_cm"]
        )) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/products") \
        .trigger(processingTime="10 seconds") \
        .start()


def process_sellers(spark):
    df = kafka_stream(spark, "sellers") \
        .select(from_json(col("payload"), sellers_schema).alias("d")) \
        .select("d.*") \
        .filter(col("seller_id").isNotNull())

    return df.writeStream \
        .foreachBatch(lambda b, i: upsert_and_refresh_gold(
            b, i,
            "silver_sellers_temp",
            "silver_sellers",
            "seller_id",
            ["seller_zip_code_prefix", "seller_city", "seller_state"]
        )) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/sellers") \
        .trigger(processingTime="10 seconds") \
        .start()


def main():
    logger.info("=" * 60)
    logger.info("Starting OlistEcommerceStreaming application.")
    logger.info(f"Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
    logger.info(f"Topic Prefix:    {TOPIC_PREFIX}")
    logger.info(f"Target DB:       {DB_HOST}/{DB_NAME}")
    logger.info("=" * 60)

    wait_for_kafka_topics(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        topics=TOPICS,
        max_retries=30,   # 30 × 10s = 5 minutes max
        delay=10
    )

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

    logger.info(f"All {len(queries)} streaming queries started successfully.")

    spark.streams.awaitAnyTermination()

    for q in queries:
        if q.exception():
            logger.error(f"Query '{q.name}' failed with: {q.exception()}")

    logger.info("Application terminated.")


if __name__ == "__main__":
    main()