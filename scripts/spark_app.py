import os
import logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

DB_URL = os.environ.get("DB_URL")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
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


def kafka_stream(spark, topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load() \
        .selectExpr("CAST(value AS STRING) as json_payload")


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

        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME,
                                user=DB_USER, password=DB_PASS)
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        conn.close()

        logger.info(f"[{main_table}] Batch {batch_id} - Upserted successfully.")

    except Exception as e:
        logger.error(f"[{main_table}] Batch {batch_id} - Failed: {e}")
        raise



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
    StructField("order_purchase_timestamp",       StringType(), True),
    StructField("order_approved_at",              StringType(), True),
    StructField("order_delivered_carrier_date",   StringType(), True),
    StructField("order_delivered_customer_date",  StringType(), True),
    StructField("order_estimated_delivery_date",  StringType(), True),
])

order_items_schema = StructType([
    StructField("order_id",            StringType(), True),
    StructField("order_item_id",       IntegerType(), True),
    StructField("product_id",          StringType(), True),
    StructField("seller_id",           StringType(), True),
    StructField("shipping_limit_date", StringType(), True),
    StructField("price",               DoubleType(),  True),
    StructField("freight_value",       DoubleType(),  True),
])

payments_schema = StructType([
    StructField("order_id",               StringType(), True),
    StructField("payment_sequential",     IntegerType(), True),
    StructField("payment_type",           StringType(), True),
    StructField("payment_installments",   IntegerType(), True),
    StructField("payment_value",          DoubleType(),  True),
])

reviews_schema = StructType([
    StructField("review_id",                StringType(), True),
    StructField("order_id",                 StringType(), True),
    StructField("review_score",             IntegerType(), True),
    StructField("review_comment_title",     StringType(), True),
    StructField("review_comment_message",   StringType(), True),
    StructField("review_creation_date",     StringType(), True),
    StructField("review_answer_timestamp",  StringType(), True),
])

products_schema = StructType([
    StructField("product_id",                   StringType(),  True),
    StructField("product_category_name",        StringType(),  True),
    StructField("product_name_lenght",          IntegerType(), True),
    StructField("product_description_lenght",   IntegerType(), True),
    StructField("product_photos_qty",           IntegerType(), True),
    StructField("product_weight_g",             IntegerType(), True),
    StructField("product_length_cm",            IntegerType(), True),
    StructField("product_height_cm",            IntegerType(), True),
    StructField("product_width_cm",             IntegerType(), True),
])

sellers_schema = StructType([
    StructField("seller_id",               StringType(), True),
    StructField("seller_zip_code_prefix",  StringType(), True),
    StructField("seller_city",             StringType(), True),
    StructField("seller_state",            StringType(), True),
])



def process_customers(spark):
    df = kafka_stream(spark, "customers_topic") \
        .select(from_json(col("json_payload"), customers_schema).alias("d")) \
        .select("d.*") \
        .filter(col("customer_id").isNotNull())

    return df.writeStream \
        .foreachBatch(lambda b, i: upsert(b, i,
            "silver_customers_temp", "silver_customers",
            "customer_id",
            ["customer_unique_id", "customer_zip_code_prefix",
             "customer_city", "customer_state"])) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/customers") \
        .trigger(processingTime="30 seconds") \
        .start()


def process_orders(spark):
    ts_cols = ["order_purchase_timestamp", "order_approved_at",
               "order_delivered_carrier_date", "order_delivered_customer_date",
               "order_estimated_delivery_date"]

    df = kafka_stream(spark, "orders_topic") \
        .select(from_json(col("json_payload"), orders_schema).alias("d")) \
        .select("d.*") \
        .filter(col("order_id").isNotNull())

    for c in ts_cols:
        df = df.withColumn(c, to_timestamp(col(c), "yyyy-MM-dd HH:mm:ss"))

    return df.writeStream \
        .foreachBatch(lambda b, i: upsert(b, i,
            "silver_orders_temp", "silver_orders",
            "order_id",
            ["customer_id", "order_status", "order_purchase_timestamp",
             "order_approved_at", "order_delivered_carrier_date",
             "order_delivered_customer_date", "order_estimated_delivery_date"])) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/orders") \
        .trigger(processingTime="30 seconds") \
        .start()


def process_order_items(spark):
    df = kafka_stream(spark, "order_items_topic") \
        .select(from_json(col("json_payload"), order_items_schema).alias("d")) \
        .select("d.*") \
        .filter(col("order_id").isNotNull()) \
        .withColumn("shipping_limit_date",
                    to_timestamp(col("shipping_limit_date"), "yyyy-MM-dd HH:mm:ss"))

    return df.writeStream \
        .foreachBatch(lambda b, i: upsert(b, i,
            "silver_order_items_temp", "silver_order_items",
            "order_id, order_item_id",
            ["product_id", "seller_id", "shipping_limit_date",
             "price", "freight_value"])) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/order_items") \
        .trigger(processingTime="30 seconds") \
        .start()


def process_payments(spark):
    df = kafka_stream(spark, "payments_topic") \
        .select(from_json(col("json_payload"), payments_schema).alias("d")) \
        .select("d.*") \
        .filter(col("order_id").isNotNull())

    return df.writeStream \
        .foreachBatch(lambda b, i: upsert(b, i,
            "silver_order_payments_temp", "silver_order_payments",
            "order_id, payment_sequential",
            ["payment_type", "payment_installments", "payment_value"])) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/payments") \
        .trigger(processingTime="30 seconds") \
        .start()


def process_reviews(spark):
    df = kafka_stream(spark, "reviews_topic") \
        .select(from_json(col("json_payload"), reviews_schema).alias("d")) \
        .select("d.*") \
        .filter(col("review_id").isNotNull()) \
        .withColumn("review_creation_date",
                    to_timestamp(col("review_creation_date"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("review_answer_timestamp",
                    to_timestamp(col("review_answer_timestamp"), "yyyy-MM-dd HH:mm:ss"))

    return df.writeStream \
        .foreachBatch(lambda b, i: upsert(b, i,
            "silver_order_reviews_temp", "silver_order_reviews",
            "review_id",
            ["order_id", "review_score", "review_comment_title",
             "review_comment_message", "review_creation_date",
             "review_answer_timestamp"])) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/reviews") \
        .trigger(processingTime="30 seconds") \
        .start()


def process_products(spark):
    df = kafka_stream(spark, "products_topic") \
        .select(from_json(col("json_payload"), products_schema).alias("d")) \
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
        .trigger(processingTime="30 seconds") \
        .start()


def process_sellers(spark):
    df = kafka_stream(spark, "sellers_topic") \
        .select(from_json(col("json_payload"), sellers_schema).alias("d")) \
        .select("d.*") \
        .filter(col("seller_id").isNotNull())

    return df.writeStream \
        .foreachBatch(lambda b, i: upsert(b, i,
            "silver_sellers_temp", "silver_sellers",
            "seller_id",
            ["seller_zip_code_prefix", "seller_city", "seller_state"])) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/sellers") \
        .trigger(processingTime="30 seconds") \
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