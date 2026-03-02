import os
import pandas as pd
from confluent_kafka import Producer
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
BATCH_SIZE        = int(os.getenv("PRODUCER_BATCH_SIZE", "500"))
MAX_WORKERS       = int(os.getenv("PRODUCER_MAX_WORKERS", "7"))
MAX_RETRIES       = int(os.getenv("PRODUCER_MAX_RETRIES", "3"))

TOPICS = {
    "olist_customers_dataset.csv":       "customers_topic",
    "olist_orders_dataset.csv":          "orders_topic",
    "olist_order_items_dataset.csv":     "order_items_topic",
    "olist_order_payments_dataset.csv":  "payments_topic",
    "olist_order_reviews_dataset.csv":   "reviews_topic",
    "olist_products_dataset.csv":        "products_topic",
    "olist_sellers_dataset.csv":         "sellers_topic",
}

CSV_DIR = os.getenv("CSV_DIR", "csvs")


def make_producer() -> Producer:
    conf = {
        'bootstrap.servers':        KAFKA_BOOTSTRAP,
        'socket.timeout.ms':        10000,
        'message.timeout.ms':       30000,
        'batch.size':               65536,
        'linger.ms':                10, 
        'compression.type':         'lz4',
        'acks':                     'all', 
        'retries':                  5,
        'retry.backoff.ms':         500,
    }
    return Producer(conf)


def wait_for_kafka():
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP}
    while True:
        try:
            p = Producer(conf)
            p.list_topics(timeout=5)
            logger.info("Kafka is ready.")
            return
        except Exception as e:
            logger.warning(f"Kafka not ready, retrying in 5s... ({e})")
            time.sleep(5)


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed [{msg.topic()}]: {err}")


def stream_csv(filename: str, topic: str) -> dict:
    filepath = os.path.join(CSV_DIR, filename)

    try:
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        raise

    total      = len(df)
    succeeded  = 0
    failed     = 0
    producer   = make_producer()

    logger.info(f"Starting [{filename}] -> [{topic}] | Rows: {total}")

    for index, row in df.iterrows():
        data = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        key  = str(list(data.values())[0])

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                producer.produce(
                    topic,
                    key=key,
                    value=json.dumps(data, default=str),
                    callback=delivery_report
                )
                producer.poll(0)
                succeeded += 1
                break
            except BufferError:
                logger.warning(f"Buffer full [{filename}] row {index}, flushing...")
                producer.flush()
            except Exception as e:
                if attempt == MAX_RETRIES:
                    logger.error(f"Row {index} failed after {MAX_RETRIES} attempts: {e}")
                    failed += 1
                else:
                    time.sleep(0.5 * attempt)

        if (index + 1) % BATCH_SIZE == 0:
            producer.flush()
            logger.info(f"[{filename}] Progress: {index + 1}/{total} "
                        f"| {succeeded} | {failed}")

    producer.flush()
    logger.info(f"Done [{filename}] | Total: {total} | "
                f"Success: {succeeded} | Failed: {failed}")

    return {"filename": filename, "total": total,
            "succeeded": succeeded, "failed": failed}


def main():
    wait_for_kafka()

    logger.info(f"Starting parallel streaming with {MAX_WORKERS} workers...")
    start_time = time.time()

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(stream_csv, filename, topic): filename
            for filename, topic in TOPICS.items()
        }

        for future in as_completed(futures):
            filename = futures[future]
            try:
                stats = future.result()
                results.append(stats)
                logger.info(f"Completed: {filename}")
            except Exception as e:
                logger.error(f"Failed: {filename} -> {e}")

    elapsed = time.time() - start_time

    logger.info("=" * 50)
    logger.info(f"All streaming completed in {elapsed:.1f}s")
    total_rows = sum(r["total"] for r in results)
    total_ok   = sum(r["succeeded"] for r in results)
    total_fail = sum(r["failed"] for r in results)
    logger.info(f"Total rows: {total_rows} | {total_ok} | {total_fail}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()