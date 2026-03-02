-- Customers
CREATE TABLE IF NOT EXISTS silver_customers (
    customer_id             VARCHAR(50) PRIMARY KEY,
    customer_unique_id      VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city           VARCHAR(100),
    customer_state          VARCHAR(5)
);

-- Orders
CREATE TABLE IF NOT EXISTS silver_orders (
    order_id                        VARCHAR(50) PRIMARY KEY,
    customer_id                     VARCHAR(50),
    order_status                    VARCHAR(20),
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP
);

-- Order Items
CREATE TABLE IF NOT EXISTS silver_order_items (
    order_id                VARCHAR(50),
    order_item_id           INTEGER,
    product_id              VARCHAR(50),
    seller_id               VARCHAR(50),
    shipping_limit_date     TIMESTAMP,
    price                   NUMERIC(10,2),
    freight_value           NUMERIC(10,2),
    PRIMARY KEY (order_id, order_item_id)
);

-- Payments
CREATE TABLE IF NOT EXISTS silver_order_payments (
    order_id                VARCHAR(50),
    payment_sequential      INTEGER,
    payment_type            VARCHAR(20),
    payment_installments    INTEGER,
    payment_value           NUMERIC(10,2),
    PRIMARY KEY (order_id, payment_sequential)
);

-- Reviews
CREATE TABLE IF NOT EXISTS silver_order_reviews (
    review_id                   VARCHAR(50) PRIMARY KEY,
    order_id                    VARCHAR(50),
    review_score                INTEGER,
    review_comment_title        TEXT,
    review_comment_message      TEXT,
    review_creation_date        TIMESTAMP,
    review_answer_timestamp     TIMESTAMP
);

-- Products
CREATE TABLE IF NOT EXISTS silver_products (
    product_id                      VARCHAR(50) PRIMARY KEY,
    product_category_name           VARCHAR(100),
    product_category_name_english   VARCHAR(100),
    product_name_lenght             INTEGER,
    product_description_lenght      INTEGER,
    product_photos_qty              INTEGER,
    product_weight_g                INTEGER,
    product_length_cm               INTEGER,
    product_height_cm               INTEGER,
    product_width_cm                INTEGER
);

-- Sellers
CREATE TABLE IF NOT EXISTS silver_sellers (
    seller_id               VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix  VARCHAR(10),
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(5)
);

-- Temp tables for upsert
CREATE TABLE IF NOT EXISTS silver_customers_temp       (LIKE silver_customers);
CREATE TABLE IF NOT EXISTS silver_orders_temp          (LIKE silver_orders);
CREATE TABLE IF NOT EXISTS silver_order_items_temp     (LIKE silver_order_items);
CREATE TABLE IF NOT EXISTS silver_order_payments_temp  (LIKE silver_order_payments);
CREATE TABLE IF NOT EXISTS silver_order_reviews_temp   (LIKE silver_order_reviews);
CREATE TABLE IF NOT EXISTS silver_products_temp        (LIKE silver_products);
CREATE TABLE IF NOT EXISTS silver_sellers_temp         (LIKE silver_sellers);