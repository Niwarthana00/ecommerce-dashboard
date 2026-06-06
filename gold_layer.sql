-- ── Monthly Sales Summary ─────────────────────────────
-- Dashboard: Revenue trend, order volume by month
CREATE MATERIALIZED VIEW IF NOT EXISTS gold_sales_monthly AS
SELECT
    DATE_TRUNC('month', o.order_purchase_timestamp)  AS month,
    COUNT(DISTINCT o.order_id)                        AS total_orders,
    COUNT(DISTINCT o.customer_id)                     AS unique_customers,
    ROUND(SUM(p.payment_value)::NUMERIC, 2)           AS total_revenue,
    ROUND(AVG(p.payment_value)::NUMERIC, 2)           AS avg_order_value,
    COUNT(CASE WHEN o.order_status = 'delivered' THEN 1 END) AS delivered_orders,
    COUNT(CASE WHEN o.order_status = 'canceled'  THEN 1 END) AS canceled_orders
FROM silver_orders o
JOIN silver_order_payments p ON o.order_id = p.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY 1
ORDER BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_sales_monthly_month
    ON gold_sales_monthly (month);


CREATE MATERIALIZED VIEW IF NOT EXISTS gold_sales_daily AS
SELECT
    DATE_TRUNC('day', o.order_purchase_timestamp)    AS day,
    COUNT(DISTINCT o.order_id)                        AS total_orders,
    COUNT(DISTINCT o.customer_id)                     AS unique_customers,
    ROUND(SUM(p.payment_value)::NUMERIC, 2)           AS total_revenue,
    ROUND(AVG(p.payment_value)::NUMERIC, 2)           AS avg_order_value
FROM silver_orders o
JOIN silver_order_payments p ON o.order_id = p.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY 1
ORDER BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_sales_daily_day
    ON gold_sales_daily (day);


-- ── Customer Metrics ──────────────────────────────────
-- Dashboard: Top customers, repeat buyers, spending patterns
CREATE MATERIALIZED VIEW IF NOT EXISTS gold_customer_metrics AS
SELECT
    c.customer_unique_id,
    c.customer_state,
    c.customer_city,
    COUNT(DISTINCT o.order_id)              AS total_orders,
    ROUND(SUM(p.payment_value)::NUMERIC, 2) AS total_spent,
    ROUND(AVG(p.payment_value)::NUMERIC, 2) AS avg_order_value,
    MIN(o.order_purchase_timestamp)         AS first_order_date,
    MAX(o.order_purchase_timestamp)         AS last_order_date,
    ROUND(AVG(r.review_score)::NUMERIC, 2)  AS avg_review_score
FROM silver_customers c
JOIN silver_orders o         ON c.customer_id       = o.customer_id
JOIN silver_order_payments p ON o.order_id          = p.order_id
LEFT JOIN silver_order_reviews r ON o.order_id      = r.order_id
GROUP BY c.customer_unique_id, c.customer_state, c.customer_city;

CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_customer_metrics_uid
    ON gold_customer_metrics (customer_unique_id);


-- ── Product Performance ───────────────────────────────
-- Dashboard: Best selling products, revenue per category
CREATE MATERIALIZED VIEW IF NOT EXISTS gold_product_performance AS
SELECT
    p.product_id,
    p.product_category_name,
    COUNT(DISTINCT oi.order_id)              AS total_orders,
    SUM(oi.order_item_id)                    AS total_units_sold,
    ROUND(SUM(oi.price)::NUMERIC, 2)         AS total_revenue,
    ROUND(AVG(oi.price)::NUMERIC, 2)         AS avg_price,
    ROUND(AVG(oi.freight_value)::NUMERIC, 2) AS avg_freight,
    ROUND(AVG(r.review_score)::NUMERIC, 2)   AS avg_review_score
FROM silver_products p
JOIN silver_order_items oi   ON p.product_id   = oi.product_id
JOIN silver_orders o         ON oi.order_id    = o.order_id
LEFT JOIN silver_order_reviews r ON o.order_id = r.order_id
GROUP BY p.product_id, p.product_category_name;

CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_product_performance_pid
    ON gold_product_performance (product_id);


-- ── Seller Performance ────────────────────────────────
-- Dashboard: Top sellers, revenue, delivery performance
CREATE MATERIALIZED VIEW IF NOT EXISTS gold_seller_performance AS
SELECT
    s.seller_id,
    s.seller_state,
    s.seller_city,
    COUNT(DISTINCT oi.order_id)              AS total_orders,
    SUM(oi.order_item_id)                    AS total_units_sold,
    ROUND(SUM(oi.price)::NUMERIC, 2)         AS total_revenue,
    ROUND(AVG(oi.price)::NUMERIC, 2)         AS avg_product_price,
    ROUND(AVG(r.review_score)::NUMERIC, 2)   AS avg_review_score,
    COUNT(DISTINCT p.product_id)             AS unique_products
FROM silver_sellers s
JOIN silver_order_items oi   ON s.seller_id    = oi.seller_id
JOIN silver_orders o         ON oi.order_id    = o.order_id
LEFT JOIN silver_order_reviews r ON o.order_id = r.order_id
LEFT JOIN silver_products p  ON oi.product_id  = p.product_id
GROUP BY s.seller_id, s.seller_state, s.seller_city;

CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_seller_performance_sid
    ON gold_seller_performance (seller_id);


-- ── Delivery Analytics ────────────────────────────────
-- Dashboard: On-time delivery rate, average delivery time
CREATE MATERIALIZED VIEW IF NOT EXISTS gold_delivery_analytics AS
SELECT
    DATE_TRUNC('month', o.order_purchase_timestamp)  AS month,
    o.order_status,
    COUNT(*)                                          AS order_count,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date - o.order_purchase_timestamp
        )) / 86400
    )::NUMERIC, 2)                                    AS avg_delivery_days,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            o.order_estimated_delivery_date - o.order_delivered_customer_date
        )) / 86400
    )::NUMERIC, 2)                                    AS avg_days_early_late,
    COUNT(CASE
        WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date
        THEN 1
    END)                                              AS on_time_deliveries,
    COUNT(CASE
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
        THEN 1
    END)                                              AS late_deliveries
FROM silver_orders o
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;

CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_delivery_analytics_month_status
    ON gold_delivery_analytics (month, order_status);


-- ── Payment Analytics ─────────────────────────────────
-- Dashboard: Payment method distribution, installment patterns
CREATE MATERIALIZED VIEW IF NOT EXISTS gold_payment_analytics AS
SELECT
    DATE_TRUNC('month', o.order_purchase_timestamp)  AS month,
    p.payment_type,
    COUNT(*)                                          AS transaction_count,
    ROUND(SUM(p.payment_value)::NUMERIC, 2)           AS total_value,
    ROUND(AVG(p.payment_value)::NUMERIC, 2)           AS avg_value,
    ROUND(AVG(p.payment_installments)::NUMERIC, 2)    AS avg_installments,
    COUNT(CASE WHEN p.payment_installments > 1 THEN 1 END) AS installment_transactions
FROM silver_order_payments p
JOIN silver_orders o ON p.order_id = o.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;

CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_payment_analytics_month_type
    ON gold_payment_analytics (month, payment_type);


-- ============================================================
-- VERIFY: Gold views data check
-- ============================================================
-- SELECT * FROM gold_sales_monthly        ORDER BY month DESC LIMIT 5;
-- SELECT * FROM gold_sales_daily          ORDER BY day DESC LIMIT 5;
-- SELECT * FROM gold_customer_metrics     ORDER BY total_spent DESC LIMIT 10;
-- SELECT * FROM gold_product_performance  ORDER BY total_revenue DESC LIMIT 10;
-- SELECT * FROM gold_seller_performance   ORDER BY total_revenue DESC LIMIT 10;
-- SELECT * FROM gold_delivery_analytics   ORDER BY month DESC LIMIT 10;
-- SELECT * FROM gold_payment_analytics    ORDER BY month DESC LIMIT 10;