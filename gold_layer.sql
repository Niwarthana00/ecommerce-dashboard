-- ============================================================
-- GOLD LAYER - FULL RECREATE (FIXED)
-- ============================================================

DROP MATERIALIZED VIEW IF EXISTS gold_executive_kpis              CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_review_sentiment            CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_repeat_purchase_analysis    CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_product_affinity            CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_demand_seasonality          CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_payment_risk_signals        CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_category_trends             CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_geographic_revenue          CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_delivery_sla_breach         CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_seller_health_score         CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_customer_clv                CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_cohort_retention            CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_rfm_analysis                CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_payment_analytics           CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_delivery_analytics          CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_seller_performance          CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_product_performance         CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_customer_metrics            CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_sales_daily                 CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold_sales_monthly               CASCADE;


-- ── Monthly Sales Summary ─────────────────────────────────────
CREATE MATERIALIZED VIEW gold_sales_monthly AS
SELECT
    DATE_TRUNC('month', o.order_purchase_timestamp)              AS month,
    COUNT(DISTINCT o.order_id)                                   AS total_orders,
    COUNT(DISTINCT o.customer_id)                                AS unique_customers,
    ROUND(SUM(p.payment_value)::NUMERIC, 2)                      AS total_revenue,
    ROUND(AVG(p.payment_value)::NUMERIC, 2)                      AS avg_order_value,
    COUNT(CASE WHEN o.order_status = 'delivered' THEN 1 END)     AS delivered_orders,
    COUNT(CASE WHEN o.order_status = 'canceled'  THEN 1 END)     AS canceled_orders
FROM silver_orders o
JOIN silver_order_payments p ON o.order_id = p.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY 1
ORDER BY 1;

CREATE UNIQUE INDEX idx_gold_sales_monthly_month
    ON gold_sales_monthly (month);


-- ── Daily Sales Summary ───────────────────────────────────────
CREATE MATERIALIZED VIEW gold_sales_daily AS
SELECT
    DATE_TRUNC('day', o.order_purchase_timestamp)                AS day,
    COUNT(DISTINCT o.order_id)                                   AS total_orders,
    COUNT(DISTINCT o.customer_id)                                AS unique_customers,
    ROUND(SUM(p.payment_value)::NUMERIC, 2)                      AS total_revenue,
    ROUND(AVG(p.payment_value)::NUMERIC, 2)                      AS avg_order_value
FROM silver_orders o
JOIN silver_order_payments p ON o.order_id = p.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY 1
ORDER BY 1;

CREATE UNIQUE INDEX idx_gold_sales_daily_day
    ON gold_sales_daily (day);


-- ── Customer Metrics ──────────────────────────────────────────
CREATE MATERIALIZED VIEW gold_customer_metrics AS
SELECT
    c.customer_unique_id,
    (ARRAY_AGG(c.customer_state ORDER BY o.order_purchase_timestamp DESC))[1] AS customer_state,
    (ARRAY_AGG(c.customer_city  ORDER BY o.order_purchase_timestamp DESC))[1] AS customer_city,
    COUNT(DISTINCT o.order_id)                                   AS total_orders,
    ROUND(SUM(p.payment_value)::NUMERIC, 2)                      AS total_spent,
    ROUND(AVG(p.payment_value)::NUMERIC, 2)                      AS avg_order_value,
    MIN(o.order_purchase_timestamp)                              AS first_order_date,
    MAX(o.order_purchase_timestamp)                              AS last_order_date,
    ROUND(AVG(r.review_score)::NUMERIC, 2)                       AS avg_review_score
FROM silver_customers c
JOIN silver_orders o          ON c.customer_id = o.customer_id
JOIN silver_order_payments p  ON o.order_id    = p.order_id
LEFT JOIN silver_order_reviews r ON o.order_id = r.order_id
GROUP BY c.customer_unique_id;

CREATE UNIQUE INDEX idx_gold_customer_metrics_uid
    ON gold_customer_metrics (customer_unique_id);


-- ── Product Performance ───────────────────────────────────────
-- FIX: product_category_name (silver_products has no _english column)
CREATE MATERIALIZED VIEW gold_product_performance AS
SELECT
    p.product_id,
    p.product_category_name,
    COUNT(DISTINCT oi.order_id)                                  AS total_orders,
    SUM(oi.order_item_id)                                        AS total_units_sold,
    ROUND(SUM(oi.price)::NUMERIC, 2)                             AS total_revenue,
    ROUND(AVG(oi.price)::NUMERIC, 2)                             AS avg_price,
    ROUND(AVG(oi.freight_value)::NUMERIC, 2)                     AS avg_freight,
    ROUND(AVG(r.review_score)::NUMERIC, 2)                       AS avg_review_score
FROM silver_products p
JOIN silver_order_items oi    ON p.product_id  = oi.product_id
JOIN silver_orders o          ON oi.order_id   = o.order_id
LEFT JOIN silver_order_reviews r ON o.order_id = r.order_id
GROUP BY p.product_id, p.product_category_name;

CREATE UNIQUE INDEX idx_gold_product_performance_pid
    ON gold_product_performance (product_id);


-- ── Seller Performance ────────────────────────────────────────
CREATE MATERIALIZED VIEW gold_seller_performance AS
SELECT
    s.seller_id,
    s.seller_state,
    s.seller_city,
    COUNT(DISTINCT oi.order_id)                                  AS total_orders,
    SUM(oi.order_item_id)                                        AS total_units_sold,
    ROUND(SUM(oi.price)::NUMERIC, 2)                             AS total_revenue,
    ROUND(AVG(oi.price)::NUMERIC, 2)                             AS avg_product_price,
    ROUND(AVG(r.review_score)::NUMERIC, 2)                       AS avg_review_score,
    COUNT(DISTINCT p.product_id)                                 AS unique_products
FROM silver_sellers s
JOIN silver_order_items oi    ON s.seller_id   = oi.seller_id
JOIN silver_orders o          ON oi.order_id   = o.order_id
LEFT JOIN silver_order_reviews r ON o.order_id = r.order_id
LEFT JOIN silver_products p   ON oi.product_id = p.product_id
GROUP BY s.seller_id, s.seller_state, s.seller_city;

CREATE UNIQUE INDEX idx_gold_seller_performance_sid
    ON gold_seller_performance (seller_id);


-- ── Delivery Analytics ────────────────────────────────────────
CREATE MATERIALIZED VIEW gold_delivery_analytics AS
SELECT
    DATE_TRUNC('month', o.order_purchase_timestamp)              AS month,
    o.order_status,
    COUNT(*)                                                     AS order_count,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date - o.order_purchase_timestamp
        )) / 86400
    )::NUMERIC, 2)                                               AS avg_delivery_days,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            o.order_estimated_delivery_date - o.order_delivered_customer_date
        )) / 86400
    )::NUMERIC, 2)                                               AS avg_days_early_late,
    COUNT(CASE
        WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date
        THEN 1 END)                                              AS on_time_deliveries,
    COUNT(CASE
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
        THEN 1 END)                                              AS late_deliveries
FROM silver_orders o
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;

CREATE UNIQUE INDEX idx_gold_delivery_analytics_month_status
    ON gold_delivery_analytics (month, order_status);


-- ── Payment Analytics ─────────────────────────────────────────
CREATE MATERIALIZED VIEW gold_payment_analytics AS
SELECT
    DATE_TRUNC('month', o.order_purchase_timestamp)              AS month,
    p.payment_type,
    COUNT(*)                                                     AS transaction_count,
    ROUND(SUM(p.payment_value)::NUMERIC, 2)                      AS total_value,
    ROUND(AVG(p.payment_value)::NUMERIC, 2)                      AS avg_value,
    ROUND(AVG(p.payment_installments)::NUMERIC, 2)               AS avg_installments,
    COUNT(CASE WHEN p.payment_installments > 1 THEN 1 END)       AS installment_transactions
FROM silver_order_payments p
JOIN silver_orders o ON p.order_id = o.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;

CREATE UNIQUE INDEX idx_gold_payment_analytics_month_type
    ON gold_payment_analytics (month, payment_type);


-- ── RFM Analysis ──────────────────────────────────────────────
CREATE MATERIALIZED VIEW gold_rfm_analysis AS
WITH rfm_base AS (
    SELECT
        c.customer_unique_id,
        MAX(o.order_purchase_timestamp)                          AS last_order_date,
        COUNT(DISTINCT o.order_id)                               AS frequency,
        ROUND(SUM(p.payment_value)::NUMERIC, 2)                  AS monetary,
        EXTRACT(DAY FROM (
            MAX(DATE_TRUNC('day', NOW())) -
            MAX(DATE_TRUNC('day', o.order_purchase_timestamp))
        ))                                                       AS recency_days
    FROM silver_customers c
    JOIN silver_orders o         ON c.customer_id  = o.customer_id
    JOIN silver_order_payments p ON o.order_id     = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY c.customer_unique_id
),
rfm_scores AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days DESC)               AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC)                   AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)                    AS m_score
    FROM rfm_base
),
rfm_segments AS (
    SELECT *,
        (r_score + f_score + m_score)                            AS rfm_total,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3                  THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score <= 2                  THEN 'New Customers'
            WHEN r_score >= 3 AND f_score <= 2 AND m_score >= 3 THEN 'Potential Loyalists'
            WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3 THEN 'At Risk'
            WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN 'Cannot Lose Them'
            WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2 THEN 'Lost'
            WHEN r_score <= 3 AND f_score <= 3 AND m_score <= 2 THEN 'Hibernating'
            ELSE 'About to Sleep'
        END AS segment
    FROM rfm_scores
)
SELECT
    customer_unique_id,
    last_order_date,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    rfm_total,
    segment
FROM rfm_segments;

CREATE UNIQUE INDEX idx_gold_rfm_uid
    ON gold_rfm_analysis (customer_unique_id);


-- ── Cohort Retention Analysis ─────────────────────────────────
CREATE MATERIALIZED VIEW gold_cohort_retention AS
WITH first_orders AS (
    SELECT
        c.customer_unique_id,
        DATE_TRUNC('month', MIN(o.order_purchase_timestamp))     AS cohort_month
    FROM silver_customers c
    JOIN silver_orders o ON c.customer_id = o.customer_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY c.customer_unique_id
),
order_months AS (
    SELECT
        c.customer_unique_id,
        DATE_TRUNC('month', o.order_purchase_timestamp)          AS order_month
    FROM silver_customers c
    JOIN silver_orders o ON c.customer_id = o.customer_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY
        c.customer_unique_id,
        DATE_TRUNC('month', o.order_purchase_timestamp)
),
cohort_data AS (
    SELECT
        f.cohort_month,
        om.order_month,
        (EXTRACT(YEAR  FROM om.order_month) - EXTRACT(YEAR  FROM f.cohort_month)) * 12 +
        (EXTRACT(MONTH FROM om.order_month) - EXTRACT(MONTH FROM f.cohort_month))
                                                                 AS period_number,
        COUNT(DISTINCT om.customer_unique_id)                    AS customers
    FROM first_orders f
    JOIN order_months om ON f.customer_unique_id = om.customer_unique_id
    GROUP BY 1, 2, 3
),
cohort_sizes AS (
    SELECT cohort_month, customers AS cohort_size
    FROM cohort_data
    WHERE period_number = 0
)
SELECT
    cd.cohort_month,
    cd.order_month,
    cd.period_number,
    cd.customers,
    cs.cohort_size,
    ROUND((cd.customers::NUMERIC / cs.cohort_size) * 100, 2)    AS retention_rate
FROM cohort_data cd
JOIN cohort_sizes cs ON cd.cohort_month = cs.cohort_month
ORDER BY cd.cohort_month, cd.period_number;

CREATE UNIQUE INDEX idx_gold_cohort_retention
    ON gold_cohort_retention (cohort_month, order_month);


-- ── Customer Lifetime Value ───────────────────────────────────
CREATE MATERIALIZED VIEW gold_customer_clv AS
WITH customer_stats AS (
    SELECT
        c.customer_unique_id,
        (ARRAY_AGG(c.customer_state ORDER BY o.order_purchase_timestamp DESC))[1] AS customer_state,
        (ARRAY_AGG(c.customer_city  ORDER BY o.order_purchase_timestamp DESC))[1] AS customer_city,
        COUNT(DISTINCT o.order_id)                               AS total_orders,
        ROUND(SUM(p.payment_value)::NUMERIC, 2)                  AS total_revenue,
        ROUND(AVG(p.payment_value)::NUMERIC, 2)                  AS avg_order_value,
        MIN(o.order_purchase_timestamp)                          AS first_order_date,
        MAX(o.order_purchase_timestamp)                          AS last_order_date,
        EXTRACT(DAY FROM (
            MAX(o.order_purchase_timestamp) - MIN(o.order_purchase_timestamp)
        )) + 1                                                   AS customer_lifespan_days,
        ROUND(AVG(r.review_score)::NUMERIC, 2)                   AS avg_review_score
    FROM silver_customers c
    JOIN silver_orders o         ON c.customer_id = o.customer_id
    JOIN silver_order_payments p ON o.order_id    = p.order_id
    LEFT JOIN silver_order_reviews r ON o.order_id = r.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY c.customer_unique_id
),
clv_calc AS (
    SELECT *,
        ROUND((total_orders::NUMERIC /
            NULLIF(customer_lifespan_days, 0) * 365), 4)        AS purchase_frequency_yearly,
        total_revenue                                            AS historical_clv,
        ROUND((avg_order_value *
            (total_orders::NUMERIC /
            NULLIF(customer_lifespan_days, 0) * 365) * 1), 2)   AS projected_clv_1yr,
        CASE
            WHEN total_revenue >= 1000 THEN 'Platinum'
            WHEN total_revenue >= 500  THEN 'Gold'
            WHEN total_revenue >= 200  THEN 'Silver'
            ELSE 'Bronze'
        END AS clv_tier
    FROM customer_stats
)
SELECT * FROM clv_calc;

CREATE UNIQUE INDEX idx_gold_clv_uid
    ON gold_customer_clv (customer_unique_id);


-- ── Seller Health Score ───────────────────────────────────────
CREATE MATERIALIZED VIEW gold_seller_health_score AS
WITH seller_base AS (
    SELECT
        s.seller_id,
        s.seller_state,
        s.seller_city,
        COUNT(DISTINCT oi.order_id)                              AS total_orders,
        ROUND(SUM(oi.price)::NUMERIC, 2)                         AS total_revenue,
        ROUND(AVG(r.review_score)::NUMERIC, 2)                   AS avg_review_score,
        COUNT(DISTINCT p.product_id)                             AS unique_products,
        COUNT(CASE
            WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date
            THEN 1 END)                                          AS on_time_count,
        COUNT(CASE
            WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
            THEN 1 END)                                          AS late_count,
        COUNT(CASE WHEN o.order_status = 'canceled' THEN 1 END)  AS canceled_count,
        ROUND(AVG(
            EXTRACT(EPOCH FROM (
                o.order_delivered_customer_date - o.order_purchase_timestamp
            )) / 86400
        )::NUMERIC, 2)                                           AS avg_delivery_days
    FROM silver_sellers s
    JOIN silver_order_items oi   ON s.seller_id  = oi.seller_id
    JOIN silver_orders o         ON oi.order_id  = o.order_id
    LEFT JOIN silver_order_reviews r ON o.order_id = r.order_id
    LEFT JOIN silver_products p  ON oi.product_id = p.product_id
    GROUP BY s.seller_id, s.seller_state, s.seller_city
),
scored AS (
    SELECT *,
        ROUND(on_time_count::NUMERIC /
            NULLIF(on_time_count + late_count, 0) * 100, 2)     AS on_time_rate,
        ROUND(canceled_count::NUMERIC /
            NULLIF(total_orders, 0) * 100, 2)                   AS cancel_rate,
        ROUND((
            COALESCE(avg_review_score, 3) / 5.0 * 40 +
            (COALESCE(on_time_count::NUMERIC /
                NULLIF(on_time_count + late_count, 0), 0.5)) * 40 +
            (1 - COALESCE(canceled_count::NUMERIC /
                NULLIF(total_orders, 0), 0)) * 20
        )::NUMERIC, 2)                                           AS health_score
    FROM seller_base
)
SELECT *,
    CASE
        WHEN health_score >= 80 THEN 'Excellent'
        WHEN health_score >= 60 THEN 'Good'
        WHEN health_score >= 40 THEN 'Average'
        WHEN health_score >= 20 THEN 'Poor'
        ELSE 'Critical'
    END AS health_grade
FROM scored;

CREATE UNIQUE INDEX idx_gold_seller_health_sid
    ON gold_seller_health_score (seller_id);


-- ── Delivery SLA Breach Analysis ──────────────────────────────
-- FIX: removed product_category_name_english (not in silver_products)
--      added unique index on (seller_id, seller_state, customer_state, category)
--      for REFRESH CONCURRENTLY support
CREATE MATERIALIZED VIEW gold_delivery_sla_breach AS
SELECT
    s.seller_id,
    s.seller_state,
    c.customer_state,
    COALESCE(p.product_category_name, 'Unknown')                 AS category,
    COUNT(*)                                                     AS total_deliveries,
    COUNT(CASE
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
        THEN 1 END)                                              AS sla_breaches,
    ROUND(COUNT(CASE
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
        THEN 1 END)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2)    AS breach_rate_pct,
    ROUND(AVG(CASE
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
        THEN EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date - o.order_estimated_delivery_date
        )) / 86400 END)::NUMERIC, 2)                             AS avg_breach_days,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date - o.order_purchase_timestamp
        )) / 86400
    )::NUMERIC, 2)                                               AS avg_total_delivery_days
FROM silver_orders o
JOIN silver_order_items oi   ON o.order_id     = oi.order_id
JOIN silver_sellers s        ON oi.seller_id   = s.seller_id
JOIN silver_customers c      ON o.customer_id  = c.customer_id
LEFT JOIN silver_products p  ON oi.product_id  = p.product_id
WHERE o.order_delivered_customer_date IS NOT NULL
  AND o.order_estimated_delivery_date IS NOT NULL
GROUP BY s.seller_id, s.seller_state, c.customer_state,
         COALESCE(p.product_category_name, 'Unknown');

-- FIX: unique index — REFRESH CONCURRENTLY requires this
CREATE UNIQUE INDEX idx_gold_sla_breach_unique
    ON gold_delivery_sla_breach (seller_id, seller_state, customer_state, category);
CREATE INDEX idx_gold_sla_breach_state
    ON gold_delivery_sla_breach (seller_state, customer_state);


-- ── Geographic Revenue Heatmap ────────────────────────────────
CREATE MATERIALIZED VIEW gold_geographic_revenue AS
SELECT
    c.customer_state,
    c.customer_city,
    COUNT(DISTINCT c.customer_unique_id)                         AS unique_customers,
    COUNT(DISTINCT o.order_id)                                   AS total_orders,
    ROUND(SUM(p.payment_value)::NUMERIC, 2)                      AS total_revenue,
    ROUND(AVG(p.payment_value)::NUMERIC, 2)                      AS avg_order_value,
    ROUND(AVG(r.review_score)::NUMERIC, 2)                       AS avg_review_score,
    COUNT(CASE
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
        THEN 1 END)                                              AS late_deliveries,
    ROUND(COUNT(CASE
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
        THEN 1 END)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2)    AS late_delivery_rate_pct
FROM silver_customers c
JOIN silver_orders o         ON c.customer_id = o.customer_id
JOIN silver_order_payments p ON o.order_id    = p.order_id
LEFT JOIN silver_order_reviews r ON o.order_id = r.order_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY c.customer_state, c.customer_city;

CREATE UNIQUE INDEX idx_gold_geo_revenue
    ON gold_geographic_revenue (customer_state, customer_city);


-- ── Category Performance Trends ───────────────────────────────
-- FIX: product_category_name_english → product_category_name
CREATE MATERIALIZED VIEW gold_category_trends AS
WITH monthly_category AS (
    SELECT
        DATE_TRUNC('month', o.order_purchase_timestamp)          AS month,
        COALESCE(p.product_category_name, 'Unknown')             AS category,
        COUNT(DISTINCT o.order_id)                               AS total_orders,
        SUM(oi.order_item_id)                                    AS units_sold,
        ROUND(SUM(oi.price)::NUMERIC, 2)                         AS revenue,
        ROUND(AVG(oi.price)::NUMERIC, 2)                         AS avg_price,
        ROUND(AVG(r.review_score)::NUMERIC, 2)                   AS avg_review_score
    FROM silver_order_items oi
    JOIN silver_orders o         ON oi.order_id   = o.order_id
    LEFT JOIN silver_products p  ON oi.product_id = p.product_id
    LEFT JOIN silver_order_reviews r ON o.order_id = r.order_id
    WHERE o.order_purchase_timestamp IS NOT NULL
    GROUP BY 1, 2
),
monthly_total AS (
    SELECT month, SUM(revenue) AS total_monthly_revenue
    FROM monthly_category
    GROUP BY month
)
SELECT
    mc.*,
    mt.total_monthly_revenue,
    ROUND(mc.revenue / NULLIF(mt.total_monthly_revenue, 0) * 100, 2) AS market_share_pct
FROM monthly_category mc
JOIN monthly_total mt ON mc.month = mt.month
ORDER BY mc.month DESC, mc.revenue DESC;

CREATE UNIQUE INDEX idx_gold_category_trends
    ON gold_category_trends (month, category);


-- ── Payment Behavior & Risk Signals ──────────────────────────
CREATE MATERIALIZED VIEW gold_payment_risk_signals AS
SELECT
    o.order_id,
    c.customer_unique_id,
    c.customer_state,
    COUNT(p.payment_sequential)                                  AS payment_parts,
    ROUND(SUM(p.payment_value)::NUMERIC, 2)                      AS total_paid,
    MAX(p.payment_installments)                                  AS max_installments,
    COUNT(DISTINCT p.payment_type)                               AS payment_types_used,
    CASE WHEN MAX(p.payment_installments) >= 12 THEN TRUE
         ELSE FALSE END                                          AS high_installments_flag,
    CASE WHEN COUNT(DISTINCT p.payment_type) > 1 THEN TRUE
         ELSE FALSE END                                          AS mixed_payment_flag,
    CASE WHEN COUNT(p.payment_sequential) > 3 THEN TRUE
         ELSE FALSE END                                          AS split_payment_flag,
    CASE WHEN SUM(p.payment_value) > 5000 THEN TRUE
         ELSE FALSE END                                          AS high_value_flag,
    (CASE WHEN MAX(p.payment_installments) >= 12 THEN 1 ELSE 0 END +
     CASE WHEN COUNT(DISTINCT p.payment_type) > 1 THEN 1 ELSE 0 END +
     CASE WHEN COUNT(p.payment_sequential) > 3 THEN 1 ELSE 0 END +
     CASE WHEN SUM(p.payment_value) > 5000 THEN 1 ELSE 0 END)  AS risk_score,
    o.order_purchase_timestamp,
    o.order_status
FROM silver_order_payments p
JOIN silver_orders o    ON p.order_id    = o.order_id
JOIN silver_customers c ON o.customer_id = c.customer_id
GROUP BY o.order_id, c.customer_unique_id, c.customer_state,
         o.order_purchase_timestamp, o.order_status;

CREATE UNIQUE INDEX idx_gold_payment_risk_oid
    ON gold_payment_risk_signals (order_id);
CREATE INDEX idx_gold_payment_risk_score
    ON gold_payment_risk_signals (risk_score DESC);


-- ── Demand Seasonality Analysis ───────────────────────────────
-- FIX: added unique index on full GROUP BY key for REFRESH CONCURRENTLY
CREATE MATERIALIZED VIEW gold_demand_seasonality AS
SELECT
    EXTRACT(YEAR  FROM o.order_purchase_timestamp)               AS year,
    EXTRACT(MONTH FROM o.order_purchase_timestamp)               AS month_num,
    TO_CHAR(o.order_purchase_timestamp, 'Month')                 AS month_name,
    EXTRACT(DOW   FROM o.order_purchase_timestamp)               AS day_of_week_num,
    TO_CHAR(o.order_purchase_timestamp, 'Day')                   AS day_of_week_name,
    EXTRACT(HOUR  FROM o.order_purchase_timestamp)               AS hour_of_day,
    COUNT(DISTINCT o.order_id)                                   AS total_orders,
    ROUND(SUM(p.payment_value)::NUMERIC, 2)                      AS total_revenue,
    ROUND(AVG(p.payment_value)::NUMERIC, 2)                      AS avg_order_value,
    COUNT(DISTINCT o.customer_id)                                AS unique_customers
FROM silver_orders o
JOIN silver_order_payments p ON o.order_id = p.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 4;

-- FIX: unique index on all GROUP BY columns for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX idx_gold_seasonality_unique
    ON gold_demand_seasonality (year, month_num, day_of_week_num, hour_of_day);
CREATE INDEX idx_gold_seasonality_year_month
    ON gold_demand_seasonality (year, month_num);


-- ── Product Basket / Cross-sell Analysis ─────────────────────
-- FIX: product_category_name_english → product_category_name
--      added unique index for REFRESH CONCURRENTLY
CREATE MATERIALIZED VIEW gold_product_affinity AS
WITH order_categories AS (
    SELECT
        oi.order_id,
        COALESCE(p.product_category_name, 'Unknown')             AS category
    FROM silver_order_items oi
    LEFT JOIN silver_products p ON oi.product_id = p.product_id
    GROUP BY oi.order_id, COALESCE(p.product_category_name, 'Unknown')
),
category_pairs AS (
    SELECT
        a.category  AS category_a,
        b.category  AS category_b,
        COUNT(*)    AS co_occurrence_count
    FROM order_categories a
    JOIN order_categories b
        ON a.order_id = b.order_id
        AND a.category < b.category
    GROUP BY a.category, b.category
    HAVING COUNT(*) >= 5
)
SELECT
    category_a,
    category_b,
    co_occurrence_count,
    RANK() OVER (
        PARTITION BY category_a
        ORDER BY co_occurrence_count DESC
    ) AS affinity_rank
FROM category_pairs
ORDER BY co_occurrence_count DESC;

-- FIX: unique index for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX idx_gold_affinity_unique
    ON gold_product_affinity (category_a, category_b);
CREATE INDEX idx_gold_affinity_cat_a
    ON gold_product_affinity (category_a);


-- ── Repeat Purchase Analysis ──────────────────────────────────
CREATE MATERIALIZED VIEW gold_repeat_purchase_analysis AS
WITH customer_order_seq AS (
    SELECT
        c.customer_unique_id,
        o.order_id,
        o.order_purchase_timestamp,
        DATE_TRUNC('month', o.order_purchase_timestamp)          AS order_month,
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_unique_id
            ORDER BY o.order_purchase_timestamp
        )                                                        AS order_sequence
    FROM silver_customers c
    JOIN silver_orders o ON c.customer_id = o.customer_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
)
SELECT
    order_month,
    COUNT(CASE WHEN order_sequence = 1 THEN 1 END)               AS new_customers,
    COUNT(CASE WHEN order_sequence > 1 THEN 1 END)               AS returning_customers,
    COUNT(*)                                                     AS total_orders,
    ROUND(COUNT(CASE WHEN order_sequence > 1 THEN 1 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2)                          AS repeat_rate_pct
FROM customer_order_seq
GROUP BY order_month
ORDER BY order_month;

CREATE UNIQUE INDEX idx_gold_repeat_purchase_month
    ON gold_repeat_purchase_analysis (order_month);


-- ── Review Sentiment Summary ──────────────────────────────────
-- FIX: product_category_name_english → product_category_name
--      added unique index for REFRESH CONCURRENTLY
CREATE MATERIALIZED VIEW gold_review_sentiment AS
SELECT
    COALESCE(p.product_category_name, 'Unknown')                 AS category,
    s.seller_state,
    COUNT(r.review_id)                                           AS total_reviews,
    ROUND(AVG(r.review_score)::NUMERIC, 2)                       AS avg_score,
    COUNT(CASE WHEN r.review_score = 5 THEN 1 END)               AS score_5_count,
    COUNT(CASE WHEN r.review_score = 4 THEN 1 END)               AS score_4_count,
    COUNT(CASE WHEN r.review_score = 3 THEN 1 END)               AS score_3_count,
    COUNT(CASE WHEN r.review_score = 2 THEN 1 END)               AS score_2_count,
    COUNT(CASE WHEN r.review_score = 1 THEN 1 END)               AS score_1_count,
    ROUND(COUNT(CASE WHEN r.review_score >= 4 THEN 1 END)::NUMERIC
        / NULLIF(COUNT(r.review_id), 0) * 100, 2)                AS positive_rate_pct,
    ROUND(COUNT(CASE WHEN r.review_score <= 2 THEN 1 END)::NUMERIC
        / NULLIF(COUNT(r.review_id), 0) * 100, 2)                AS negative_rate_pct,
    COUNT(CASE WHEN r.review_comment_message IS NOT NULL
               AND LENGTH(r.review_comment_message) > 10
               THEN 1 END)                                       AS reviews_with_comments,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            r.review_answer_timestamp - r.review_creation_date
        )) / 86400
    )::NUMERIC, 2)                                               AS avg_response_days
FROM silver_order_reviews r
JOIN silver_orders o         ON r.order_id    = o.order_id
JOIN silver_order_items oi   ON o.order_id    = oi.order_id
JOIN silver_sellers s        ON oi.seller_id  = s.seller_id
LEFT JOIN silver_products p  ON oi.product_id = p.product_id
GROUP BY COALESCE(p.product_category_name, 'Unknown'), s.seller_state;

-- FIX: unique index for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX idx_gold_review_sentiment_unique
    ON gold_review_sentiment (category, seller_state);
CREATE INDEX idx_gold_review_sentiment_cat
    ON gold_review_sentiment (category);


-- ── Executive KPI Summary ─────────────────────────────────────
-- NOTE: single-row view — no unique index needed, use regular REFRESH
CREATE MATERIALIZED VIEW gold_executive_kpis AS
SELECT
    ROUND(SUM(p.payment_value)::NUMERIC, 2)                      AS total_gmv,
    COUNT(DISTINCT o.order_id)                                   AS total_orders,
    COUNT(DISTINCT c.customer_unique_id)                         AS total_unique_customers,
    COUNT(DISTINCT s.seller_id)                                  AS total_active_sellers,
    ROUND(AVG(p.payment_value)::NUMERIC, 2)                      AS overall_avg_order_value,
    ROUND(COUNT(CASE
        WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date
        THEN 1 END)::NUMERIC /
        NULLIF(COUNT(CASE WHEN o.order_delivered_customer_date IS NOT NULL
            THEN 1 END), 0) * 100, 2)                            AS overall_on_time_rate_pct,
    ROUND(AVG(CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
        THEN EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date - o.order_purchase_timestamp
        )) / 86400 END)::NUMERIC, 2)                             AS overall_avg_delivery_days,
    ROUND(AVG(r.review_score)::NUMERIC, 2)                       AS overall_avg_review_score,
    ROUND(COUNT(CASE WHEN o.order_status = 'canceled' THEN 1 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2)                          AS overall_cancel_rate_pct,
    MIN(o.order_purchase_timestamp)                              AS data_start_date,
    MAX(o.order_purchase_timestamp)                              AS data_end_date,
    NOW()                                                        AS last_refreshed_at
FROM silver_orders o
JOIN silver_customers c      ON o.customer_id = c.customer_id
JOIN silver_order_payments p ON o.order_id    = p.order_id
JOIN silver_order_items oi   ON o.order_id    = oi.order_id
JOIN silver_sellers s        ON oi.seller_id  = s.seller_id
LEFT JOIN silver_order_reviews r ON o.order_id = r.order_id;

-- single-row view — no unique index, use non-concurrent refresh
CREATE UNIQUE INDEX idx_gold_executive_kpis_unique
    ON gold_executive_kpis ((1));


-- ============================================================
-- VERIFY
-- ============================================================
-- SELECT * FROM gold_sales_monthly            ORDER BY month DESC LIMIT 5;
-- SELECT * FROM gold_sales_daily              ORDER BY day DESC LIMIT 5;
-- SELECT * FROM gold_customer_metrics         ORDER BY total_spent DESC LIMIT 10;
-- SELECT * FROM gold_product_performance      ORDER BY total_revenue DESC LIMIT 10;
-- SELECT * FROM gold_seller_performance       ORDER BY total_revenue DESC LIMIT 10;
-- SELECT * FROM gold_delivery_analytics       ORDER BY month DESC LIMIT 10;
-- SELECT * FROM gold_payment_analytics        ORDER BY month DESC LIMIT 10;
-- SELECT * FROM gold_rfm_analysis             ORDER BY monetary DESC LIMIT 10;
-- SELECT * FROM gold_cohort_retention         ORDER BY cohort_month, period_number LIMIT 20;
-- SELECT * FROM gold_customer_clv             ORDER BY historical_clv DESC LIMIT 10;
-- SELECT * FROM gold_seller_health_score      ORDER BY health_score DESC LIMIT 10;
-- SELECT * FROM gold_delivery_sla_breach      ORDER BY breach_rate_pct DESC LIMIT 10;
-- SELECT * FROM gold_geographic_revenue       ORDER BY total_revenue DESC LIMIT 10;
-- SELECT * FROM gold_category_trends          ORDER BY month DESC, revenue DESC LIMIT 20;
-- SELECT * FROM gold_payment_risk_signals     WHERE risk_score >= 3 LIMIT 10;
-- SELECT * FROM gold_demand_seasonality       ORDER BY year, month_num LIMIT 20;
-- SELECT * FROM gold_product_affinity         ORDER BY co_occurrence_count DESC LIMIT 20;
-- SELECT * FROM gold_repeat_purchase_analysis ORDER BY order_month LIMIT 20;
-- SELECT * FROM gold_review_sentiment         ORDER BY avg_score ASC LIMIT 10;
-- SELECT * FROM gold_executive_kpis;


-- ============================================================
-- REFRESH ALL (run after each data load)
-- ============================================================
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_sales_monthly;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_sales_daily;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_customer_metrics;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_product_performance;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_seller_performance;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_delivery_analytics;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_payment_analytics;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_rfm_analysis;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_cohort_retention;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_customer_clv;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_seller_health_score;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_delivery_sla_breach;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_geographic_revenue;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_category_trends;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_payment_risk_signals;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_demand_seasonality;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_product_affinity;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_repeat_purchase_analysis;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_review_sentiment;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY gold_executive_kpis;