ep-shy-bread-adc9l0yf-pooler.c-2.us-east-1.aws.neon.tech
5432
neondb
neondb_owner
npg_awcQzOX3rV2y





1. SELECT * FROM gold_executive_kpis
2. SELECT * FROM gold_sales_monthly
3. SELECT * FROM gold_sales_daily
4. SELECT * FROM gold_geographic_revenue
5. SELECT * FROM gold_rfm_analysis
6. SELECT * FROM gold_product_performance
7. SELECT * FROM gold_delivery_analytics
8. SELECT * FROM gold_payment_analytics
9. SELECT * FROM gold_seller_performance
10. SELECT * FROM gold_category_trends
11. SELECT * FROM gold_customer_metrics
12. SELECT * FROM gold_customer_clv
13. SELECT * FROM gold_cohort_retention
14. SELECT * FROM gold_seller_health_score
15. SELECT * FROM gold_delivery_sla_breach
16. SELECT * FROM gold_payment_risk_signals
17. SELECT * FROM gold_demand_seasonality
18. SELECT * FROM gold_product_affinity
19. SELECT * FROM gold_repeat_purchase_analysis
20. SELECT * FROM gold_review_sentiment



SELECT 
    n.nspname AS schema_name,
    c.relname AS materialized_view_name,
    a.attname AS column_name
FROM 
    pg_attribute a
JOIN 
    pg_class c ON a.attrelid = c.oid
JOIN 
    pg_namespace n ON c.relnamespace = n.oid
WHERE 
    c.relkind = 'm'  -- 'm'
    AND n.nspname = 'public'
    AND a.attname = 'total_gmv'
    AND a.attnum > 0 
    AND NOT a.attisdropped;