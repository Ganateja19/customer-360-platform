-- ============================================================
-- Customer 360 — Analytics Queries
-- Amazon Redshift
-- ============================================================
-- Production-ready queries for BI dashboards and ad-hoc analysis.
-- Each query is optimized for the star schema with sort/dist keys.
-- ============================================================

SET search_path TO analytics;

-- ────────────────────────────────────────────────────────────
-- 1. CUSTOMER LIFETIME VALUE (CLV) — Top 50 Customers
-- ────────────────────────────────────────────────────────────
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name  AS full_name,
    c.customer_tier,
    c.total_transactions,
    c.total_spend                        AS lifetime_value,
    c.avg_order_value,
    c.first_purchase_date,
    c.last_purchase_date,
    c.tenure_days,
    c.days_since_last_purchase,
    ROUND(c.total_spend / NULLIF(c.tenure_days, 0) * 365, 2) AS annual_value
FROM analytics.dim_customer c
WHERE c.total_transactions > 0
ORDER BY c.total_spend DESC
LIMIT 50;


-- ────────────────────────────────────────────────────────────
-- 2. MONTHLY REVENUE TREND
-- ────────────────────────────────────────────────────────────
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.transaction_id)     AS total_orders,
    COUNT(DISTINCT f.customer_key)       AS unique_customers,
    SUM(f.quantity)                       AS total_units,
    ROUND(SUM(f.gross_amount), 2)        AS gross_revenue,
    ROUND(SUM(f.discount_amount), 2)     AS total_discounts,
    ROUND(SUM(f.net_amount), 2)          AS net_revenue,
    ROUND(AVG(f.net_amount), 2)          AS avg_order_value
FROM analytics.fact_sales f
JOIN analytics.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- ────────────────────────────────────────────────────────────
-- 3. PRODUCT CATEGORY PERFORMANCE
-- ────────────────────────────────────────────────────────────
SELECT
    p.category,
    p.subcategory,
    COUNT(DISTINCT f.transaction_id)     AS orders,
    SUM(f.quantity)                       AS units_sold,
    ROUND(SUM(f.net_amount), 2)          AS revenue,
    ROUND(AVG(f.net_amount), 2)          AS avg_order_value,
    COUNT(DISTINCT f.customer_key)       AS unique_buyers,
    ROUND(AVG(p.profit_margin), 2)       AS avg_margin_pct
FROM analytics.fact_sales f
JOIN analytics.dim_product p ON f.product_key = p.product_key
GROUP BY p.category, p.subcategory
ORDER BY revenue DESC;


-- ────────────────────────────────────────────────────────────
-- 4. CUSTOMER COHORT ANALYSIS (by registration month)
-- ────────────────────────────────────────────────────────────
WITH cohorts AS (
    SELECT
        c.customer_key,
        DATE_TRUNC('month', c.registration_date) AS cohort_month,
        DATE_TRUNC('month', f.date_key)           AS purchase_month
    FROM analytics.fact_sales f
    JOIN analytics.dim_customer c ON f.customer_key = c.customer_key
),
cohort_size AS (
    SELECT cohort_month, COUNT(DISTINCT customer_key) AS cohort_count
    FROM cohorts
    GROUP BY cohort_month
)
SELECT
    co.cohort_month,
    cs.cohort_count,
    DATEDIFF(month, co.cohort_month, co.purchase_month) AS months_since_signup,
    COUNT(DISTINCT co.customer_key)                      AS active_customers,
    ROUND(
        COUNT(DISTINCT co.customer_key)::DECIMAL / cs.cohort_count * 100, 2
    ) AS retention_pct
FROM cohorts co
JOIN cohort_size cs ON co.cohort_month = cs.cohort_month
GROUP BY co.cohort_month, cs.cohort_count,
         DATEDIFF(month, co.cohort_month, co.purchase_month)
ORDER BY co.cohort_month, months_since_signup;


-- ────────────────────────────────────────────────────────────
-- 5. CHANNEL ATTRIBUTION — Revenue by Channel
-- ────────────────────────────────────────────────────────────
SELECT
    f.channel,
    COUNT(DISTINCT f.transaction_id)     AS orders,
    COUNT(DISTINCT f.customer_key)       AS unique_customers,
    ROUND(SUM(f.net_amount), 2)          AS revenue,
    ROUND(AVG(f.net_amount), 2)          AS avg_order_value,
    ROUND(
        SUM(f.net_amount) / SUM(SUM(f.net_amount)) OVER () * 100, 2
    ) AS revenue_share_pct
FROM analytics.fact_sales f
GROUP BY f.channel
ORDER BY revenue DESC;


-- ────────────────────────────────────────────────────────────
-- 6. PURCHASE FREQUENCY DISTRIBUTION
-- ────────────────────────────────────────────────────────────
SELECT
    frequency_bucket,
    COUNT(*)                             AS customer_count,
    ROUND(AVG(total_spend), 2)           AS avg_spend,
    ROUND(AVG(avg_order_value), 2)       AS avg_aov
FROM (
    SELECT
        c.customer_key,
        c.total_transactions,
        c.total_spend,
        c.avg_order_value,
        CASE
            WHEN c.total_transactions = 1  THEN '1 purchase'
            WHEN c.total_transactions <= 3 THEN '2-3 purchases'
            WHEN c.total_transactions <= 5 THEN '4-5 purchases'
            WHEN c.total_transactions <= 10 THEN '6-10 purchases'
            ELSE '11+ purchases'
        END AS frequency_bucket
    FROM analytics.dim_customer c
    WHERE c.total_transactions > 0
)
GROUP BY frequency_bucket
ORDER BY MIN(total_transactions);


-- ────────────────────────────────────────────────────────────
-- 7. CLICKSTREAM FUNNEL ANALYSIS
-- ────────────────────────────────────────────────────────────
SELECT
    event_type,
    COUNT(*)                             AS total_events,
    COUNT(DISTINCT session_id)           AS unique_sessions,
    COUNT(DISTINCT customer_key)         AS unique_users
FROM analytics.fact_clickstream
WHERE date_key >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY event_type
ORDER BY
    CASE event_type
        WHEN 'page_view'       THEN 1
        WHEN 'search'          THEN 2
        WHEN 'add_to_cart'     THEN 3
        WHEN 'wishlist_add'    THEN 4
        WHEN 'remove_from_cart' THEN 5
        WHEN 'purchase'        THEN 6
    END;


-- ────────────────────────────────────────────────────────────
-- 8. PRODUCT AFFINITY — Frequently Bought Together
-- ────────────────────────────────────────────────────────────
WITH order_products AS (
    SELECT DISTINCT
        f.customer_key,
        f.date_key,
        p.product_name,
        p.category
    FROM analytics.fact_sales f
    JOIN analytics.dim_product p ON f.product_key = p.product_key
)
SELECT
    a.product_name  AS product_a,
    b.product_name  AS product_b,
    COUNT(*)        AS co_purchase_count
FROM order_products a
JOIN order_products b
    ON a.customer_key = b.customer_key
    AND a.date_key = b.date_key
    AND a.product_name < b.product_name
GROUP BY a.product_name, b.product_name
HAVING COUNT(*) >= 5
ORDER BY co_purchase_count DESC
LIMIT 20;


-- ────────────────────────────────────────────────────────────
-- 9. CUSTOMER CHURN RISK DASHBOARD
-- ────────────────────────────────────────────────────────────
SELECT
    c.churn_risk,
    c.customer_tier,
    COUNT(*)                             AS customers,
    ROUND(AVG(c.total_spend), 2)         AS avg_lifetime_spend,
    ROUND(AVG(c.days_since_last_purchase)) AS avg_days_inactive,
    ROUND(AVG(c.total_transactions), 1)  AS avg_transactions
FROM analytics.dim_customer c
WHERE c.total_transactions > 0
GROUP BY c.churn_risk, c.customer_tier
ORDER BY
    CASE c.churn_risk WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
    c.customer_tier;


-- ────────────────────────────────────────────────────────────
-- 10. WEEKDAY vs WEEKEND REVENUE COMPARISON
-- ────────────────────────────────────────────────────────────
SELECT
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS period,
    d.day_name,
    COUNT(DISTINCT f.transaction_id)     AS orders,
    ROUND(SUM(f.net_amount), 2)          AS revenue,
    ROUND(AVG(f.net_amount), 2)          AS avg_order_value
FROM analytics.fact_sales f
JOIN analytics.dim_date d ON f.date_key = d.date_key
GROUP BY d.is_weekend, d.day_name, d.day_of_week
ORDER BY d.day_of_week;
