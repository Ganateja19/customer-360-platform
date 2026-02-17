-- ============================================================
-- Redshift Maintenance Scripts
-- ============================================================

SET search_path TO analytics;

-- ────────────────────────────────────────────────────────────
-- VACUUM: Reclaim space and re-sort rows
-- Run after large batch loads or deletes
-- ────────────────────────────────────────────────────────────

-- Full vacuum on fact tables (most impacted by incremental loads)
VACUUM FULL analytics.fact_sales;
VACUUM FULL analytics.fact_clickstream;

-- Sort-only vacuum on dimensions (less churn)
VACUUM SORT ONLY analytics.dim_customer;
VACUUM SORT ONLY analytics.dim_product;
VACUUM SORT ONLY analytics.dim_date;

-- ────────────────────────────────────────────────────────────
-- ANALYZE: Update query planner statistics
-- ────────────────────────────────────────────────────────────

ANALYZE analytics.fact_sales;
ANALYZE analytics.fact_clickstream;
ANALYZE analytics.dim_customer;
ANALYZE analytics.dim_product;
ANALYZE analytics.dim_date;
ANALYZE analytics.dim_channel;

-- ────────────────────────────────────────────────────────────
-- TABLE HEALTH CHECK: Identify tables needing maintenance
-- ────────────────────────────────────────────────────────────

-- Tables with high unsorted percentage
SELECT
    schema                     AS "Schema",
    "table"                    AS "Table",
    size                       AS "Size (MB)",
    tbl_rows                   AS "Rows",
    unsorted                   AS "Unsorted %",
    stats_off                  AS "Stats Stale %",
    pct_used                   AS "Disk Used %"
FROM svv_table_info
WHERE schema = 'analytics'
ORDER BY unsorted DESC;

-- ────────────────────────────────────────────────────────────
-- QUERY PERFORMANCE: Identify slow queries
-- ────────────────────────────────────────────────────────────

SELECT
    query,
    TRIM(querytxt)             AS query_text,
    starttime,
    endtime,
    DATEDIFF(second, starttime, endtime) AS duration_sec,
    aborted
FROM stl_query
WHERE database = 'customer360'
  AND starttime >= DATEADD(hour, -24, GETDATE())
  AND DATEDIFF(second, starttime, endtime) > 5
ORDER BY duration_sec DESC
LIMIT 20;

-- ────────────────────────────────────────────────────────────
-- DISK USAGE by Table
-- ────────────────────────────────────────────────────────────

SELECT
    schema                     AS "Schema",
    "table"                    AS "Table",
    size                       AS "Size (MB)",
    tbl_rows                   AS "Row Count",
    ROUND(size::DECIMAL / NULLIF(tbl_rows, 0) * 1000000, 2) AS "Bytes per Row"
FROM svv_table_info
WHERE schema = 'analytics'
ORDER BY size DESC;
