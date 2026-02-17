-- ============================================================
-- Staging Tables for Redshift COPY / MERGE Pattern
-- ============================================================
-- These tables mirror the target schema but without constraints.
-- Used as intermediate landing zone for upsert operations.
-- ============================================================

SET search_path TO analytics;

-- Staging: dim_customer
CREATE TABLE IF NOT EXISTS analytics.stg_dim_customer (LIKE analytics.dim_customer);

-- Staging: dim_product
CREATE TABLE IF NOT EXISTS analytics.stg_dim_product (LIKE analytics.dim_product);

-- Staging: dim_date
CREATE TABLE IF NOT EXISTS analytics.stg_dim_date (LIKE analytics.dim_date);

-- Staging: fact_sales
CREATE TABLE IF NOT EXISTS analytics.stg_fact_sales (LIKE analytics.fact_sales);

-- Staging: fact_clickstream
CREATE TABLE IF NOT EXISTS analytics.stg_fact_clickstream (LIKE analytics.fact_clickstream);


-- ============================================================
-- MERGE / Upsert Procedure: dim_customer
-- ============================================================
-- Usage: CALL analytics.upsert_dim_customer();
-- ============================================================

CREATE OR REPLACE PROCEDURE analytics.upsert_dim_customer()
AS $$
BEGIN
    -- Delete existing records that have updates
    DELETE FROM analytics.dim_customer
    USING analytics.stg_dim_customer
    WHERE analytics.dim_customer.customer_id = analytics.stg_dim_customer.customer_id;

    -- Insert all staging records
    INSERT INTO analytics.dim_customer
    SELECT * FROM analytics.stg_dim_customer;

    -- Clean up staging
    TRUNCATE TABLE analytics.stg_dim_customer;
END;
$$ LANGUAGE plpgsql;


-- ============================================================
-- MERGE / Upsert Procedure: dim_product
-- ============================================================

CREATE OR REPLACE PROCEDURE analytics.upsert_dim_product()
AS $$
BEGIN
    DELETE FROM analytics.dim_product
    USING analytics.stg_dim_product
    WHERE analytics.dim_product.product_id = analytics.stg_dim_product.product_id;

    INSERT INTO analytics.dim_product
    SELECT * FROM analytics.stg_dim_product;

    TRUNCATE TABLE analytics.stg_dim_product;
END;
$$ LANGUAGE plpgsql;


-- ============================================================
-- Incremental Load Procedure: fact_sales
-- ============================================================
-- Deletes existing data for the target month, then inserts.
-- ============================================================

CREATE OR REPLACE PROCEDURE analytics.load_fact_sales(
    p_year INTEGER,
    p_month INTEGER
)
AS $$
BEGIN
    -- Delete existing data for the target month
    DELETE FROM analytics.fact_sales
    WHERE EXTRACT(YEAR FROM date_key) = p_year
      AND EXTRACT(MONTH FROM date_key) = p_month;

    -- Insert from staging
    INSERT INTO analytics.fact_sales
    SELECT * FROM analytics.stg_fact_sales;

    TRUNCATE TABLE analytics.stg_fact_sales;
END;
$$ LANGUAGE plpgsql;
