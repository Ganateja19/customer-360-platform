-- ============================================================
-- Customer 360 Data Warehouse — Star Schema DDL
-- Amazon Redshift
-- ============================================================
-- Design Decisions:
--   • Distribution: KEY on high-cardinality join columns
--   • Sort keys: Optimize for common WHERE/JOIN patterns
--   • Compression: AZ64 for numeric, ZSTD for strings
-- ============================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path TO analytics;

-- ────────────────────────────────────────────────────────────
-- DIMENSION: dim_date
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key            DATE            NOT NULL  ENCODE RAW,
    year                SMALLINT        NOT NULL  ENCODE AZ64,
    quarter             SMALLINT        NOT NULL  ENCODE AZ64,
    month               SMALLINT        NOT NULL  ENCODE AZ64,
    month_name          VARCHAR(20)     NOT NULL  ENCODE ZSTD,
    week_of_year        SMALLINT        NOT NULL  ENCODE AZ64,
    day_of_month        SMALLINT        NOT NULL  ENCODE AZ64,
    day_of_week         SMALLINT        NOT NULL  ENCODE AZ64,
    day_name            VARCHAR(20)     NOT NULL  ENCODE ZSTD,
    is_weekend          BOOLEAN         NOT NULL  ENCODE RAW,
    is_month_end        BOOLEAN         NOT NULL  ENCODE RAW,
    fiscal_year         SMALLINT        NOT NULL  ENCODE AZ64,
    fiscal_quarter      SMALLINT        NOT NULL  ENCODE AZ64,

    PRIMARY KEY (date_key)
)
DISTSTYLE ALL        -- Small table, replicate to all nodes
SORTKEY (date_key);


-- ────────────────────────────────────────────────────────────
-- DIMENSION: dim_customer
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_key             BIGINT         NOT NULL  ENCODE AZ64,
    customer_id              VARCHAR(20)    NOT NULL  ENCODE ZSTD,
    first_name               VARCHAR(100)   ENCODE ZSTD,
    last_name                VARCHAR(100)   ENCODE ZSTD,
    email                    VARCHAR(255)   ENCODE ZSTD,
    phone                    VARCHAR(30)    ENCODE ZSTD,
    date_of_birth            DATE           ENCODE AZ64,
    gender                   VARCHAR(10)    ENCODE ZSTD,
    address_street           VARCHAR(255)   ENCODE ZSTD,
    address_city             VARCHAR(100)   ENCODE ZSTD,
    address_state            VARCHAR(50)    ENCODE ZSTD,
    address_zip              VARCHAR(20)    ENCODE ZSTD,
    address_country          VARCHAR(50)    ENCODE ZSTD,
    registration_date        DATE           ENCODE AZ64,
    customer_segment         VARCHAR(20)    ENCODE ZSTD,
    lifetime_value           DECIMAL(12,2)  ENCODE AZ64,
    -- Enriched fields from ETL
    total_transactions       INTEGER        ENCODE AZ64,
    total_spend              DECIMAL(14,2)  ENCODE AZ64,
    avg_order_value          DECIMAL(10,2)  ENCODE AZ64,
    first_purchase_date      DATE           ENCODE AZ64,
    last_purchase_date       DATE           ENCODE AZ64,
    unique_products_purchased INTEGER       ENCODE AZ64,
    days_since_last_purchase INTEGER        ENCODE AZ64,
    tenure_days              INTEGER        ENCODE AZ64,
    customer_tier            VARCHAR(20)    ENCODE ZSTD,
    churn_risk               VARCHAR(10)    ENCODE ZSTD,

    PRIMARY KEY (customer_key)
)
DISTSTYLE KEY
DISTKEY (customer_key)
SORTKEY (customer_id, last_purchase_date);


-- ────────────────────────────────────────────────────────────
-- DIMENSION: dim_product
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key         BIGINT         NOT NULL  ENCODE AZ64,
    product_id          VARCHAR(20)    NOT NULL  ENCODE ZSTD,
    product_name        VARCHAR(255)   ENCODE ZSTD,
    category            VARCHAR(100)   ENCODE ZSTD,
    subcategory         VARCHAR(100)   ENCODE ZSTD,
    brand               VARCHAR(100)   ENCODE ZSTD,
    price               DECIMAL(10,2)  ENCODE AZ64,
    cost                DECIMAL(10,2)  ENCODE AZ64,
    weight_kg           DECIMAL(8,3)   ENCODE AZ64,
    is_active           BOOLEAN        ENCODE RAW,
    created_date        DATE           ENCODE AZ64,
    -- Enriched fields
    total_units_sold    INTEGER        ENCODE AZ64,
    total_revenue       DECIMAL(14,2)  ENCODE AZ64,
    unique_buyers       INTEGER        ENCODE AZ64,
    avg_selling_price   DECIMAL(10,2)  ENCODE AZ64,
    profit_margin       DECIMAL(6,2)   ENCODE AZ64,

    PRIMARY KEY (product_key)
)
DISTSTYLE ALL        -- Relatively small, replicate
SORTKEY (product_id, category);


-- ────────────────────────────────────────────────────────────
-- DIMENSION: dim_channel
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.dim_channel (
    channel_key     SMALLINT       NOT NULL  ENCODE RAW,
    channel_name    VARCHAR(30)    NOT NULL  ENCODE ZSTD,
    channel_type    VARCHAR(30)    ENCODE ZSTD,  -- online / offline
    is_digital      BOOLEAN        ENCODE RAW,

    PRIMARY KEY (channel_key)
)
DISTSTYLE ALL
SORTKEY (channel_key);

-- Seed channel dimension
INSERT INTO analytics.dim_channel VALUES
    (1, 'web',        'online',  TRUE),
    (2, 'mobile_app', 'online',  TRUE),
    (3, 'tablet',     'online',  TRUE),
    (4, 'in_store',   'offline', FALSE),
    (5, 'phone',      'offline', FALSE);


-- ────────────────────────────────────────────────────────────
-- FACT: fact_sales
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    transaction_id       VARCHAR(50)    NOT NULL  ENCODE ZSTD,
    customer_key         BIGINT         NOT NULL  ENCODE AZ64,
    product_key          BIGINT         NOT NULL  ENCODE AZ64,
    date_key             DATE           NOT NULL  ENCODE AZ64,
    quantity             INTEGER        NOT NULL  ENCODE AZ64,
    unit_price           DECIMAL(10,2)  NOT NULL  ENCODE AZ64,
    gross_amount         DECIMAL(12,2)  NOT NULL  ENCODE AZ64,
    discount_amount      DECIMAL(10,2)  ENCODE AZ64,
    discount_percentage  DECIMAL(5,2)   ENCODE AZ64,
    net_amount           DECIMAL(12,2)  NOT NULL  ENCODE AZ64,
    payment_method       VARCHAR(30)    ENCODE ZSTD,
    channel              VARCHAR(30)    ENCODE ZSTD,
    store_id             VARCHAR(20)    ENCODE ZSTD,

    PRIMARY KEY (transaction_id),
    FOREIGN KEY (customer_key) REFERENCES analytics.dim_customer(customer_key),
    FOREIGN KEY (product_key)  REFERENCES analytics.dim_product(product_key),
    FOREIGN KEY (date_key)     REFERENCES analytics.dim_date(date_key)
)
DISTSTYLE KEY
DISTKEY (customer_key)      -- Collocate with dim_customer for fast joins
SORTKEY (date_key, customer_key);


-- ────────────────────────────────────────────────────────────
-- FACT: fact_clickstream
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.fact_clickstream (
    event_id            VARCHAR(50)    NOT NULL  ENCODE ZSTD,
    event_type          VARCHAR(30)    NOT NULL  ENCODE ZSTD,
    event_timestamp     TIMESTAMP      NOT NULL  ENCODE AZ64,
    customer_key        BIGINT         NOT NULL  ENCODE AZ64,
    session_id          VARCHAR(50)    ENCODE ZSTD,
    date_key            DATE           NOT NULL  ENCODE AZ64,
    channel             VARCHAR(30)    ENCODE ZSTD,
    device              VARCHAR(30)    ENCODE ZSTD,
    browser             VARCHAR(30)    ENCODE ZSTD,
    region              VARCHAR(30)    ENCODE ZSTD,
    page_url            VARCHAR(500)   ENCODE ZSTD,
    product_id          VARCHAR(20)    ENCODE ZSTD,
    search_term         VARCHAR(255)   ENCODE ZSTD,
    order_total         DECIMAL(12,2)  ENCODE AZ64,
    event_sequence      INTEGER        ENCODE AZ64,

    PRIMARY KEY (event_id),
    FOREIGN KEY (customer_key) REFERENCES analytics.dim_customer(customer_key),
    FOREIGN KEY (date_key)     REFERENCES analytics.dim_date(date_key)
)
DISTSTYLE KEY
DISTKEY (customer_key)
SORTKEY (date_key, event_timestamp);
