# S3 Data Lake — Layered Architecture

## Overview

The Customer 360 Data Lake uses a **3-layer architecture** on Amazon S3, following enterprise lakehouse best practices. Each layer serves a distinct purpose and has its own retention, schema, and access policies.

```
┌──────────────────────────────────────────────────────────────────┐
│                        S3 DATA LAKE                              │
├───────────────┬───────────────────┬──────────────────────────────┤
│   RAW LAYER   │   CLEAN LAYER     │        CURATED LAYER         │
│  (Bronze)     │   (Silver)        │        (Gold)                │
├───────────────┼───────────────────┼──────────────────────────────┤
│ • Immutable   │ • Standardized    │ • Business-logic applied     │
│ • JSON / CSV  │ • Parquet         │ • Parquet (optimized)        │
│ • Ingestion   │ • Schema enforced │ • Joined / aggregated        │
│   partitioned │ • Deduped         │ • Business-date partitioned  │
│ • 90-day      │ • 180-day         │ • 365-day retention          │
│   retention   │   retention       │                              │
└───────────────┴───────────────────┴──────────────────────────────┘
```

---

## Layer Details

### 🗂 Raw Layer (Bronze)

**Bucket:** `c360-raw-{env}`

| Property | Value |
|----------|-------|
| Format | JSON (streaming), CSV (batch) |
| Mutability | **Immutable** — append only |
| Partitioning | Hive-style: `year=/month=/day=/hour=` |
| Encryption | SSE-KMS |
| Retention | 90 days (S3 Lifecycle) |
| Access | Write: Lambda, Batch upload scripts; Read: Glue ETL |

**Key Structure:**
```
s3://c360-raw-dev/
├── events/
│   ├── page_view/year=2024/month=01/day=15/hour=14/*.json
│   ├── purchase/year=2024/month=01/day=15/hour=14/*.json
│   ├── add_to_cart/...
│   └── search/...
└── batch/
    ├── customers/year=2024/month=01/day=15/*.csv
    ├── products/year=2024/month=01/day=15/*.csv
    └── transactions/year=2024/month=01/day=15/*.csv
```

---

### 🧹 Clean Layer (Silver)

**Bucket:** `c360-clean-{env}`

| Property | Value |
|----------|-------|
| Format | **Parquet** (columnar, compressed) |
| Schema | Enforced & type-corrected |
| Deduplication | Applied on primary key |
| Null handling | Default values / filtered |
| Partitioning | `year=/month=/day=` |
| Encryption | SSE-KMS |
| Retention | 180 days |

**Key Structure:**
```
s3://c360-clean-dev/
├── clickstream/year=2024/month=01/day=15/*.parquet
├── customers/year=2024/month=01/day=15/*.parquet
├── products/year=2024/month=01/day=15/*.parquet
└── transactions/year=2024/month=01/day=15/*.parquet
```

---

### 📊 Curated Layer (Gold)

**Bucket:** `c360-curated-{env}`

| Property | Value |
|----------|-------|
| Format | **Parquet** (Snappy compressed) |
| Schema | Star-schema-aligned |
| Transformations | Joins, aggregations, business rules |
| Partitioning | `year=/month=` (by business date) |
| Encryption | SSE-KMS |
| Retention | 365 days |

**Key Structure:**
```
s3://c360-curated-dev/
├── fact_sales/year=2024/month=01/*.parquet
├── fact_clickstream/year=2024/month=01/*.parquet
├── dim_customer/*.parquet
├── dim_product/*.parquet
└── dim_date/*.parquet
```

---

## Lifecycle Policies

| Layer | Transition | Expiration |
|-------|-----------|------------|
| Raw | → IA after 30 days, → Glacier after 60 days | Delete after 90 days |
| Clean | → IA after 90 days | Delete after 180 days |
| Curated | → IA after 180 days | Delete after 365 days |
| Quality Logs | — | Delete after 30 days |
