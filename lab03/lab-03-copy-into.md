# Lab-03: Loading Data into Snowflake with COPY INTO

This lab demonstrates the **COPY INTO command**, Snowflake’s primary method for loading data into tables from staged files.

---

## 🎯 Learning Objectives
- Understand how `COPY INTO` works in Snowflake.  
- Learn how to load **CSV, JSON, and Parquet** data from stages.  
- Practice validation, error handling, and file management.  
- Compare with external stages (Azure, S3, GCS).  
- Monitor and troubleshoot data loading jobs.  

---

## 📌 Step-by-Step Tasks

### 1. Create Target Table
```sql
CREATE OR REPLACE TABLE sales_raw (
  order_id    NUMBER,
  order_date  DATE,
  customer    STRING,
  amount      NUMBER(10,2)
);
```

### 2. Create File Format (CSV Example)
```sql
CREATE OR REPLACE FILE FORMAT ff_csv_standard
  TYPE = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('\\N','NULL','');
```

### 3. Create Internal Stage
```sql
CREATE OR REPLACE STAGE stg_sales FILE_FORMAT = ff_csv_standard;
```

Upload files:
```bash
snowsql -q "PUT file://./data/*.csv @stg_sales AUTO_COMPRESS=TRUE"
```

Check files:
```sql
LIST @stg_sales;
```

### 4. Validate Files
```sql
COPY INTO sales_raw
FROM @stg_sales
VALIDATION_MODE = 'RETURN_2_ROWS';
```

### 5. Load Data
```sql
COPY INTO sales_raw
FROM @stg_sales
ON_ERROR = 'ABORT_STATEMENT';
```

### 6. Semi-Structured JSON Example
```sql
CREATE OR REPLACE TABLE events_raw (
  id       NUMBER,
  payload  VARIANT,
  ts       TIMESTAMP_NTZ
);

CREATE OR REPLACE FILE FORMAT ff_json TYPE = JSON;
CREATE OR REPLACE STAGE stg_events FILE_FORMAT = ff_json;

COPY INTO events_raw (id, payload, ts)
FROM (
  SELECT
    t.$1:id::NUMBER,
    t.$1,
    t.$1:ts::TIMESTAMP_NTZ
  FROM @stg_events t
)
FILE_FORMAT = (TYPE=JSON);
```

### 7. Parquet Example
```sql
CREATE OR REPLACE TABLE metrics_parq (
  name STRING,
  val  FLOAT,
  ts   TIMESTAMP_NTZ
);

CREATE OR REPLACE FILE FORMAT ff_parquet TYPE = PARQUET;
CREATE OR REPLACE STAGE stg_parq FILE_FORMAT = ff_parquet;

COPY INTO metrics_parq
FROM @stg_parq
FILE_FORMAT = (TYPE=PARQUET);
```

### 8. Common Options
- `ON_ERROR` → `ABORT_STATEMENT`, `CONTINUE`, `SKIP_FILE`  
- `PURGE=TRUE` → remove staged files after success  
- `FORCE=TRUE` → reload files already marked as loaded  
- `MATCH_BY_COLUMN_NAME` → map CSV headers to table columns  

### 9. Monitor & Troubleshoot
```sql
-- Show errors only
COPY INTO sales_raw
FROM @stg_sales
VALIDATION_MODE = 'RETURN_ERRORS';

-- Copy history
SELECT *
FROM TABLE(
  INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SALES_RAW',
    START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
  )
);
```

---

## ✅ Summary
- `COPY INTO` is the main tool for **bulk loading** data.  
- Works with CSV, JSON, Parquet, Avro, ORC, XML.  
- Supports **internal and external stages**.  
- Idempotent by default (avoids double loads).  
- Flexible error handling and validation options.  

---

## 📝 Next Steps
- Try different `ON_ERROR` strategies.  
- Load from **external cloud storage** with a `STORAGE INTEGRATION`.  
- Explore **Snowpipe** for continuous ingestion.  
