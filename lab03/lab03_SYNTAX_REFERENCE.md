# Lab 03 Syntax Deep Dive & Line‑by‑Line Annotations

This companion file provides an expanded “syntax explanation” for every construct used in Lab 03 (`lab03/README.md`).  
It is organized as:

1. Quick Reference Tables (Data Types, Commands, Functions)
2. Environment & Context Commands
3. DDL (Create / Alter / Drop) Syntax Explained
4. File Formats & Stages
5. Semi‑Structured Data (VARIANT / JSON)
6. Data Generation & Randomization Functions
7. Transformations & String / Conditional Logic
8. Analytical Queries (Grouping, Aggregates, Window)
9. Warehouse Management & Scaling Syntax
10. Monitoring & Metadata Functions / Views
11. Data Pipeline (Staging → Processed)
12. Data Quality Pattern
13. Cleanup Patterns
14. Full Annotated Code (Inline Comments)

---

## 1. Quick Reference Tables

### 1.1 Core Commands

| Category | Keyword / Pattern | Meaning |
|----------|------------------|---------|
| Context  | `USE WAREHOUSE <name>` | Selects active compute cluster. |
| Context  | `USE DATABASE <name>` | Sets active database. |
| Context  | `USE SCHEMA <name>` | Sets active schema inside current database. |
| DDL      | `CREATE OR REPLACE TABLE` | Creates new or replaces existing table. |
| DDL      | `CREATE OR REPLACE STAGE` | Creates named internal stage (managed storage). |
| DDL      | `CREATE OR REPLACE FILE FORMAT` | Reusable parsing definition for load. |
| DDL      | `CREATE OR REPLACE WAREHOUSE` | Defines virtual warehouse (compute). |
| DML      | `INSERT INTO ... VALUES` | Insert literal rows. |
| DML      | `INSERT INTO ... SELECT` | Insert derived / transformed rows. |
| Query    | `SELECT ... FROM ...` | Retrieve data. |
| Metadata | `SHOW <OBJECTS>` | Lists object metadata. |
| Metadata | `RESULT_SCAN(LAST_QUERY_ID())` | Re-query last metadata result set. |
| Cleanup  | `DROP TABLE IF EXISTS` | Remove table if it exists. |
| Warehouse| `ALTER WAREHOUSE ... SUSPEND` | Stops billing (park). |

### 1.2 Data Types Used

| Type | Notes |
|------|-------|
| `NUMBER` or `NUMBER(p,s)` | Numeric (integer / fixed point). `p`=precision, `s`=scale. |
| `STRING` | Alias of `VARCHAR`. |
| `DATE` | Calendar date (no time). |
| `TIMESTAMP` | Date+time (default scale). |
| `VARIANT` | Semi-structured universal container (JSON, etc.). |

### 1.3 Common Functions

| Function | Purpose |
|----------|---------|
| `UPPER()/LOWER()` | Change case of strings. |
| `SPLIT_PART(str, delim, index)` | Return 1-based token. |
| `PARSE_JSON(text)` | Convert string to VARIANT JSON. |
| `DATEADD(part, value, base)` | Date/time arithmetic. |
| `ROUND(number, scale)` | Numeric rounding. |
| `SEQ4()` | Sequential 64-bit integers per statement. |
| `RANDOM()` | Pseudo-random float. |
| `UNIFORM(min, max, seed)` | Pseudo-random integer in range `[min, max)` (upper exclusive). |
| `ROW_NUMBER() OVER(...)` | Window row enumeration. |
| `CURRENT_DATE() / CURRENT_TIMESTAMP()` | System date/time. |
| `TO_CHAR(value, format)` | Formatting. |
| Aggregates | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV` |
| Casting | `expr::TYPE` or `CAST(expr AS TYPE)` |

---

## 2. Context & Session Control

```sql
USE WAREHOUSE LEARN_WH;
USE DATABASE LEARN_SNOWFLAKE;
USE SCHEMA SANDBOX;
```

- These change *execution contexts*; objects referenced without qualifiers will resolve in current database+schema.
- Must exist or error (unless created earlier).

---

## 3. Table & Column Definitions (DDL)

Pattern:

```sql
CREATE OR REPLACE TABLE table_name (
  column_name DATA_TYPE [DEFAULT <expr>] [COMMENT '<text>']
);
```

Key points used:
- `OR REPLACE` = drop and recreate (destroying existing data).
- `DEFAULT CURRENT_TIMESTAMP()` auto-fills when column omitted in INSERT.
- `TEMPORARY TABLE` = lifetime only in current session (not visible to others; not billed for long-term storage after session ends).

---

## 4. Stages & File Formats

### 4.1 Stage Creation with Inline Format

```sql
CREATE OR REPLACE STAGE csv_stage
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL','null','')
  EMPTY_FIELD_AS_NULL = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
);
```

Explanation of properties:
- `FIELD_DELIMITER` – splitting character.
- `SKIP_HEADER` – ignore leading lines (commonly 1).
- `FIELD_OPTIONALLY_ENCLOSED_BY` – allows quoting; needed when values contain delimiter.
- `NULL_IF` – string tokens mapped to SQL NULL.
- `EMPTY_FIELD_AS_NULL` – treat blank tokens as NULL instead of empty string.
- `ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE` – prevents load failure when rows have varying column counts.

### 4.2 File Format Objects

Standalone formats allow reuse:

```sql
CREATE OR REPLACE FILE FORMAT json_format
TYPE='JSON'
STRIP_OUTER_ARRAY=TRUE
STRIP_NULL_VALUES=FALSE
IGNORE_UTF8_ERRORS=FALSE;
```

- `STRIP_OUTER_ARRAY=TRUE` splits top-level JSON array into multiple rows.
- `STRIP_NULL_VALUES=FALSE` retains explicit null entries.
- `IGNORE_UTF8_ERRORS=FALSE` fails on invalid encoding (safer).

---

## 5. Semi-Structured Data (VARIANT & JSON Paths)

Access pattern:

```
variant_column:field
variant_column:nested:inner
variant_column:arrayField[0]:child
```

Casting:

```
variant_column:amount::NUMBER(10,2)
```

Behavior:
- Missing path → NULL (no error).
- Casting errors (e.g., non-numeric string to NUMBER) raise errors unless wrapped in `TRY_CAST`.

---

## 6. Data Generation & Randomization

Example block:

```sql
FROM TABLE(GENERATOR(ROWCOUNT => 50000));
```

- `GENERATOR` returns virtual rows; used to synthesize data.

Functions:
- `UNIFORM(a,b,RANDOM())` returns integer from `a` to `b-1`.
- `CASE (UNIFORM(...)) WHEN 1 THEN 'X' ... END` maps random code to label.
- `SEQ4()` gives deterministic sequential integers (0-based).
- `DATEADD('day', -UNIFORM(1,730,RANDOM()), CURRENT_DATE())` random historical date in last 2 years.

---

## 7. Transformations & Logic

### 7.1 String Building (CSV Simulation)

```sql
customer_id || ',' || '"' || customer_name || '",'
```
- `||` concatenates strings (numbers auto-cast to text).
- Quotation embedding builds properly quoted CSV tokens.

### 7.2 CASE Expression

```sql
CASE 
  WHEN amount < 50 THEN 'Low'
  WHEN amount < 200 THEN 'Medium'
  WHEN amount < 500 THEN 'High'
  ELSE 'Premium'
END
```

- Evaluated top-down; first true branch returns value.
- Final `ELSE` is fallback (optional; defaults to NULL if omitted).

---

## 8. Analytical Query Elements

```sql
SELECT category,
       COUNT(*) AS transaction_count,
       SUM(amount) AS total_sales
FROM performance_test
WHERE date_created >= '2023-01-01'
GROUP BY category
HAVING COUNT(*) > 10
ORDER BY total_sales DESC;
```

Sequence:
1. `FROM` / `WHERE` filter rows.
2. `GROUP BY` partitions for aggregation.
3. Aggregate functions computed.
4. `HAVING` filters aggregated groups.
5. `ORDER BY` sorts final rows.
6. `SELECT` defines projection (logically last but written earlier).

---

## 9. Warehouse Management

Creation:

```sql
CREATE OR REPLACE WAREHOUSE AUTO_SCALE_WH
WITH 
  WAREHOUSE_SIZE='MEDIUM'
  AUTO_SUSPEND=300
  AUTO_RESUME=TRUE
  MIN_CLUSTER_COUNT=1
  MAX_CLUSTER_COUNT=3
  SCALING_POLICY='STANDARD';
```

Key parameters:
- `AUTO_SUSPEND` (seconds) – idle timeout.
- `AUTO_RESUME` – automatically start when query arrives.
- Multi-cluster: scale concurrency (not single-query speed).
- `SCALING_POLICY='STANDARD'` – adds clusters sooner (vs `ECONOMY`).

Suspend:

```sql
ALTER WAREHOUSE XS_WH SUSPEND;
```

---

## 10. Monitoring & Metadata

### 10.1 Query History

```sql
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
```

- Table function; must be wrapped in `TABLE()`.
- Filters with `WHERE start_time >= DATEADD('hour', -2, CURRENT_TIMESTAMP())`.

Common columns:
- `query_text`, `user_name`, `warehouse_name`, `total_elapsed_time` (ms), `bytes_scanned`, etc.

### 10.2 Account Usage

```sql
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
```

- Historical credit usage (latency ~hour).
- Aggregation patterns with `SUM(credits_used)`.

### 10.3 Result Reuse

```sql
SELECT ... FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
```

- Re-reads result set (e.g., from `SHOW WAREHOUSES`).
- Column names from SHOW are *lower-case quoted* fields → must reference as `"name"` or alias.

---

## 11. Data Pipeline Pattern

Stages:
1. Load raw JSON → `raw_sales_staging(raw_data VARIANT, metadata columns)`
2. Transform & cast → `processed_sales` typed columns
3. Aggregate / validate.

Insert transformation:

```sql
(raw_data:quantity::NUMBER * raw_data:unit_price::NUMBER)
```

Discount:

```sql
ROUND( (qty * price) * (1 - discount/100), 2 )
```

---

## 12. Data Quality Pattern

```sql
INSERT INTO data_quality_results (check_name, check_description, result_count, expected_count, status)
VALUES
('null_customer_ids', 'Count ...',
 (SELECT COUNT(*) FROM processed_sales WHERE customer_id IS NULL),
 0,
 CASE WHEN (SELECT COUNT(*) FROM processed_sales WHERE customer_id IS NULL)=0 THEN 'PASS' ELSE 'FAIL' END),
...
```

Patterns:
- Scalar subqueries inside VALUES.
- Repeated subquery pattern (could be optimized using CTE).
- `CASE` to derive PASS/FAIL.

---

## 13. Cleanup

```sql
DROP TABLE IF EXISTS temp_csv_data;
```

- Safe drop (no error if absent).

Suspending warehouses reduces ongoing credit charges.

---

## 14. Full Annotated Code (Inline Comments)

Below: Each original lab snippet annotated line-by-line.

```sql
-- ==== Context Setup ====
USE WAREHOUSE LEARN_WH;          -- Select compute cluster
USE DATABASE LEARN_SNOWFLAKE;    -- Set active database
USE SCHEMA SANDBOX;              -- Set active schema

-- ==== Sample Data Table ====
CREATE OR REPLACE TABLE sample_data (    -- Create table (replace if exists)
    customer_id NUMBER,                 -- Numeric (variable precision)
    customer_name STRING,               -- Text field
    email STRING,                       -- Text field
    purchase_date DATE,                 -- Calendar date
    product_category STRING,            -- Text category
    amount NUMBER(10,2),                -- Fixed precision: 10 digits, 2 decimals
    region STRING                       -- Geographic / region label
);

-- Insert multiple rows in one statement
INSERT INTO sample_data VALUES
(1,'John Smith','john.smith@email.com','2024-01-15','Electronics',1299.99,'North'),
(2,'Jane Doe','jane.doe@company.org','2024-01-16','Clothing',89.50,'South'),
(3,'Bob Johnson','bob.j@domain.net','2024-01-17','Books',24.99,'East'),
(4,'Alice Brown','alice.brown@startup.io','2024-01-18','Home',156.75,'West'),
(5,'Charlie Wilson','charlie.w@corp.com','2024-01-19','Sports',78.25,'North');

SELECT * FROM sample_data;  -- View inserted data

-- ==== Stages & File Formats ====
SHOW STAGES;  -- List accessible stages

CREATE OR REPLACE STAGE my_internal_stage
COMMENT='Internal stage for data loading practice';  -- Named internal stage

CREATE OR REPLACE STAGE csv_stage
FILE_FORMAT = (                      -- Inline file format specification
  TYPE='CSV'
  FIELD_DELIMITER=','
  RECORD_DELIMITER='\n'
  SKIP_HEADER=1
  FIELD_OPTIONALLY_ENCLOSED_BY='"'
  NULL_IF=('NULL','null','')
  EMPTY_FIELD_AS_NULL=TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
);

CREATE OR REPLACE STAGE json_stage
FILE_FORMAT = (
  TYPE='JSON'
  STRIP_OUTER_ARRAY=TRUE             -- Break outer JSON array into rows
);

-- Reusable file formats
CREATE OR REPLACE FILE FORMAT csv_format
TYPE='CSV'
FIELD_DELIMITER=','
RECORD_DELIMITER='\n'
SKIP_HEADER=1
FIELD_OPTIONALLY_ENCLOSED_BY='"'
NULL_IF=('NULL','null','','N/A')
EMPTY_FIELD_AS_NULL=TRUE
COMPRESSION='AUTO'
ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
VALIDATE_UTF8=TRUE;

CREATE OR REPLACE FILE FORMAT json_format
TYPE='JSON'
STRIP_OUTER_ARRAY=TRUE
STRIP_NULL_VALUES=FALSE
IGNORE_UTF8_ERRORS=FALSE;

CREATE OR REPLACE FILE FORMAT parquet_format
TYPE='PARQUET'
COMPRESSION='AUTO';

SHOW FILE FORMATS;  -- List defined formats

-- ==== Simulated CSV Construction ====
CREATE OR REPLACE TABLE customers_from_file (
  customer_id NUMBER,
  customer_name STRING,
  email STRING,
  purchase_date DATE,
  product_category STRING,
  amount NUMBER(10,2),
  region STRING
);

CREATE OR REPLACE TEMPORARY TABLE temp_csv_data AS
SELECT
  customer_id || ',' ||
  '"' || customer_name || '",' ||
  '"' || email || '",' ||
  purchase_date || ',' ||
  '"' || product_category || '",' ||
  amount || ',' ||
  '"' || region || '"' AS csv_line
FROM sample_data;

SELECT csv_line FROM temp_csv_data;  -- Show constructed pseudo-CSV lines

-- ==== JSON Handling ====
CREATE OR REPLACE TABLE json_data (raw_data VARIANT);

CREATE OR REPLACE TABLE temp_json_source AS
SELECT PARSE_JSON('{
  "customer_id": ' || customer_id || ',
  "customer_name": "' || customer_name || '",
  "email": "' || email || '",
  "purchase_date": "' || purchase_date || '",
  "product_category": "' || product_category || '",
  "amount": ' || amount || ',
  "region": "' || region || '",
  "metadata": {
     "source": "online",
     "campaign": "winter_sale"
  }
}') AS json_data
FROM sample_data;

INSERT INTO json_data
SELECT json_data FROM temp_json_source;

SELECT
  raw_data:customer_id::NUMBER        AS customer_id,
  raw_data:customer_name::STRING      AS customer_name,
  raw_data:email::STRING              AS email,
  raw_data:purchase_date::DATE        AS purchase_date,
  raw_data:product_category::STRING   AS product_category,
  raw_data:amount::NUMBER(10,2)       AS amount,
  raw_data:region::STRING             AS region,
  raw_data:metadata:source::STRING    AS source,
  raw_data:metadata:campaign::STRING  AS campaign
FROM json_data;

-- ==== Enriched Transformation ====
CREATE OR REPLACE TABLE enriched_customers (
  customer_id NUMBER,
  customer_name STRING,
  email STRING,
  email_domain STRING,
  purchase_date DATE,
  product_category STRING,
  amount NUMBER(10,2),
  amount_tier STRING,
  region STRING,
  load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO enriched_customers (
  customer_id, customer_name, email, email_domain,
  purchase_date, product_category, amount, amount_tier, region
)
SELECT
  customer_id,
  UPPER(customer_name)                             AS customer_name,
  LOWER(email)                                     AS email,
  SPLIT_PART(email,'@',2)                          AS email_domain,
  purchase_date,
  product_category,
  amount,
  CASE
    WHEN amount < 50  THEN 'Low'
    WHEN amount < 200 THEN 'Medium'
    WHEN amount < 500 THEN 'High'
    ELSE 'Premium'
  END                                              AS amount_tier,
  region
FROM sample_data;

SELECT * FROM enriched_customers;

-- ==== Warehouses ====
CREATE OR REPLACE WAREHOUSE XS_WH WITH WAREHOUSE_SIZE='X-SMALL' AUTO_SUSPEND=60;
CREATE OR REPLACE WAREHOUSE S_WH  WITH WAREHOUSE_SIZE='SMALL'   AUTO_SUSPEND=60;
CREATE OR REPLACE WAREHOUSE M_WH  WITH WAREHOUSE_SIZE='MEDIUM'  AUTO_SUSPEND=60;
SHOW WAREHOUSES;

SELECT
  "name" AS warehouse_name,
  "size" AS warehouse_size,
  "min_cluster_count",
  "max_cluster_count",
  "auto_suspend",
  "auto_resume",
  "state"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  -- Reuse last result

-- ==== Performance Test Data ====
CREATE OR REPLACE TABLE performance_test (
  id NUMBER,
  category STRING,
  subcategory STRING,
  amount NUMBER,
  date_created DATE,
  region STRING,
  customer_segment STRING
);

INSERT INTO performance_test
SELECT
  SEQ4() AS id,
  CASE (UNIFORM(1,5,RANDOM()))
    WHEN 1 THEN 'Electronics'
    WHEN 2 THEN 'Clothing'
    WHEN 3 THEN 'Books'
    WHEN 4 THEN 'Home'
    ELSE 'Sports'
  END AS category,
  CASE (UNIFORM(1,10,RANDOM()))
    WHEN 1 THEN 'Laptops' WHEN 2 THEN 'Phones' WHEN 3 THEN 'Accessories'
    WHEN 4 THEN 'Shirts'  WHEN 5 THEN 'Pants'  WHEN 6 THEN 'Shoes'
    WHEN 7 THEN 'Fiction' WHEN 8 THEN 'Non-Fiction' WHEN 9 THEN 'Textbooks'
    ELSE 'General'
  END AS subcategory,
  ROUND(UNIFORM(10,2000,RANDOM()),2) AS amount,
  DATEADD('day', -UNIFORM(1,730,RANDOM()), CURRENT_DATE()) AS date_created,
  CASE (UNIFORM(1,4,RANDOM()))
    WHEN 1 THEN 'North' WHEN 2 THEN 'South'
    WHEN 3 THEN 'East'  ELSE 'West'
  END AS region,
  CASE (UNIFORM(1,3,RANDOM()))
    WHEN 1 THEN 'Enterprise'
    WHEN 2 THEN 'SMB'
    ELSE 'Consumer'
  END AS customer_segment
FROM TABLE(GENERATOR(ROWCOUNT => 50000));

USE WAREHOUSE XS_WH;

SELECT
  category,
  subcategory,
  region,
  customer_segment,
  COUNT(*)      AS transaction_count,
  SUM(amount)   AS total_sales,
  AVG(amount)   AS avg_sale,
  MIN(amount)   AS min_sale,
  MAX(amount)   AS max_sale,
  STDDEV(amount) AS stddev_amount
FROM performance_test
WHERE date_created >= '2023-01-01'
GROUP BY category, subcategory, region, customer_segment
HAVING COUNT(*) > 10
ORDER BY total_sales DESC;

-- ==== Auto-Scaling Warehouse ====
CREATE OR REPLACE WAREHOUSE AUTO_SCALE_WH
WITH
  WAREHOUSE_SIZE='MEDIUM'
  AUTO_SUSPEND=300
  AUTO_RESUME=TRUE
  MIN_CLUSTER_COUNT=1
  MAX_CLUSTER_COUNT=3
  SCALING_POLICY='STANDARD'
  COMMENT='Auto-scaling warehouse for variable workloads';

ALTER WAREHOUSE LEARN_WH SET
  MAX_CLUSTER_COUNT=2
  SCALING_POLICY='STANDARD';

SHOW WAREHOUSES LIKE 'AUTO_SCALE_WH';

-- ==== Pipeline Raw Staging ====
CREATE OR REPLACE TABLE raw_sales_staging (
  raw_data VARIANT,
  load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  file_name STRING,
  file_row_number NUMBER
);

CREATE OR REPLACE TABLE processed_sales (
  sale_id NUMBER,
  customer_id NUMBER,
  customer_name STRING,
  product_id NUMBER,
  product_name STRING,
  category STRING,
  sale_date DATE,
  quantity NUMBER,
  unit_price NUMBER(10,2),
  total_amount NUMBER(10,2),
  discount_percent NUMBER(5,2),
  final_amount NUMBER(10,2),
  sales_rep STRING,
  region STRING,
  processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO raw_sales_staging (raw_data, file_name, file_row_number)
SELECT
  PARSE_JSON('{
    "sale_id": ' || (ROW_NUMBER() OVER (ORDER BY RANDOM())) || ',
    "customer_id": ' || UNIFORM(1001,9999,RANDOM()) || ',
    "customer_name": "Customer ' || UNIFORM(1,1000,RANDOM()) || '",
    "product_id": ' || UNIFORM(1,100,RANDOM()) || ',
    "product_name": "Product ' || UNIFORM(1,100,RANDOM()) || '",
    "category": "' || CASE (UNIFORM(1,4,RANDOM()))
                       WHEN 1 THEN 'Electronics'
                       WHEN 2 THEN 'Clothing'
                       WHEN 3 THEN 'Books'
                       ELSE 'Home'
                     END || '",
    "sale_date": "' || DATEADD('day', -UNIFORM(1,90,RANDOM()), CURRENT_DATE()) || '",
    "quantity": ' || UNIFORM(1,10,RANDOM()) || ',
    "unit_price": ' || ROUND(UNIFORM(10,500,RANDOM()),2) || ',
    "discount_percent": ' || ROUND(UNIFORM(0,25,RANDOM()),2) || ',
    "sales_rep": "Rep' || UNIFORM(1,20,RANDOM()) || '",
    "region": "' || CASE (UNIFORM(1,4,RANDOM()))
                      WHEN 1 THEN 'North'
                      WHEN 2 THEN 'South'
                      WHEN 3 THEN 'East'
                      ELSE 'West'
                    END || '"
  }') AS raw_data,
  'sales_batch_' || TO_CHAR(CURRENT_DATE(),'YYYY_MM_DD') || '.json' AS file_name,
  ROW_NUMBER() OVER (ORDER BY RANDOM()) AS file_row_number
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

INSERT INTO processed_sales (
  sale_id, customer_id, customer_name, product_id, product_name,
  category, sale_date, quantity, unit_price, total_amount,
  discount_percent, final_amount, sales_rep, region
)
SELECT
  raw_data:sale_id::NUMBER               AS sale_id,
  raw_data:customer_id::NUMBER           AS customer_id,
  raw_data:customer_name::STRING         AS customer_name,
  raw_data:product_id::NUMBER            AS product_id,
  raw_data:product_name::STRING          AS product_name,
  raw_data:category::STRING              AS category,
  raw_data:sale_date::DATE               AS sale_date,
  raw_data:quantity::NUMBER              AS quantity,
  raw_data:unit_price::NUMBER(10,2)      AS unit_price,
  (raw_data:quantity::NUMBER * raw_data:unit_price::NUMBER) AS total_amount,
  raw_data:discount_percent::NUMBER(5,2) AS discount_percent,
  ROUND(
    (raw_data:quantity::NUMBER * raw_data:unit_price::NUMBER) *
    (1 - raw_data:discount_percent::NUMBER / 100), 2
  ) AS final_amount,
  raw_data:sales_rep::STRING             AS sales_rep,
  raw_data:region::STRING                AS region
FROM raw_sales_staging
WHERE raw_data IS NOT NULL;

SELECT
  COUNT(*)                 AS total_sales,
  SUM(final_amount)        AS total_revenue,
  AVG(final_amount)        AS avg_sale,
  COUNT(DISTINCT customer_id) AS unique_customers,
  COUNT(DISTINCT sales_rep)   AS sales_reps
FROM processed_sales;

-- ==== Data Quality Checks ====
CREATE OR REPLACE TABLE data_quality_results (
  check_name STRING,
  check_description STRING,
  result_count NUMBER,
  expected_count NUMBER,
  status STRING,
  check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO data_quality_results (check_name, check_description, result_count, expected_count, status)
VALUES
('null_customer_ids', 'Count of records with null customer_id',
 (SELECT COUNT(*) FROM processed_sales WHERE customer_id IS NULL),
 0,
 CASE WHEN (SELECT COUNT(*) FROM processed_sales WHERE customer_id IS NULL)=0 THEN 'PASS' ELSE 'FAIL' END),
('negative_amounts', 'Count of records with negative final_amount',
 (SELECT COUNT(*) FROM processed_sales WHERE final_amount < 0),
 0,
 CASE WHEN (SELECT COUNT(*) FROM processed_sales WHERE final_amount < 0)=0 THEN 'PASS' ELSE 'FAIL' END),
('future_dates', 'Count of records with future sale_date',
 (SELECT COUNT(*) FROM processed_sales WHERE sale_date > CURRENT_DATE()),
 0,
 CASE WHEN (SELECT COUNT(*) FROM processed_sales WHERE sale_date > CURRENT_DATE())=0 THEN 'PASS' ELSE 'FAIL' END),
('invalid_discounts', 'Count of records with discount > 50%',
 (SELECT COUNT(*) FROM processed_sales WHERE discount_percent > 50),
 0,
 CASE WHEN (SELECT COUNT(*) FROM processed_sales WHERE discount_percent > 50)=0 THEN 'PASS' ELSE 'FAIL' END);

SELECT * FROM data_quality_results ORDER BY check_timestamp DESC;

-- ==== Monitoring ====
SELECT
  query_id,
  query_text,
  user_name,
  warehouse_name,
  warehouse_size,
  execution_status,
  total_elapsed_time,
  bytes_scanned,
  rows_produced,
  compilation_time,
  execution_time,
  start_time
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE start_time >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
  AND warehouse_name IS NOT NULL
ORDER BY total_elapsed_time DESC
LIMIT 20;

SELECT
  warehouse_name,
  COUNT(*)                AS query_count,
  AVG(total_elapsed_time) AS avg_execution_time,
  SUM(total_elapsed_time) AS total_execution_time,
  SUM(bytes_scanned)      AS total_bytes_scanned,
  SUM(rows_produced)      AS total_rows_produced
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
  AND warehouse_name IS NOT NULL
GROUP BY warehouse_name
ORDER BY total_execution_time DESC;

SELECT
  table_catalog,
  table_schema,
  table_name,
  row_count,
  bytes,
  ROUND(bytes / (1024*1024*1024),2) AS size_gb,
  ROUND(bytes / NULLIF(row_count,0),0) AS avg_bytes_per_row
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema='SANDBOX'
  AND table_type='BASE TABLE'
ORDER BY bytes DESC;

SELECT
  warehouse_name,
  SUM(credits_used) AS total_credits,
  AVG(credits_used) AS avg_credits_per_hour,
  COUNT(*)          AS active_hours
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY warehouse_name
ORDER BY total_credits DESC;

-- ==== Cleanup ====
DROP TABLE IF EXISTS temp_csv_data;
DROP TABLE IF EXISTS temp_json_source;

ALTER WAREHOUSE XS_WH SUSPEND;
ALTER WAREHOUSE S_WH  SUSPEND;
ALTER WAREHOUSE M_WH  SUSPEND;
ALTER WAREHOUSE AUTO_SCALE_WH SUSPEND;

SHOW WAREHOUSES;
```

---

## 15. Subtle Points & Best-Practice Notes

| Topic | Detail | Recommendation |
|-------|--------|----------------|
| Random ranges | `UNIFORM(min,max,seed)` upper bound is exclusive | Use `UNIFORM(1,6,...)` for 1–5 inclusive |
| Repeated scalar subqueries | Data quality section repeats `SELECT COUNT(*)` | Use CTE to compute once if large |
| JSON building via concatenation | Manual string risk for quotes | For production, ingest real JSON or use `OBJECT_CONSTRUCT` |
| Lack of COPY examples | Simulated loads only | Add real `PUT` (if SnowSQL) + `COPY INTO` |
| Multi-cluster scaling | Only helpful for concurrency | Use larger size for single heavy queries |
| Data type enforcement | Casting JSON fields | Use `TRY_CAST` if uncertain input |

---

## 16. Suggested Extensions (Optional)

1. Add `COPY INTO` validation mode:
   ```
   COPY INTO target_table
   FROM @stage/files/
   FILE_FORMAT=(FORMAT_NAME=csv_format)
   VALIDATION_MODE='RETURN_ERRORS';
   ```
2. Use `OBJECT_CONSTRUCT` for JSON building:
   ```
   OBJECT_CONSTRUCT(
     'sale_id', seq4(),
     'category', category
   )
   ```
3. Introduce `TASK` + `STREAM` for incremental pipeline.
4. Add masking policy example for email domain obfuscation.
5. Add `CLUSTER BY (sale_date)` to large fact table if query patterns justify.

---

## 17. Glossary (Selected)

| Term | Meaning |
|------|---------|
| Stage | Location for files pre/post load (internal/external). |
| Warehouse | Compute resource (billing unit). |
| Micro-partition | Internal storage unit (automatic, not user-managed). |
| Variant Flattening | Breaking nested arrays with `LATERAL FLATTEN()`. |
| Credit | Billing unit (per warehouse size per time). |

---

## 18. Need More?

Ask for:
- Flashcards version
- Diff patch to inject this into repo via PR
- Real external stage (S3/Azure/GCS) examples
- `COPY INTO` error diagnostics tutorial

---

Happy querying!