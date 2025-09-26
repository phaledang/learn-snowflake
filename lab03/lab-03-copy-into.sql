-- lab-03-copy-into.sql
-- Lab-03: Demonstrating COPY INTO for CSV, JSON, Parquet

-- Step 1: Create Target Table for CSV
CREATE OR REPLACE TABLE sales_raw (
  order_id    NUMBER,
  order_date  DATE,
  customer    STRING,
  amount      NUMBER(10,2)
);

-- Step 2: Create File Format (CSV)
CREATE OR REPLACE FILE FORMAT ff_csv_standard
  TYPE = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('\\N','NULL','');

-- Step 3: Create Stage
CREATE OR REPLACE STAGE stg_sales FILE_FORMAT = ff_csv_standard;

-- Step 4: Validate Files (preview)
COPY INTO sales_raw
FROM @stg_sales
VALIDATION_MODE = 'RETURN_2_ROWS';

-- Step 5: Load Data from CSV
COPY INTO sales_raw
FROM @stg_sales
ON_ERROR = 'ABORT_STATEMENT';

-- Step 6: JSON Example (load semi-structured data)
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

-- Step 7: Parquet Example
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

-- Step 8: Monitor Copy History
SELECT *
FROM TABLE(
  INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SALES_RAW',
    START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
  )
);
