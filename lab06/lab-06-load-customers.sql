-- lab-06-load-customers.sql
-- Purpose: Load the lab-06-customers.csv file into the CUSTOMERS table using COPY INTO.



-- 1) Create / replace a CSV file format suitable for the sample file
CREATE OR REPLACE FILE FORMAT ff_csv_customers
  TYPE = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('\\N','NULL','');

-- 2) Create / replace a named internal stage and associate the file format
CREATE OR REPLACE STAGE stg_customers FILE_FORMAT = ff_csv_customers;

-- 3) Upload the CSV into the stage (run from your terminal; this line is a comment)
--    snowsql -q "PUT file://lab-06-customers.csv @stg_customers AUTO_COMPRESS=TRUE"
-- C:/code/learn/snowflake/learn-snowflake/lab06/lab-06-customers.csv.gz
--  (Windows path example; adjust for your OS and file location)
snowsql -q "PUT file://C:/code/learn/snowflake/learn-snowflake/lab06/lab-06-customers.csv @stg_customers AUTO_COMPRESS=TRUE"

-- 4) Inspect staged files (optional)
LIST @stg_customers;

-- 5) Validate before loading (optional: returns a couple of preview rows)
COPY INTO customers
FROM @stg_customers
VALIDATION_MODE = 'RETURN_2_ROWS';

-- 6) Load the data (strict mode; aborts on first error)
COPY INTO customers
FROM @stg_customers/lab-06-customers.csv.gz
FILE_FORMAT = (FORMAT_NAME = ff_csv_customers)
ON_ERROR = 'ABORT_STATEMENT';

-- 7) Verify
SELECT COUNT(*) AS loaded_rows FROM customers;
SELECT * FROM customers ORDER BY customer_id;
