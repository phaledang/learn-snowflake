-- lab-06-data-classification.sql
-- Lab-06: Applying Data Classification Tags in Snowflake

-- 1) Create Tags
CREATE OR REPLACE TAG pii_classification
    COMMENT = 'Marks columns that contain Personally Identifiable Information (PII)';

CREATE OR REPLACE TAG sensitivity_level
    COMMENT = 'Defines sensitivity classification levels';

-- 2) Create Sample Table
CREATE OR REPLACE TABLE customers (
    customer_id NUMBER,
    full_name   STRING,
    email       STRING,
    phone       STRING,
    balance     NUMBER
);

-- 3) Apply Tags
ALTER TABLE customers
    ALTER COLUMN full_name SET TAG pii_classification = 'true';

ALTER TABLE customers
    ALTER COLUMN email SET TAG pii_classification = 'true';

ALTER TABLE customers
    ALTER COLUMN phone SET TAG sensitivity_level = 'Confidential';

-- 4) Query Tags (for auditing)
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
WHERE TAG_NAME IN ('PII_CLASSIFICATION', 'SENSITIVITY_LEVEL');

SELECT *
FROM TABLE(
  INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'CUSTOMERS',
    'TABLE'
  )
);

-- 5) Optional: Masking Policy linked to Tag
CREATE MASKING POLICY mask_pii
  AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ANALYST_ROLE') THEN '***MASKED***'
    ELSE val
  END;

ALTER TAG pii_classification
  SET MASKING POLICY mask_pii;
