# Lab 03: Data Loading & Warehouse Management

## 🎯 Objectives
By the end of this lab, you will:
- Master different data loading techniques
- Understand file formats and staging
- Learn warehouse sizing and management
- Practice with bulk loading and transformations
- Implement data pipelines

## ⏱️ Estimated Time: 60 minutes

## 📋 Prerequisites
- Completed Lab 01 and Lab 02
- Understanding of SQL and data formats
- Basic knowledge of CSV, JSON, and Parquet files

## 📁 Step 1: Understanding File Formats

### 1.1 Supported File Formats

Snowflake supports various file formats:
- **CSV** (Comma-Separated Values)
- **JSON** (JavaScript Object Notation)
- **Avro** (Binary format with schema)
- **ORC** (Optimized Row Columnar)
- **Parquet** (Columnar storage format)
- **XML** (Extensible Markup Language)

### 1.2 Create Sample Data Files

Let's create sample data to work with:

```sql
-- Set up environment
USE WAREHOUSE LEARN_WH;
USE DATABASE LEARN_SNOWFLAKE;
USE SCHEMA SANDBOX;

-- Create a table to generate sample data
CREATE OR REPLACE TABLE sample_data (
    customer_id NUMBER,
    customer_name STRING,
    email STRING,
    purchase_date DATE,
    product_category STRING,
    amount NUMBER(10,2),
    region STRING
);

-- Insert sample data
INSERT INTO sample_data VALUES
(1, 'John Smith', 'john.smith@email.com', '2024-01-15', 'Electronics', 1299.99, 'North'),
(2, 'Jane Doe', 'jane.doe@company.org', '2024-01-16', 'Clothing', 89.50, 'South'),
(3, 'Bob Johnson', 'bob.j@domain.net', '2024-01-17', 'Books', 24.99, 'East'),
(4, 'Alice Brown', 'alice.brown@startup.io', '2024-01-18', 'Home', 156.75, 'West'),
(5, 'Charlie Wilson', 'charlie.w@corp.com', '2024-01-19', 'Sports', 78.25, 'North');

-- Export data to understand the format (this creates a result set we can reference)
SELECT * FROM sample_data;
```

## 🗂️ Step 2: Creating and Managing Stages

### 2.1 Internal Stages

Stages are storage locations for data files. Snowflake provides internal stages automatically:

```sql
-- View available stages
SHOW STAGES;

-- Create a named internal stage
CREATE OR REPLACE STAGE my_internal_stage
COMMENT = 'Internal stage for data loading practice';

-- Create stage with file format
CREATE OR REPLACE STAGE csv_stage
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
);

-- Create stage for JSON files
CREATE OR REPLACE STAGE json_stage
FILE_FORMAT = (
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
);
```

### 2.2 File Formats

File formats define how Snowflake should interpret your data files:

```sql
-- Create CSV file format
CREATE OR REPLACE FILE FORMAT csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'null', '', 'N/A')
EMPTY_FIELD_AS_NULL = TRUE
COMPRESSION = 'AUTO'
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
VALIDATE_UTF8 = TRUE;

-- Create JSON file format
CREATE OR REPLACE FILE FORMAT json_format
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE
STRIP_NULL_VALUES = FALSE
IGNORE_UTF8_ERRORS = FALSE;

-- Create Parquet file format
CREATE OR REPLACE FILE FORMAT parquet_format
TYPE = 'PARQUET'
COMPRESSION = 'AUTO';

-- Show file formats
SHOW FILE FORMATS;
```

## 📊 Step 3: Data Loading Methods

### 3.1 COPY INTO Command

The COPY INTO command is the primary method for loading data:

```sql
-- Create target table for loading
CREATE OR REPLACE TABLE customers_from_file (
    customer_id NUMBER,
    customer_name STRING,
    email STRING,
    purchase_date DATE,
    product_category STRING,
    amount NUMBER(10,2),
    region STRING
);

-- Note: In a real scenario, you would first upload files to a stage
-- For this lab, we'll simulate with existing data

-- Create a simulated CSV in an internal stage using SELECT
-- First, let's create sample CSV data in a table
CREATE OR REPLACE TEMPORARY TABLE temp_csv_data AS
SELECT 
    customer_id || ',' || 
    '"' || customer_name || '",' ||
    '"' || email || '",' ||
    purchase_date || ',' ||
    '"' || product_category || '",' ||
    amount || ',' ||
    '"' || region || '"' as csv_line
FROM sample_data;

-- View the CSV format
SELECT csv_line FROM temp_csv_data;
```

### 3.2 Loading JSON Data

```sql
-- Create table for JSON data
CREATE OR REPLACE TABLE json_data (
    raw_data VARIANT
);

-- Create sample JSON data
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
}') as json_data
FROM sample_data;

-- Insert JSON data
INSERT INTO json_data
SELECT json_data FROM temp_json_source;

-- Query JSON data
SELECT 
    raw_data:customer_id::NUMBER as customer_id,
    raw_data:customer_name::STRING as customer_name,
    raw_data:email::STRING as email,
    raw_data:purchase_date::DATE as purchase_date,
    raw_data:product_category::STRING as product_category,
    raw_data:amount::NUMBER(10,2) as amount,
    raw_data:region::STRING as region,
    raw_data:metadata:source::STRING as source,
    raw_data:metadata:campaign::STRING as campaign
FROM json_data;
```

### 3.3 Bulk Loading with Transformations

```sql
-- Create target table with additional computed columns
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

-- Load with transformations
INSERT INTO enriched_customers (
    customer_id, customer_name, email, email_domain, 
    purchase_date, product_category, amount, amount_tier, region
)
SELECT 
    customer_id,
    UPPER(customer_name) as customer_name,
    LOWER(email) as email,
    SPLIT_PART(email, '@', 2) as email_domain,
    purchase_date,
    product_category,
    amount,
    CASE 
        WHEN amount < 50 THEN 'Low'
        WHEN amount < 200 THEN 'Medium'
        WHEN amount < 500 THEN 'High'
        ELSE 'Premium'
    END as amount_tier,
    region
FROM sample_data;

-- View enriched data
SELECT * FROM enriched_customers;
```

## 🏭 Step 4: Warehouse Management

### 4.1 Warehouse Sizing

```sql
-- Create warehouses of different sizes for comparison
CREATE OR REPLACE WAREHOUSE XS_WH 
WITH WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60;

CREATE OR REPLACE WAREHOUSE S_WH 
WITH WAREHOUSE_SIZE = 'SMALL' AUTO_SUSPEND = 60;

CREATE OR REPLACE WAREHOUSE M_WH 
WITH WAREHOUSE_SIZE = 'MEDIUM' AUTO_SUSPEND = 60;

-- Show all warehouses
SHOW WAREHOUSES;

-- Compare warehouse properties
SELECT 
    "name" as warehouse_name,
    "size" as warehouse_size,
    "min_cluster_count",
    "max_cluster_count",
    "auto_suspend",
    "auto_resume",
    "state"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
```

### 4.2 Performance Testing with Different Warehouse Sizes

```sql
-- Create a larger dataset for performance testing
CREATE OR REPLACE TABLE performance_test (
    id NUMBER,
    category STRING,
    subcategory STRING,
    amount NUMBER,
    date_created DATE,
    region STRING,
    customer_segment STRING
);

-- Generate 50,000 records for testing
INSERT INTO performance_test
SELECT 
    SEQ4() as id,
    CASE (UNIFORM(1, 5, RANDOM()))
        WHEN 1 THEN 'Electronics'
        WHEN 2 THEN 'Clothing'
        WHEN 3 THEN 'Books'
        WHEN 4 THEN 'Home'
        ELSE 'Sports'
    END as category,
    CASE (UNIFORM(1, 10, RANDOM()))
        WHEN 1 THEN 'Laptops' WHEN 2 THEN 'Phones' WHEN 3 THEN 'Accessories'
        WHEN 4 THEN 'Shirts' WHEN 5 THEN 'Pants' WHEN 6 THEN 'Shoes'
        WHEN 7 THEN 'Fiction' WHEN 8 THEN 'Non-Fiction' WHEN 9 THEN 'Textbooks'
        ELSE 'General'
    END as subcategory,
    ROUND(UNIFORM(10, 2000, RANDOM()), 2) as amount,
    DATEADD('day', -UNIFORM(1, 730, RANDOM()), CURRENT_DATE()) as date_created,
    CASE (UNIFORM(1, 4, RANDOM()))
        WHEN 1 THEN 'North' WHEN 2 THEN 'South' 
        WHEN 3 THEN 'East' ELSE 'West'
    END as region,
    CASE (UNIFORM(1, 3, RANDOM()))
        WHEN 1 THEN 'Enterprise'
        WHEN 2 THEN 'SMB'
        ELSE 'Consumer'
    END as customer_segment
FROM TABLE(GENERATOR(ROWCOUNT => 50000));

-- Test query performance on X-SMALL warehouse
USE WAREHOUSE XS_WH;

SELECT 
    category,
    subcategory,
    region,
    customer_segment,
    COUNT(*) as transaction_count,
    SUM(amount) as total_sales,
    AVG(amount) as avg_sale,
    MIN(amount) as min_sale,
    MAX(amount) as max_sale,
    STDDEV(amount) as stddev_amount
FROM performance_test
WHERE date_created >= '2023-01-01'
GROUP BY category, subcategory, region, customer_segment
HAVING COUNT(*) > 10
ORDER BY total_sales DESC;

-- Same query on SMALL warehouse (you can switch and re-run)
-- USE WAREHOUSE S_WH;
-- [Re-run the same query above]
```

### 4.3 Auto-Scaling Configuration

```sql
-- Create auto-scaling warehouse
CREATE OR REPLACE WAREHOUSE AUTO_SCALE_WH
WITH 
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Auto-scaling warehouse for variable workloads';

-- Modify existing warehouse to enable auto-scaling
ALTER WAREHOUSE LEARN_WH SET 
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'STANDARD';

-- Show warehouse configuration
SHOW WAREHOUSES LIKE 'AUTO_SCALE_WH';
```

## 🔄 Step 5: Data Pipeline Implementation

### 5.1 Creating a Data Pipeline

```sql
-- Create staging table for raw data
CREATE OR REPLACE TABLE raw_sales_staging (
    raw_data VARIANT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    file_name STRING,
    file_row_number NUMBER
);

-- Create final processed table
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

-- Simulate loading raw JSON data into staging
INSERT INTO raw_sales_staging (raw_data, file_name, file_row_number)
SELECT 
    PARSE_JSON('{
        "sale_id": ' || (ROW_NUMBER() OVER (ORDER BY RANDOM())) || ',
        "customer_id": ' || UNIFORM(1001, 9999, RANDOM()) || ',
        "customer_name": "Customer ' || UNIFORM(1, 1000, RANDOM()) || '",
        "product_id": ' || UNIFORM(1, 100, RANDOM()) || ',
        "product_name": "Product ' || UNIFORM(1, 100, RANDOM()) || '",
        "category": "' || 
            CASE (UNIFORM(1, 4, RANDOM()))
                WHEN 1 THEN 'Electronics'
                WHEN 2 THEN 'Clothing'
                WHEN 3 THEN 'Books'
                ELSE 'Home'
            END || '",
        "sale_date": "' || DATEADD('day', -UNIFORM(1, 90, RANDOM()), CURRENT_DATE()) || '",
        "quantity": ' || UNIFORM(1, 10, RANDOM()) || ',
        "unit_price": ' || ROUND(UNIFORM(10, 500, RANDOM()), 2) || ',
        "discount_percent": ' || ROUND(UNIFORM(0, 25, RANDOM()), 2) || ',
        "sales_rep": "Rep' || UNIFORM(1, 20, RANDOM()) || '",
        "region": "' || 
            CASE (UNIFORM(1, 4, RANDOM()))
                WHEN 1 THEN 'North'
                WHEN 2 THEN 'South'
                WHEN 3 THEN 'East'
                ELSE 'West'
            END || '"
    }') as raw_data,
    'sales_batch_' || TO_CHAR(CURRENT_DATE(), 'YYYY_MM_DD') || '.json' as file_name,
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as file_row_number
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

-- Transform and load data from staging to processed table
INSERT INTO processed_sales (
    sale_id, customer_id, customer_name, product_id, product_name,
    category, sale_date, quantity, unit_price, total_amount,
    discount_percent, final_amount, sales_rep, region
)
SELECT 
    raw_data:sale_id::NUMBER as sale_id,
    raw_data:customer_id::NUMBER as customer_id,
    raw_data:customer_name::STRING as customer_name,
    raw_data:product_id::NUMBER as product_id,
    raw_data:product_name::STRING as product_name,
    raw_data:category::STRING as category,
    raw_data:sale_date::DATE as sale_date,
    raw_data:quantity::NUMBER as quantity,
    raw_data:unit_price::NUMBER(10,2) as unit_price,
    (raw_data:quantity::NUMBER * raw_data:unit_price::NUMBER) as total_amount,
    raw_data:discount_percent::NUMBER(5,2) as discount_percent,
    ROUND(
        (raw_data:quantity::NUMBER * raw_data:unit_price::NUMBER) * 
        (1 - raw_data:discount_percent::NUMBER / 100), 2
    ) as final_amount,
    raw_data:sales_rep::STRING as sales_rep,
    raw_data:region::STRING as region
FROM raw_sales_staging
WHERE raw_data IS NOT NULL;

-- Validate the pipeline
SELECT 
    COUNT(*) as total_sales,
    SUM(final_amount) as total_revenue,
    AVG(final_amount) as avg_sale,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT sales_rep) as sales_reps
FROM processed_sales;
```

### 5.2 Data Quality Checks

```sql
-- Create data quality check table
CREATE OR REPLACE TABLE data_quality_results (
    check_name STRING,
    check_description STRING,
    result_count NUMBER,
    expected_count NUMBER,
    status STRING,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Implement data quality checks
INSERT INTO data_quality_results (check_name, check_description, result_count, expected_count, status)
VALUES
-- Check for null customer IDs
('null_customer_ids', 'Count of records with null customer_id', 
 (SELECT COUNT(*) FROM processed_sales WHERE customer_id IS NULL), 
 0, 
 CASE WHEN (SELECT COUNT(*) FROM processed_sales WHERE customer_id IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END),

-- Check for negative amounts
('negative_amounts', 'Count of records with negative final_amount',
 (SELECT COUNT(*) FROM processed_sales WHERE final_amount < 0),
 0,
 CASE WHEN (SELECT COUNT(*) FROM processed_sales WHERE final_amount < 0) = 0 THEN 'PASS' ELSE 'FAIL' END),

-- Check for future dates
('future_dates', 'Count of records with future sale_date',
 (SELECT COUNT(*) FROM processed_sales WHERE sale_date > CURRENT_DATE()),
 0,
 CASE WHEN (SELECT COUNT(*) FROM processed_sales WHERE sale_date > CURRENT_DATE()) = 0 THEN 'PASS' ELSE 'FAIL' END),

-- Check for reasonable discount range
('invalid_discounts', 'Count of records with discount > 50%',
 (SELECT COUNT(*) FROM processed_sales WHERE discount_percent > 50),
 0,
 CASE WHEN (SELECT COUNT(*) FROM processed_sales WHERE discount_percent > 50) = 0 THEN 'PASS' ELSE 'FAIL' END);

-- View quality check results
SELECT * FROM data_quality_results ORDER BY check_timestamp DESC;
```

## 📈 Step 6: Monitoring and Optimization

### 6.1 Query Performance Monitoring

```sql
-- Check recent query performance
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

-- Warehouse usage summary
SELECT 
    warehouse_name,
    COUNT(*) as query_count,
    AVG(total_elapsed_time) as avg_execution_time,
    SUM(total_elapsed_time) as total_execution_time,
    SUM(bytes_scanned) as total_bytes_scanned,
    SUM(rows_produced) as total_rows_produced
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
  AND warehouse_name IS NOT NULL
GROUP BY warehouse_name
ORDER BY total_execution_time DESC;
```

### 6.2 Storage and Cost Monitoring

```sql
-- Check table storage usage
SELECT 
    table_catalog,
    table_schema,
    table_name,
    row_count,
    bytes,
    ROUND(bytes / (1024 * 1024 * 1024), 2) as size_gb,
    ROUND(bytes / row_count, 0) as avg_bytes_per_row
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'SANDBOX'
  AND table_type = 'BASE TABLE'
ORDER BY bytes DESC;

-- Warehouse credit usage (last 7 days)
SELECT 
    warehouse_name,
    SUM(credits_used) as total_credits,
    AVG(credits_used) as avg_credits_per_hour,
    COUNT(*) as active_hours
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY warehouse_name
ORDER BY total_credits DESC;
```

## 🧹 Step 7: Cleanup and Best Practices

### 7.1 Cleanup Commands

```sql
-- Clean up temporary objects
DROP TABLE IF EXISTS temp_csv_data;
DROP TABLE IF EXISTS temp_json_source;

-- Suspend all warehouses to save credits
ALTER WAREHOUSE XS_WH SUSPEND;
ALTER WAREHOUSE S_WH SUSPEND;
ALTER WAREHOUSE M_WH SUSPEND;
ALTER WAREHOUSE AUTO_SCALE_WH SUSPEND;

-- Show final warehouse status
SHOW WAREHOUSES;
```

### 7.2 Best Practices Summary

1. **File Formats**: Choose appropriate formats (Parquet for analytics, JSON for flexibility)
2. **Staging**: Use stages for organized data loading
3. **Transformations**: Apply transformations during loading when possible
4. **Warehouse Sizing**: Start small, scale as needed
5. **Auto-suspend**: Always configure auto-suspend to control costs
6. **Monitoring**: Regularly check query performance and costs
7. **Data Quality**: Implement checks at every stage

## ✅ Lab Completion Checklist

- [ ] Understood different file formats and their use cases
- [ ] Created and configured internal stages
- [ ] Implemented various data loading methods
- [ ] Tested different warehouse sizes and configurations
- [ ] Built a complete data pipeline with transformations
- [ ] Implemented data quality checks
- [ ] Monitored query performance and warehouse usage
- [ ] Applied cost optimization best practices

## 🎉 Congratulations!

You've mastered data loading and warehouse management! Key achievements:
- Created efficient data pipelines
- Optimized warehouse performance and costs
- Implemented quality control processes
- Learned monitoring and optimization techniques

## 🔜 Next Steps

Continue with [Lab 04: Advanced SQL & Functions](../lab04/) to explore:
- Complex analytical functions
- User-defined functions (UDFs)
- Stored procedures
- Advanced data manipulation techniques

## 🆘 Troubleshooting

### Common Issues:

**Issue**: COPY INTO command fails
**Solution**: Check file format settings and ensure data matches expected schema

**Issue**: Warehouse takes too long to start
**Solution**: Use AUTO_RESUME = TRUE for faster startup

**Issue**: High credit consumption
**Solution**: Implement proper AUTO_SUSPEND settings and right-size warehouses

**Issue**: Data quality failures
**Solution**: Review source data and adjust transformation logic

## 📚 Additional Resources

- [Data Loading Guide](https://docs.snowflake.com/en/user-guide-data-load.html)
- [Warehouse Management](https://docs.snowflake.com/en/user-guide/warehouses.html)
- [File Format Options](https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html)