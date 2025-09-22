# Lab 04: Advanced SQL Functions & User-Defined Functions

## 🎯 Objectives
By the end of this lab, you will:
- Master advanced analytical functions
- Create User-Defined Functions (UDFs)
- Implement stored procedures
- Use advanced data manipulation techniques
- Build complex data transformations

## ⏱️ Estimated Time: 60 minutes

## 📋 Prerequisites
- Completed Labs 01-03
- Strong understanding of SQL fundamentals
- Knowledge of programming concepts

## 🔧 Step 1: Advanced Analytical Functions

### 1.1 Complex Window Functions

```sql
-- Set up environment
USE WAREHOUSE LEARN_WH;
USE DATABASE LEARN_SNOWFLAKE;
USE SCHEMA SANDBOX;

-- Create comprehensive sales data for analysis
CREATE OR REPLACE TABLE advanced_sales (
    sale_id NUMBER,
    customer_id NUMBER,
    product_id NUMBER,
    sale_date DATE,
    amount NUMBER(10,2),
    quantity NUMBER,
    sales_rep STRING,
    region STRING,
    product_category STRING
);

-- Insert sample data
INSERT INTO advanced_sales 
SELECT 
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as sale_id,
    UNIFORM(1001, 1100, RANDOM()) as customer_id,
    UNIFORM(1, 50, RANDOM()) as product_id,
    DATEADD('day', -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) as sale_date,
    ROUND(UNIFORM(50, 2000, RANDOM()), 2) as amount,
    UNIFORM(1, 10, RANDOM()) as quantity,
    CASE (UNIFORM(1, 8, RANDOM()))
        WHEN 1 THEN 'Alice Smith'
        WHEN 2 THEN 'Bob Johnson' 
        WHEN 3 THEN 'Carol Davis'
        WHEN 4 THEN 'David Wilson'
        WHEN 5 THEN 'Eve Brown'
        WHEN 6 THEN 'Frank Miller'
        WHEN 7 THEN 'Grace Lee'
        ELSE 'Henry Taylor'
    END as sales_rep,
    CASE (UNIFORM(1, 4, RANDOM()))
        WHEN 1 THEN 'North'
        WHEN 2 THEN 'South'
        WHEN 3 THEN 'East'
        ELSE 'West'
    END as region,
    CASE (UNIFORM(1, 5, RANDOM()))
        WHEN 1 THEN 'Electronics'
        WHEN 2 THEN 'Clothing'
        WHEN 3 THEN 'Home'
        WHEN 4 THEN 'Books'
        ELSE 'Sports'
    END as product_category
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

-- Advanced window function analysis
SELECT 
    sales_rep,
    region,
    sale_date,
    amount,
    -- Running totals
    SUM(amount) OVER (PARTITION BY sales_rep ORDER BY sale_date) as running_total,
    
    -- Moving averages
    AVG(amount) OVER (PARTITION BY sales_rep ORDER BY sale_date 
                     ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg_7day,
    
    -- Percentile rankings
    PERCENT_RANK() OVER (PARTITION BY region ORDER BY amount) as amount_percentile,
    
    -- Quartiles
    NTILE(4) OVER (PARTITION BY product_category ORDER BY amount) as amount_quartile,
    
    -- Lead/Lag analysis
    LAG(amount, 1) OVER (PARTITION BY sales_rep ORDER BY sale_date) as prev_sale,
    LEAD(amount, 1) OVER (PARTITION BY sales_rep ORDER BY sale_date) as next_sale,
    
    -- First/Last values
    FIRST_VALUE(amount) OVER (PARTITION BY sales_rep ORDER BY sale_date 
                             ROWS UNBOUNDED PRECEDING) as first_sale_amount,
    LAST_VALUE(amount) OVER (PARTITION BY sales_rep ORDER BY sale_date 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_sale_amount,
    
    -- Ratio calculations
    amount / SUM(amount) OVER (PARTITION BY sales_rep) * 100 as pct_of_rep_total,
    
    -- Dense rank for tie handling
    DENSE_RANK() OVER (PARTITION BY region ORDER BY amount DESC) as region_rank
FROM advanced_sales
ORDER BY sales_rep, sale_date
LIMIT 100;
```

### 1.2 Advanced Aggregation Functions

```sql
-- Advanced aggregation examples
SELECT 
    region,
    product_category,
    
    -- Standard aggregations
    COUNT(*) as transaction_count,
    SUM(amount) as total_sales,
    AVG(amount) as avg_sale,
    
    -- Statistical functions
    STDDEV(amount) as std_deviation,
    VARIANCE(amount) as variance,
    
    -- Percentile functions
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount) as q1,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount) as q3,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount) as p90,
    
    -- Mode calculation (most frequent value)
    MODE(quantity) as most_common_quantity,
    
    -- Collect distinct values
    LISTAGG(DISTINCT sales_rep, ', ') as sales_reps,
    
    -- Count distinct
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT product_id) as unique_products

FROM advanced_sales
GROUP BY region, product_category
ORDER BY total_sales DESC;
```

## 🛠️ Step 2: User-Defined Functions (UDFs)

### 2.1 SQL UDFs

```sql
-- Create simple SQL UDF for calculating discount
CREATE OR REPLACE FUNCTION calculate_discount(amount NUMBER, discount_pct NUMBER)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
    amount * (discount_pct / 100)
$$;

-- Create UDF for sales tier classification
CREATE OR REPLACE FUNCTION classify_sale_tier(amount NUMBER)
RETURNS STRING
LANGUAGE SQL
AS
$$
    CASE 
        WHEN amount < 100 THEN 'Low'
        WHEN amount < 500 THEN 'Medium'
        WHEN amount < 1000 THEN 'High'
        ELSE 'Premium'
    END
$$;

-- Create UDF for business days calculation
CREATE OR REPLACE FUNCTION business_days_between(start_date DATE, end_date DATE)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
    DATEDIFF('day', start_date, end_date) - 
    (DATEDIFF('week', start_date, end_date) * 2) -
    CASE WHEN DAYOFWEEK(start_date) = 1 THEN 1 ELSE 0 END -
    CASE WHEN DAYOFWEEK(end_date) = 7 THEN 1 ELSE 0 END
$$;

-- Test the UDFs
SELECT 
    sale_id,
    amount,
    calculate_discount(amount, 10) as discount_10pct,
    classify_sale_tier(amount) as tier,
    business_days_between(sale_date, CURRENT_DATE()) as business_days_ago
FROM advanced_sales
LIMIT 20;
```

### 2.2 JavaScript UDFs

```sql
-- Create JavaScript UDF for complex calculations
CREATE OR REPLACE FUNCTION calculate_compound_interest(
    principal NUMBER, 
    rate NUMBER, 
    time_years NUMBER, 
    compound_frequency NUMBER
)
RETURNS NUMBER
LANGUAGE JAVASCRIPT
AS
$$
    if (PRINCIPAL <= 0 || RATE <= 0 || TIME_YEARS <= 0 || COMPOUND_FREQUENCY <= 0) {
        return 0;
    }
    
    var amount = PRINCIPAL * Math.pow((1 + RATE / COMPOUND_FREQUENCY), 
                                     COMPOUND_FREQUENCY * TIME_YEARS);
    return Math.round(amount * 100) / 100;
$$;

-- Create JavaScript UDF for text processing
CREATE OR REPLACE FUNCTION extract_domain(email STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    if (!EMAIL || typeof EMAIL !== 'string') {
        return null;
    }
    
    var atIndex = EMAIL.indexOf('@');
    if (atIndex === -1) {
        return null;
    }
    
    return EMAIL.substring(atIndex + 1).toLowerCase();
$$;

-- Create JavaScript UDF for data validation
CREATE OR REPLACE FUNCTION validate_phone_number(phone STRING)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    if (!PHONE || typeof PHONE !== 'string') {
        return false;
    }
    
    // Remove all non-digit characters
    var digits = PHONE.replace(/\D/g, '');
    
    // Check if it's a valid US phone number (10 digits)
    return digits.length === 10 || (digits.length === 11 && digits[0] === '1');
$$;

-- Test JavaScript UDFs
SELECT 
    calculate_compound_interest(1000, 0.05, 5, 12) as compound_interest,
    extract_domain('user@example.com') as domain,
    validate_phone_number('(555) 123-4567') as is_valid_phone;
```

## 📊 Step 3: Stored Procedures

### 3.1 Data Processing Procedure

```sql
-- Create stored procedure for data quality analysis
CREATE OR REPLACE PROCEDURE analyze_data_quality(table_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result_text STRING DEFAULT '';
    total_rows NUMBER;
    null_counts STRING;
    duplicate_count NUMBER;
    sql_query STRING;
BEGIN
    -- Get total row count
    sql_query := 'SELECT COUNT(*) FROM ' || table_name;
    EXECUTE IMMEDIATE sql_query INTO total_rows;
    
    result_text := 'Data Quality Analysis for ' || table_name || '\n';
    result_text := result_text || 'Total Rows: ' || total_rows || '\n';
    
    -- Check for duplicates (this is simplified)
    sql_query := 'SELECT COUNT(*) - COUNT(DISTINCT *) FROM ' || table_name;
    EXECUTE IMMEDIATE sql_query INTO duplicate_count;
    result_text := result_text || 'Duplicate Rows: ' || duplicate_count || '\n';
    
    RETURN result_text;
END;
$$;

-- Create stored procedure for sales reporting
CREATE OR REPLACE PROCEDURE generate_sales_report(
    start_date DATE, 
    end_date DATE,
    region_filter STRING DEFAULT NULL
)
RETURNS TABLE (
    region STRING,
    total_sales NUMBER,
    transaction_count NUMBER,
    avg_sale NUMBER,
    top_sales_rep STRING
)
LANGUAGE SQL
AS
$$
DECLARE
    sql_query STRING;
BEGIN
    sql_query := '
        WITH sales_summary AS (
            SELECT 
                region,
                SUM(amount) as total_sales,
                COUNT(*) as transaction_count,
                AVG(amount) as avg_sale
            FROM advanced_sales
            WHERE sale_date BETWEEN ? AND ?';
    
    IF (region_filter IS NOT NULL) THEN
        sql_query := sql_query || ' AND region = ?';
    END IF;
    
    sql_query := sql_query || '
            GROUP BY region
        ),
        top_reps AS (
            SELECT 
                region,
                sales_rep,
                SUM(amount) as rep_sales,
                ROW_NUMBER() OVER (PARTITION BY region ORDER BY SUM(amount) DESC) as rn
            FROM advanced_sales
            WHERE sale_date BETWEEN ? AND ?';
    
    IF (region_filter IS NOT NULL) THEN
        sql_query := sql_query || ' AND region = ?';
    END IF;
    
    sql_query := sql_query || '
            GROUP BY region, sales_rep
        )
        SELECT 
            s.region,
            s.total_sales,
            s.transaction_count,
            s.avg_sale,
            t.sales_rep as top_sales_rep
        FROM sales_summary s
        LEFT JOIN top_reps t ON s.region = t.region AND t.rn = 1
        ORDER BY s.total_sales DESC';
    
    -- Create result table
    CREATE OR REPLACE TEMPORARY TABLE temp_sales_report AS
    SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    
    -- Return the result
    RETURN TABLE(SELECT * FROM temp_sales_report);
END;
$$;

-- Test the stored procedures
CALL analyze_data_quality('advanced_sales');

CALL generate_sales_report('2023-01-01', '2023-12-31', NULL);
```

### 3.2 ETL Procedure

```sql
-- Create comprehensive ETL stored procedure
CREATE OR REPLACE PROCEDURE etl_daily_sales_processing()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_processed NUMBER DEFAULT 0;
    error_count NUMBER DEFAULT 0;
    result_message STRING;
BEGIN
    -- Start transaction
    BEGIN TRANSACTION;
    
    -- Create staging table
    CREATE OR REPLACE TEMPORARY TABLE daily_sales_staging AS
    SELECT * FROM advanced_sales WHERE 1=0; -- Empty table with same structure
    
    -- Simulate data extraction (in real scenario, this would load from external source)
    INSERT INTO daily_sales_staging
    SELECT * FROM advanced_sales
    WHERE sale_date = CURRENT_DATE() - 1;
    
    GET DIAGNOSTICS rows_processed = ROW_COUNT;
    
    -- Data validation
    DELETE FROM daily_sales_staging 
    WHERE amount <= 0 OR customer_id IS NULL;
    
    GET DIAGNOSTICS error_count = ROW_COUNT;
    
    -- Data transformation and loading
    MERGE INTO advanced_sales AS target
    USING daily_sales_staging AS source
    ON target.sale_id = source.sale_id
    WHEN MATCHED THEN
        UPDATE SET 
            amount = source.amount,
            quantity = source.quantity
    WHEN NOT MATCHED THEN
        INSERT VALUES (
            source.sale_id, source.customer_id, source.product_id,
            source.sale_date, source.amount, source.quantity,
            source.sales_rep, source.region, source.product_category
        );
    
    -- Commit transaction
    COMMIT;
    
    result_message := 'ETL Process Completed Successfully. ' ||
                     'Rows Processed: ' || rows_processed ||
                     ', Errors Corrected: ' || error_count;
    
    RETURN result_message;
    
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'ETL Process Failed: ' || SQLERRM;
END;
$$;

-- Execute ETL procedure
CALL etl_daily_sales_processing();
```

## 🔄 Step 4: Advanced Data Transformations

### 4.1 Pivot and Unpivot Operations

```sql
-- Create sample data for pivot operations
CREATE OR REPLACE TABLE monthly_sales AS
SELECT 
    sales_rep,
    MONTHNAME(sale_date) as month_name,
    SUM(amount) as total_sales
FROM advanced_sales
WHERE YEAR(sale_date) = 2023
GROUP BY sales_rep, MONTHNAME(sale_date), MONTH(sale_date)
ORDER BY sales_rep, MONTH(sale_date);

-- Pivot sales data by month
SELECT *
FROM monthly_sales
PIVOT(SUM(total_sales) FOR month_name IN (
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
)) AS pivoted_sales
ORDER BY sales_rep;

-- Create pivoted table for unpivot example
CREATE OR REPLACE TABLE sales_by_quarter AS
SELECT 
    region,
    SUM(CASE WHEN QUARTER(sale_date) = 1 THEN amount ELSE 0 END) as Q1,
    SUM(CASE WHEN QUARTER(sale_date) = 2 THEN amount ELSE 0 END) as Q2,
    SUM(CASE WHEN QUARTER(sale_date) = 3 THEN amount ELSE 0 END) as Q3,
    SUM(CASE WHEN QUARTER(sale_date) = 4 THEN amount ELSE 0 END) as Q4
FROM advanced_sales
WHERE YEAR(sale_date) = 2023
GROUP BY region;

-- Unpivot quarterly data
SELECT 
    region,
    quarter,
    sales_amount
FROM sales_by_quarter
UNPIVOT(sales_amount FOR quarter IN (Q1, Q2, Q3, Q4))
ORDER BY region, quarter;
```

### 4.2 Advanced JSON Processing

```sql
-- Create table with JSON data
CREATE OR REPLACE TABLE product_catalog (
    product_id NUMBER,
    product_info VARIANT
);

-- Insert sample JSON data
INSERT INTO product_catalog VALUES
(1, PARSE_JSON('{"name": "Laptop Pro", "category": "Electronics", "specs": {"cpu": "Intel i7", "ram": "16GB", "storage": "512GB SSD"}, "price": 1299.99, "tags": ["business", "portable", "high-performance"]}')),
(2, PARSE_JSON('{"name": "Running Shoes", "category": "Sports", "specs": {"size": "10", "color": "blue", "material": "mesh"}, "price": 129.99, "tags": ["running", "comfortable", "lightweight"]}')),
(3, PARSE_JSON('{"name": "Coffee Maker", "category": "Home", "specs": {"capacity": "12 cups", "features": ["programmable", "auto-shutoff"]}, "price": 89.99, "tags": ["kitchen", "appliance", "coffee"]}'));

-- Advanced JSON queries
SELECT 
    product_id,
    product_info:name::STRING as product_name,
    product_info:category::STRING as category,
    product_info:price::NUMBER(10,2) as price,
    
    -- Extract nested objects
    product_info:specs:cpu::STRING as cpu,
    product_info:specs:ram::STRING as ram,
    
    -- Array operations
    ARRAY_SIZE(product_info:tags) as tag_count,
    product_info:tags[0]::STRING as first_tag,
    
    -- Check if key exists
    IFNULL(product_info:specs:cpu::STRING, 'N/A') as cpu_info,
    
    -- Convert array to string
    ARRAY_TO_STRING(product_info:tags, ', ') as all_tags
FROM product_catalog;

-- Flatten JSON arrays
SELECT 
    product_id,
    product_info:name::STRING as product_name,
    f.value::STRING as tag
FROM product_catalog,
LATERAL FLATTEN(input => product_info:tags) f;
```

## ✅ Lab Completion Checklist

- [ ] Mastered complex window functions and analytical queries
- [ ] Created and tested SQL User-Defined Functions
- [ ] Implemented JavaScript UDFs for complex logic
- [ ] Built stored procedures for data processing
- [ ] Developed ETL procedures with error handling
- [ ] Used PIVOT and UNPIVOT for data reshaping
- [ ] Processed complex JSON data structures
- [ ] Applied advanced data transformation techniques

## 🎉 Congratulations!

You've mastered advanced SQL functions and programming in Snowflake! You can now:
- Build sophisticated analytical queries
- Create reusable functions for complex business logic
- Implement robust data processing procedures
- Handle complex data transformations and JSON processing

## 🔜 Next Steps

Continue with [Lab 05: Python Integration with Snowflake](../lab05/) to learn how to:
- Connect to Snowflake using Python
- Integrate with pandas and other data science libraries
- Build automated data pipelines
- Create interactive applications

## 🆘 Troubleshooting

### Common Issues:

**Issue**: UDF creation fails
**Solution**: Check syntax and ensure proper parameter types

**Issue**: Stored procedure execution errors
**Solution**: Use proper exception handling and validate input parameters

**Issue**: JSON parsing errors
**Solution**: Validate JSON structure before parsing

## 📚 Additional Resources

- [User-Defined Functions](https://docs.snowflake.com/en/sql-reference/user-defined-functions.html)
- [Stored Procedures](https://docs.snowflake.com/en/sql-reference/stored-procedures.html)
- [Semi-structured Data](https://docs.snowflake.com/en/user-guide/semistructured-concepts.html)