# Lab 04 - Snowflake SQL Syntax Reference Guide

## 📖 Overview
This document provides comprehensive syntax reference for all SQL scripts and examples used in Lab 04: Advanced SQL Functions & User-Defined Functions.

## 🔍 Quick Reference Index
- [Environment Setup](#environment-setup)
- [Advanced Window Functions](#advanced-window-functions)
- [Advanced Aggregation Functions](#advanced-aggregation-functions)
- [User-Defined Functions (SQL)](#user-defined-functions-sql)
- [User-Defined Functions (JavaScript)](#user-defined-functions-javascript)
- [Stored Procedures](#stored-procedures)
- [Data Transformation Operations](#data-transformation-operations)
- [JSON Processing](#json-processing)

---

## Environment Setup

### Context Setting
```sql
USE WAREHOUSE warehouse_name;
USE DATABASE database_name;
USE SCHEMA schema_name;
```

**Purpose**: Sets the active warehouse, database, and schema for the session.

**Parameters**:
- `warehouse_name`: Name of the virtual warehouse to use
- `database_name`: Name of the database to use
- `schema_name`: Name of the schema to use

---

## Advanced Window Functions

### Window Function Syntax
```sql
function_name([arguments]) OVER (
    [PARTITION BY partition_expression, ...]
    [ORDER BY sort_expression [ASC|DESC], ...]
    [frame_specification]
)
```

### 1. Running Totals
```sql
SUM(column_name) OVER (PARTITION BY partition_col ORDER BY order_col)
```

**Purpose**: Calculates cumulative sum within each partition.

**Parameters**:
- `column_name`: Column to sum
- `partition_col`: Column to group by
- `order_col`: Column to order the cumulative calculation

### 2. Moving Averages
```sql
AVG(column_name) OVER (
    PARTITION BY partition_col 
    ORDER BY order_col 
    ROWS BETWEEN n PRECEDING AND CURRENT ROW
)
```

**Purpose**: Calculates moving average over specified number of rows.

**Parameters**:
- `n`: Number of preceding rows to include (e.g., 6 for 7-day moving average)

### 3. Ranking Functions
```sql
-- Simple ranking (allows gaps)
RANK() OVER (ORDER BY column_name DESC)

-- Dense ranking (no gaps)
DENSE_RANK() OVER (ORDER BY column_name DESC)

-- Row numbering (sequential)
ROW_NUMBER() OVER (ORDER BY column_name DESC)

-- Percentile ranking
PERCENT_RANK() OVER (ORDER BY column_name)

-- Quartiles
NTILE(4) OVER (ORDER BY column_name)
```

### 4. Lead and Lag Functions
```sql
-- Previous value
LAG(column_name, offset) OVER (PARTITION BY partition_col ORDER BY order_col)

-- Next value
LEAD(column_name, offset) OVER (PARTITION BY partition_col ORDER BY order_col)
```

**Parameters**:
- `offset`: Number of rows to look back/forward (default: 1)

### 5. First and Last Value Functions
```sql
-- First value in window
FIRST_VALUE(column_name) OVER (
    PARTITION BY partition_col 
    ORDER BY order_col 
    ROWS UNBOUNDED PRECEDING
)

-- Last value in window
LAST_VALUE(column_name) OVER (
    PARTITION BY partition_col 
    ORDER BY order_col 
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
```

### Frame Specifications
- `ROWS UNBOUNDED PRECEDING`: From start of partition to current row
- `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`: Entire partition
- `ROWS BETWEEN n PRECEDING AND CURRENT ROW`: n rows before to current
- `ROWS BETWEEN CURRENT ROW AND n FOLLOWING`: Current to n rows after

---

## Advanced Aggregation Functions

### Statistical Functions
```sql
-- Standard deviation
STDDEV(column_name)

-- Variance
VARIANCE(column_name)

-- Most frequent value
MODE(column_name)
```

### Percentile Functions
```sql
-- Continuous percentile (interpolates between values)
PERCENTILE_CONT(percentile) WITHIN GROUP (ORDER BY column_name)

-- Discrete percentile (returns actual values)
PERCENTILE_DISC(percentile) WITHIN GROUP (ORDER BY column_name)
```

**Parameters**:
- `percentile`: Decimal between 0 and 1 (e.g., 0.5 for median, 0.25 for Q1)

### String Aggregation
```sql
-- Concatenate values with separator
LISTAGG([DISTINCT] column_name, 'separator') [WITHIN GROUP (ORDER BY sort_col)]
```

**Parameters**:
- `DISTINCT`: Optional, removes duplicates
- `separator`: String to separate values
- `sort_col`: Optional column for ordering

---

## User-Defined Functions (SQL)

### Basic SQL UDF Syntax
```sql
CREATE [OR REPLACE] FUNCTION function_name(parameter_name parameter_type, ...)
RETURNS return_type
LANGUAGE SQL
AS
$$
    function_body
$$;
```

### Example: Discount Calculator
```sql
CREATE OR REPLACE FUNCTION calculate_discount(amount NUMBER, discount_pct NUMBER)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
    amount * (discount_pct / 100)
$$;
```

### Example: Classification Function
```sql
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
```

### Example: Date Calculation Function
```sql
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
```

---

## User-Defined Functions (JavaScript)

### JavaScript UDF Syntax
```sql
CREATE [OR REPLACE] FUNCTION function_name(parameter_name parameter_type, ...)
RETURNS return_type
LANGUAGE JAVASCRIPT
AS
$$
    javascript_code
$$;
```

### Example: Compound Interest Calculator
```sql
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
```

### Example: Text Processing Function
```sql
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
```

### JavaScript UDF Key Points
- Parameter names in JavaScript are UPPERCASE
- Use JavaScript syntax and built-in functions
- Return `null` for invalid inputs
- Handle type checking for robustness

---

## Stored Procedures

### Basic Stored Procedure Syntax
```sql
CREATE [OR REPLACE] PROCEDURE procedure_name(parameter_name parameter_type, ...)
RETURNS return_type
LANGUAGE SQL
AS
$$
DECLARE
    variable_name data_type [DEFAULT default_value];
BEGIN
    procedure_body
END;
$$;
```

### Table-Returning Procedure Syntax
```sql
CREATE [OR REPLACE] PROCEDURE procedure_name(parameters...)
RETURNS TABLE (
    column1_name data_type,
    column2_name data_type,
    ...
)
LANGUAGE SQL
AS
$$
DECLARE
    variables...
BEGIN
    procedure_body
    RETURN TABLE(SELECT ...);
END;
$$;
```

### Dynamic SQL in Procedures
```sql
DECLARE
    sql_query STRING;
BEGIN
    sql_query := 'SELECT * FROM ' || table_name || ' WHERE condition';
    EXECUTE IMMEDIATE sql_query INTO result_variable;
END;
```

### Exception Handling
```sql
BEGIN
    -- Transaction logic
    BEGIN TRANSACTION;
    -- SQL operations
    COMMIT;
    
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'Error: ' || SQLERRM;
END;
```

### Conditional Logic
```sql
IF (condition) THEN
    statements;
ELSEIF (condition) THEN
    statements;
ELSE
    statements;
END IF;
```

---

## Data Transformation Operations

### PIVOT Syntax
```sql
SELECT *
FROM source_table
PIVOT(
    aggregate_function(value_column) 
    FOR pivot_column 
    IN (value1, value2, value3, ...)
) AS alias;
```

**Example**:
```sql
SELECT *
FROM monthly_sales
PIVOT(SUM(total_sales) FOR month_name IN (
    'January', 'February', 'March', 'April'
)) AS pivoted_sales;
```

### UNPIVOT Syntax
```sql
SELECT *
FROM source_table
UNPIVOT(
    value_column_name FOR name_column_name 
    IN (column1, column2, column3, ...)
) AS alias;
```

**Example**:
```sql
SELECT region, quarter, sales_amount
FROM sales_by_quarter
UNPIVOT(sales_amount FOR quarter IN (Q1, Q2, Q3, Q4));
```

### MERGE Statement Syntax
```sql
MERGE INTO target_table AS target
USING source_table AS source
ON join_condition
WHEN MATCHED THEN
    UPDATE SET column1 = source.column1, column2 = source.column2
WHEN NOT MATCHED THEN
    INSERT (columns...) VALUES (source.columns...);
```

---

## JSON Processing

### JSON Parsing Functions
```sql
-- Parse JSON string
PARSE_JSON('{"key": "value"}')

-- Extract JSON values
json_column:key::data_type
json_column:nested.key::data_type
```

### JSON Array Operations
```sql
-- Array size
ARRAY_SIZE(json_column:array_key)

-- Array element access
json_column:array_key[index]::data_type

-- Convert array to string
ARRAY_TO_STRING(json_column:array_key, 'separator')
```

### FLATTEN Function for JSON Arrays
```sql
SELECT 
    main_columns,
    f.value::data_type as array_element
FROM table_name,
LATERAL FLATTEN(input => json_column:array_key) f;
```

### JSON Existence Check
```sql
-- Check if key exists with null handling
IFNULL(json_column:key::data_type, 'default_value')

-- Alternative existence check
CASE 
    WHEN json_column:key IS NULL THEN 'Not found'
    ELSE json_column:key::STRING
END
```

---

## Data Generation and Random Functions

### GENERATOR Function
```sql
SELECT columns
FROM TABLE(GENERATOR(ROWCOUNT => number_of_rows));
```

### Random Functions
```sql
-- Random number between min and max
UNIFORM(min_value, max_value, RANDOM())

-- Random floating point
RANDOM()

-- Round to decimal places
ROUND(number, decimal_places)
```

### Date Generation
```sql
-- Subtract random days from current date
DATEADD('day', -UNIFORM(1, 365, RANDOM()), CURRENT_DATE())
```

---

## Data Types and Conversions

### Common Data Types
- `NUMBER`: Integer or decimal numbers
- `NUMBER(precision, scale)`: Numbers with specific precision and scale
- `STRING` or `VARCHAR`: Text data
- `DATE`: Date values
- `TIMESTAMP` or `TIMESTAMP_NTZ`: Date and time values
- `BOOLEAN`: True/false values
- `VARIANT`: Semi-structured data (JSON, XML, etc.)
- `ARRAY`: Array data type
- `OBJECT`: Object data type

### Type Casting
```sql
-- Explicit casting
column_name::data_type

-- Function-based conversion
TO_NUMBER(string_value)
TO_DATE(string_value, 'format')
TO_CHAR(value, 'format')
```

---

## Best Practices and Tips

### 1. Window Functions
- Use appropriate frame specifications for the intended calculation
- Consider performance impact of large partitions
- Use `QUALIFY` clause to filter window function results

### 2. UDFs
- Keep UDF logic simple for better performance
- Use SQL UDFs for simple logic, JavaScript for complex operations
- Consider security implications of JavaScript UDFs

### 3. Stored Procedures
- Always include proper exception handling
- Use transactions for data consistency
- Validate input parameters
- Use dynamic SQL carefully to avoid injection issues

### 4. JSON Processing
- Validate JSON structure before processing
- Use appropriate data types for extracted values
- Consider performance impact of complex JSON operations

### 5. Performance Considerations
- Use appropriate clustering keys for large tables
- Consider materialized views for frequently used aggregations
- Monitor warehouse usage and resize as needed
- Use `LIMIT` clauses during development and testing

---

## Error Handling Reference

### Common SQL Errors and Solutions

1. **Division by Zero**
   ```sql
   CASE WHEN denominator = 0 THEN NULL ELSE numerator/denominator END
   ```

2. **Null Value Handling**
   ```sql
   IFNULL(column_name, default_value)
   COALESCE(value1, value2, default_value)
   ```

3. **Date Format Issues**
   ```sql
   TRY_TO_DATE(date_string, 'YYYY-MM-DD')
   ```

4. **Type Conversion Errors**
   ```sql
   TRY_TO_NUMBER(string_value)
   ```

### JavaScript UDF Error Handling
```javascript
try {
    // Your logic here
    return result;
} catch (e) {
    return null; // or appropriate error value
}
```

---

## Function Reference Summary

| Category | Function | Purpose |
|----------|----------|---------|
| Window | `ROW_NUMBER()` | Sequential numbering |
| Window | `RANK()` | Ranking with gaps |
| Window | `DENSE_RANK()` | Ranking without gaps |
| Window | `LAG()` / `LEAD()` | Previous/next values |
| Window | `SUM() OVER()` | Running totals |
| Aggregate | `PERCENTILE_CONT()` | Percentile calculation |
| Aggregate | `LISTAGG()` | String concatenation |
| Aggregate | `MODE()` | Most frequent value |
| JSON | `PARSE_JSON()` | Parse JSON string |
| JSON | `FLATTEN()` | Expand JSON arrays |
| JSON | `ARRAY_SIZE()` | Array length |
| Date | `DATEDIFF()` | Date difference |
| Date | `DATEADD()` | Date arithmetic |
| String | `EXTRACT_DOMAIN()` | Custom UDF example |
| Math | `CALCULATE_COMPOUND_INTEREST()` | Custom UDF example |

---

*This syntax reference covers all SQL patterns and functions demonstrated in Lab 04. Use it as a quick reference while working through the exercises or implementing similar functionality in your own projects.*