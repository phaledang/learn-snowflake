# Lab 02: Snowflake Architecture & Advanced SQL

## 🎯 Objectives
By the end of this lab, you will:
- Understand Snowflake's unique architecture
- Master advanced SQL functions and data types
- Learn about data clustering and optimization
- Practice with complex queries and analytics

## ⏱️ Estimated Time: 45 minutes

## 📋 Prerequisites
- Completed Lab 01
- Active Snowflake trial account
- Understanding of basic SQL

## 🏗️ Step 1: Understanding Snowflake Architecture

### 1.1 The Three-Layer Architecture

Snowflake's architecture consists of three distinct layers:

1. **Database Storage Layer**: Stores data in a compressed, columnar format
2. **Query Processing Layer**: Virtual warehouses that provide compute power
3. **Cloud Services Layer**: Manages metadata, security, and optimization

### 1.2 Key Architectural Benefits

```sql
-- Let's explore the architecture through queries
USE WAREHOUSE LEARN_WH;
USE DATABASE LEARN_SNOWFLAKE;
USE SCHEMA SANDBOX;

-- View account information
SELECT CURRENT_ACCOUNT(), CURRENT_REGION();

-- Check available databases
SHOW DATABASES;

-- Explore system functions
SELECT 
    CURRENT_DATABASE() as current_db,
    CURRENT_SCHEMA() as current_schema,
    CURRENT_WAREHOUSE() as current_warehouse,
    CURRENT_USER() as current_user,
    CURRENT_ROLE() as current_role;
```

### 1.3 Virtual Warehouses Deep Dive

```sql
-- Create warehouses of different sizes
CREATE WAREHOUSE IF NOT EXISTS SMALL_WH 
WITH WAREHOUSE_SIZE = 'SMALL' AUTO_SUSPEND = 300;

CREATE WAREHOUSE IF NOT EXISTS MEDIUM_WH 
WITH WAREHOUSE_SIZE = 'MEDIUM' AUTO_SUSPEND = 300;

-- Show all warehouses
SHOW WAREHOUSES;

-- Compare warehouse properties
SELECT "name", "state", "size", "min_cluster_count", "max_cluster_count"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
```

## 📊 Step 2: Advanced Data Types

### 2.1 Working with Different Data Types

```sql
-- Create a comprehensive table with various data types
CREATE OR REPLACE TABLE data_types_demo (
    id NUMBER,
    name STRING,
    age NUMBER(3),
    salary NUMBER(10,2),
    is_active BOOLEAN,
    hire_date DATE,
    last_login TIMESTAMP,
    metadata VARIANT,
    skills ARRAY,
    address OBJECT
);

-- Insert data with various types
INSERT INTO data_types_demo VALUES 
(1, 'Alice Johnson', 28, 75000.50, TRUE, '2023-01-15', 
 '2024-01-15 08:30:00', 
 '{"department": "Engineering", "level": "Senior"}',
 ['Python', 'SQL', 'Snowflake'],
 {'street': '123 Main St', 'city': 'Seattle', 'state': 'WA'}),
(2, 'Bob Smith', 35, 82000.00, FALSE, '2022-06-20',
 '2024-01-10 14:22:15',
 '{"department": "Marketing", "level": "Manager"}',
 ['JavaScript', 'HTML', 'CSS', 'React'],
 {'street': '456 Oak Ave', 'city': 'Portland', 'state': 'OR'});

-- Query the data
SELECT * FROM data_types_demo;
```

### 2.2 Working with Semi-Structured Data

```sql
-- Extract data from VARIANT column
SELECT 
    name,
    metadata:department::STRING as department,
    metadata:level::STRING as level
FROM data_types_demo;

-- Work with ARRAY data
SELECT 
    name,
    skills,
    ARRAY_SIZE(skills) as skill_count,
    skills[0]::STRING as first_skill
FROM data_types_demo;

-- Extract from OBJECT data
SELECT 
    name,
    address:city::STRING as city,
    address:state::STRING as state
FROM data_types_demo;
```

## 🔧 Step 3: Advanced SQL Functions

### 3.1 Window Functions

```sql
-- Create sales data for window function examples
CREATE OR REPLACE TABLE sales_data (
    salesperson STRING,
    region STRING,
    quarter STRING,
    sales_amount NUMBER
);

INSERT INTO sales_data VALUES
('John', 'North', 'Q1', 150000),
('John', 'North', 'Q2', 180000),
('John', 'North', 'Q3', 175000),
('Jane', 'South', 'Q1', 200000),
('Jane', 'South', 'Q2', 210000),
('Jane', 'South', 'Q3', 195000),
('Bob', 'East', 'Q1', 165000),
('Bob', 'East', 'Q2', 185000),
('Bob', 'East', 'Q3', 190000);

-- Window functions for analytics
SELECT 
    salesperson,
    region,
    quarter,
    sales_amount,
    -- Running total
    SUM(sales_amount) OVER (PARTITION BY salesperson ORDER BY quarter) as running_total,
    -- Rank within region
    RANK() OVER (PARTITION BY region ORDER BY sales_amount DESC) as region_rank,
    -- Moving average
    AVG(sales_amount) OVER (PARTITION BY salesperson ORDER BY quarter 
                           ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as moving_avg,
    -- Percentage of total
    ROUND(sales_amount / SUM(sales_amount) OVER () * 100, 2) as pct_of_total
FROM sales_data
ORDER BY salesperson, quarter;
```

### 3.2 Date and Time Functions

```sql
-- Create time-based data
CREATE OR REPLACE TABLE events (
    event_id NUMBER,
    event_name STRING,
    event_timestamp TIMESTAMP,
    event_date DATE
);

INSERT INTO events VALUES
(1, 'User Login', '2024-01-15 08:30:00', '2024-01-15'),
(2, 'Purchase', '2024-01-15 10:45:00', '2024-01-15'),
(3, 'User Logout', '2024-01-15 17:20:00', '2024-01-15'),
(4, 'User Login', '2024-01-16 09:15:00', '2024-01-16'),
(5, 'Support Ticket', '2024-01-16 14:30:00', '2024-01-16');

-- Advanced date/time operations
SELECT 
    event_name,
    event_timestamp,
    -- Extract components
    YEAR(event_timestamp) as year,
    MONTH(event_timestamp) as month,
    DAY(event_timestamp) as day,
    HOUR(event_timestamp) as hour,
    -- Format dates
    TO_CHAR(event_timestamp, 'YYYY-MM-DD HH24:MI:SS') as formatted_timestamp,
    -- Date arithmetic
    DATEADD('day', 7, event_date) as one_week_later,
    DATEDIFF('hour', LAG(event_timestamp) OVER (ORDER BY event_timestamp), event_timestamp) as hours_since_last
FROM events
ORDER BY event_timestamp;
```

### 3.3 String Functions

```sql
-- Create customer data for string operations
CREATE OR REPLACE TABLE customers (
    customer_id NUMBER,
    full_name STRING,
    email STRING,
    phone STRING
);

INSERT INTO customers VALUES
(1, 'John Michael Smith', 'john.smith@email.com', '(555) 123-4567'),
(2, 'Jane Marie Doe', 'jane.doe@company.org', '555.987.6543'),
(3, 'Robert James Wilson', 'bob.wilson@domain.net', '555-456-7890');

-- String manipulation
SELECT 
    customer_id,
    full_name,
    -- Split names
    SPLIT_PART(full_name, ' ', 1) as first_name,
    SPLIT_PART(full_name, ' ', -1) as last_name,
    -- Email domain
    SPLIT_PART(email, '@', 2) as email_domain,
    -- Clean phone numbers
    REGEXP_REPLACE(phone, '[^0-9]', '') as clean_phone,
    -- String functions
    UPPER(full_name) as name_upper,
    LENGTH(full_name) as name_length,
    LEFT(email, POSITION('@', email) - 1) as email_username,
    -- Pattern matching
    CASE 
        WHEN email LIKE '%.com' THEN 'Commercial'
        WHEN email LIKE '%.org' THEN 'Organization'
        WHEN email LIKE '%.net' THEN 'Network'
        ELSE 'Other'
    END as email_type
FROM customers;
```

## 📈 Step 4: Performance and Optimization

### 4.1 Understanding Query Performance

```sql
-- Create a larger dataset for performance testing
CREATE OR REPLACE TABLE large_sales (
    id NUMBER,
    product_category STRING,
    sales_date DATE,
    amount NUMBER,
    customer_region STRING
);

-- Insert sample data (this creates a moderate-sized dataset)
INSERT INTO large_sales
SELECT 
    SEQ4() as id,
    CASE (UNIFORM(1, 5, RANDOM()))
        WHEN 1 THEN 'Electronics'
        WHEN 2 THEN 'Clothing'
        WHEN 3 THEN 'Books'
        WHEN 4 THEN 'Home'
        ELSE 'Sports'
    END as product_category,
    DATEADD('day', -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) as sales_date,
    UNIFORM(10, 1000, RANDOM()) as amount,
    CASE (UNIFORM(1, 4, RANDOM()))
        WHEN 1 THEN 'North'
        WHEN 2 THEN 'South'
        WHEN 3 THEN 'East'
        ELSE 'West'
    END as customer_region
FROM TABLE(GENERATOR(ROWCOUNT => 10000));

-- Analyze the data
SELECT COUNT(*) as total_records FROM large_sales;

-- Performance comparison queries
-- Query 1: Simple aggregation
SELECT 
    product_category,
    COUNT(*) as sales_count,
    SUM(amount) as total_sales,
    AVG(amount) as avg_sales
FROM large_sales
GROUP BY product_category
ORDER BY total_sales DESC;

-- Query 2: Complex analytics
SELECT 
    customer_region,
    product_category,
    DATE_TRUNC('month', sales_date) as sales_month,
    COUNT(*) as transaction_count,
    SUM(amount) as monthly_sales,
    AVG(amount) as avg_transaction,
    MIN(amount) as min_sale,
    MAX(amount) as max_sale
FROM large_sales
WHERE sales_date >= '2023-01-01'
GROUP BY customer_region, product_category, DATE_TRUNC('month', sales_date)
ORDER BY sales_month, customer_region, product_category;
```

### 4.2 Query Optimization Techniques

```sql
-- Check query history and performance
SELECT 
    query_id,
    query_text,
    execution_status,
    total_elapsed_time,
    warehouse_name,
    rows_produced
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE start_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
ORDER BY start_time DESC
LIMIT 10;

-- Use EXPLAIN to understand query plans
-- EXPLAIN SELECT * FROM large_sales WHERE product_category = 'Electronics';
```

## 🏷️ Step 5: Working with Constraints and Keys

```sql
-- Create a properly structured table with constraints
CREATE OR REPLACE TABLE products (
    product_id NUMBER NOT NULL,
    product_name STRING NOT NULL,
    category STRING NOT NULL,
    price NUMBER(10,2) CHECK (price > 0),
    created_date DATE DEFAULT CURRENT_DATE(),
    is_active BOOLEAN DEFAULT TRUE,
    CONSTRAINT pk_products PRIMARY KEY (product_id)
);

-- Create orders table with foreign key relationship
CREATE OR REPLACE TABLE orders (
    order_id NUMBER NOT NULL,
    product_id NUMBER NOT NULL,
    customer_name STRING NOT NULL,
    order_date DATE DEFAULT CURRENT_DATE(),
    quantity NUMBER CHECK (quantity > 0),
    CONSTRAINT pk_orders PRIMARY KEY (order_id),
    CONSTRAINT fk_orders_products FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Insert sample products
INSERT INTO products (product_id, product_name, category, price) VALUES
(1, 'Laptop Pro', 'Electronics', 1299.99),
(2, 'Wireless Mouse', 'Electronics', 29.99),
(3, 'Office Chair', 'Furniture', 249.50),
(4, 'Desk Lamp', 'Furniture', 79.99);

-- Insert sample orders
INSERT INTO orders (order_id, product_id, customer_name, quantity) VALUES
(101, 1, 'Alice Johnson', 1),
(102, 2, 'Bob Smith', 2),
(103, 3, 'Carol Davis', 1),
(104, 1, 'David Wilson', 1);

-- Query with joins
SELECT 
    o.order_id,
    o.customer_name,
    p.product_name,
    p.category,
    o.quantity,
    p.price,
    (o.quantity * p.price) as total_amount
FROM orders o
JOIN products p ON o.product_id = p.product_id
ORDER BY o.order_date DESC;
```

## 🔍 Step 6: Information Schema and Metadata

```sql
-- Explore database metadata
SELECT 
    table_catalog,
    table_schema,
    table_name,
    table_type,
    row_count,
    bytes,
    created
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'SANDBOX'
ORDER BY created DESC;

-- Column information
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'SANDBOX'
  AND table_name = 'PRODUCTS'
ORDER BY ordinal_position;

-- View constraints
SELECT 
    constraint_name,
    table_name,
    constraint_type,
    column_name
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
  ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_schema = 'SANDBOX'
ORDER BY table_name, constraint_name;
```

## ✅ Lab Completion Checklist

- [ ] Understood Snowflake's three-layer architecture
- [ ] Created and worked with different data types
- [ ] Practiced semi-structured data queries
- [ ] Implemented window functions for analytics
- [ ] Used advanced date/time and string functions
- [ ] Created performance test queries
- [ ] Implemented table constraints and relationships
- [ ] Explored database metadata

## 🎉 Congratulations!

You've mastered Snowflake architecture and advanced SQL! You now understand:
- How Snowflake's unique architecture benefits performance
- Advanced data types including semi-structured data
- Complex analytical queries using window functions
- Performance optimization techniques
- Database design best practices

## 🔜 Next Steps

Continue with [Lab 03: Data Loading & Warehouses](../lab03/) to learn about:
- Different data loading methods
- Working with file formats
- Staging and transformation
- Warehouse sizing and management

## 🆘 Troubleshooting

### Common Issues:

**Issue**: Semi-structured data queries not working
**Solution**: Ensure proper casting with `::STRING`, `::NUMBER`, etc.

**Issue**: Window function errors
**Solution**: Check PARTITION BY and ORDER BY clauses syntax

**Issue**: Constraint violations
**Solution**: Verify data meets constraint requirements before insertion

## 📚 Additional Resources

- [Snowflake Architecture Guide](https://docs.snowflake.com/en/user-guide/intro-key-concepts.html)
- [SQL Reference](https://docs.snowflake.com/en/sql-reference.html)
- [Semi-structured Data](https://docs.snowflake.com/en/user-guide/semistructured-concepts.html)