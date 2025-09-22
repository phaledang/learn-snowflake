# Lab 08: Real-World Data Engineering Project

## 🎯 Objectives
By the end of this lab, you will:
- Build a complete end-to-end data pipeline
- Implement a real-world e-commerce analytics solution
- Integrate multiple data sources
- Create automated monitoring and alerting
- Build executive dashboards and reports
- Apply best practices for production systems

## ⏱️ Estimated Time: 90 minutes

## 📋 Prerequisites
- Completed Labs 01-07
- Understanding of data engineering concepts
- Knowledge of business analytics requirements

## 🏢 Project Overview: E-Commerce Analytics Platform

We'll build a comprehensive analytics platform for an e-commerce company that includes:
- Customer behavior analysis
- Product performance tracking  
- Inventory management
- Financial reporting
- Real-time monitoring

## 🗄️ Step 1: Data Architecture Setup

### 1.1 Create Database Schema

```sql
-- Set up environment
USE WAREHOUSE LEARN_WH;

-- Create dedicated database for the project
CREATE DATABASE IF NOT EXISTS ECOMMERCE_ANALYTICS
COMMENT = 'Real-world e-commerce analytics platform';

USE DATABASE ECOMMERCE_ANALYTICS;

-- Create schemas for different data layers
CREATE SCHEMA IF NOT EXISTS RAW_DATA
COMMENT = 'Raw data from source systems';

CREATE SCHEMA IF NOT EXISTS STAGING
COMMENT = 'Staging area for data transformation';

CREATE SCHEMA IF NOT EXISTS ANALYTICS
COMMENT = 'Analytics and reporting layer';

CREATE SCHEMA IF NOT EXISTS MONITORING
COMMENT = 'Data quality and monitoring';
```

### 1.2 Raw Data Tables

```sql
-- Switch to raw data schema
USE SCHEMA RAW_DATA;

-- Customers table
CREATE OR REPLACE TABLE customers (
    customer_id NUMBER PRIMARY KEY,
    email STRING UNIQUE NOT NULL,
    first_name STRING NOT NULL,
    last_name STRING NOT NULL,
    phone STRING,
    address_line1 STRING,
    address_line2 STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    country STRING DEFAULT 'US',
    registration_date TIMESTAMP,
    customer_segment STRING,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Products table
CREATE OR REPLACE TABLE products (
    product_id NUMBER PRIMARY KEY,
    sku STRING UNIQUE NOT NULL,
    product_name STRING NOT NULL,
    category STRING NOT NULL,
    subcategory STRING,
    brand STRING,
    cost_price NUMBER(10,2),
    selling_price NUMBER(10,2),
    weight_kg NUMBER(8,3),
    dimensions_cm STRING, -- JSON: {"length": 10, "width": 5, "height": 3}
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Orders table
CREATE OR REPLACE TABLE orders (
    order_id NUMBER PRIMARY KEY,
    customer_id NUMBER NOT NULL,
    order_date TIMESTAMP NOT NULL,
    order_status STRING NOT NULL, -- pending, confirmed, shipped, delivered, cancelled
    shipping_address VARIANT, -- JSON object
    billing_address VARIANT, -- JSON object
    payment_method STRING,
    subtotal NUMBER(12,2),
    tax_amount NUMBER(12,2),
    shipping_cost NUMBER(12,2),
    discount_amount NUMBER(12,2),
    total_amount NUMBER(12,2),
    currency STRING DEFAULT 'USD',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Order items table
CREATE OR REPLACE TABLE order_items (
    order_item_id NUMBER PRIMARY KEY,
    order_id NUMBER NOT NULL,
    product_id NUMBER NOT NULL,
    quantity NUMBER NOT NULL,
    unit_price NUMBER(10,2) NOT NULL,
    total_price NUMBER(12,2) GENERATED ALWAYS AS (quantity * unit_price),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Inventory table
CREATE OR REPLACE TABLE inventory (
    inventory_id NUMBER PRIMARY KEY,
    product_id NUMBER NOT NULL,
    warehouse_location STRING NOT NULL,
    quantity_on_hand NUMBER NOT NULL DEFAULT 0,
    quantity_reserved NUMBER NOT NULL DEFAULT 0,
    quantity_available GENERATED ALWAYS AS (quantity_on_hand - quantity_reserved),
    reorder_point NUMBER DEFAULT 10,
    max_stock_level NUMBER DEFAULT 1000,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Website events table (clickstream data)
CREATE OR REPLACE TABLE website_events (
    event_id STRING PRIMARY KEY,
    customer_id NUMBER,
    session_id STRING,
    event_type STRING, -- page_view, product_view, add_to_cart, purchase, etc.
    event_timestamp TIMESTAMP NOT NULL,
    page_url STRING,
    product_id NUMBER,
    event_properties VARIANT, -- JSON for additional event data
    user_agent STRING,
    ip_address STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

## 📊 Step 2: Generate Realistic Sample Data

### 2.1 Data Generation Scripts

```sql
-- Generate sample customers
INSERT INTO customers (
    customer_id, email, first_name, last_name, phone, 
    city, state, zip_code, registration_date, customer_segment
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as customer_id,
    'customer' || ROW_NUMBER() OVER (ORDER BY RANDOM()) || '@email.com' as email,
    CASE (UNIFORM(1, 10, RANDOM()))
        WHEN 1 THEN 'John' WHEN 2 THEN 'Jane' WHEN 3 THEN 'Bob'
        WHEN 4 THEN 'Alice' WHEN 5 THEN 'Charlie' WHEN 6 THEN 'Diana'
        WHEN 7 THEN 'Frank' WHEN 8 THEN 'Grace' WHEN 9 THEN 'Henry'
        ELSE 'Isabel'
    END as first_name,
    CASE (UNIFORM(1, 10, RANDOM()))
        WHEN 1 THEN 'Smith' WHEN 2 THEN 'Johnson' WHEN 3 THEN 'Williams'
        WHEN 4 THEN 'Brown' WHEN 5 THEN 'Jones' WHEN 6 THEN 'Garcia'
        WHEN 7 THEN 'Miller' WHEN 8 THEN 'Davis' WHEN 9 THEN 'Rodriguez'
        ELSE 'Wilson'
    END as last_name,
    '555-' || LPAD(UNIFORM(100, 999, RANDOM()), 3, '0') || '-' || LPAD(UNIFORM(1000, 9999, RANDOM()), 4, '0') as phone,
    CASE (UNIFORM(1, 8, RANDOM()))
        WHEN 1 THEN 'New York' WHEN 2 THEN 'Los Angeles' WHEN 3 THEN 'Chicago'
        WHEN 4 THEN 'Houston' WHEN 5 THEN 'Phoenix' WHEN 6 THEN 'Philadelphia'
        WHEN 7 THEN 'San Antonio' ELSE 'San Diego'
    END as city,
    CASE (UNIFORM(1, 8, RANDOM()))
        WHEN 1 THEN 'NY' WHEN 2 THEN 'CA' WHEN 3 THEN 'IL'
        WHEN 4 THEN 'TX' WHEN 5 THEN 'AZ' WHEN 6 THEN 'PA'
        WHEN 7 THEN 'TX' ELSE 'CA'
    END as state,
    LPAD(UNIFORM(10000, 99999, RANDOM()), 5, '0') as zip_code,
    DATEADD('day', -UNIFORM(1, 730, RANDOM()), CURRENT_TIMESTAMP()) as registration_date,
    CASE (UNIFORM(1, 3, RANDOM()))
        WHEN 1 THEN 'Premium'
        WHEN 2 THEN 'Regular'
        ELSE 'New'
    END as customer_segment
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

-- Generate sample products
INSERT INTO products (
    product_id, sku, product_name, category, subcategory, brand,
    cost_price, selling_price, weight_kg
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as product_id,
    'SKU-' || LPAD(ROW_NUMBER() OVER (ORDER BY RANDOM()), 6, '0') as sku,
    CASE category
        WHEN 'Electronics' THEN 
            CASE (UNIFORM(1, 5, RANDOM()))
                WHEN 1 THEN 'Smartphone Pro' WHEN 2 THEN 'Laptop Ultra'
                WHEN 3 THEN 'Tablet Air' WHEN 4 THEN 'Headphones'
                ELSE 'Smart Watch'
            END
        WHEN 'Clothing' THEN
            CASE (UNIFORM(1, 5, RANDOM()))
                WHEN 1 THEN 'Cotton T-Shirt' WHEN 2 THEN 'Denim Jeans'
                WHEN 3 THEN 'Running Shoes' WHEN 4 THEN 'Winter Jacket'
                ELSE 'Baseball Cap'
            END
        WHEN 'Home' THEN
            CASE (UNIFORM(1, 5, RANDOM()))
                WHEN 1 THEN 'Coffee Maker' WHEN 2 THEN 'Vacuum Cleaner'
                WHEN 3 THEN 'Bed Sheets Set' WHEN 4 THEN 'Kitchen Knife'
                ELSE 'Picture Frame'
            END
        ELSE 'Generic Product'
    END || ' ' || (UNIFORM(1, 100, RANDOM())) as product_name,
    category,
    CASE category
        WHEN 'Electronics' THEN 
            CASE (UNIFORM(1, 3, RANDOM()))
                WHEN 1 THEN 'Mobile' WHEN 2 THEN 'Computers' ELSE 'Accessories'
            END
        WHEN 'Clothing' THEN
            CASE (UNIFORM(1, 3, RANDOM()))
                WHEN 1 THEN 'Shirts' WHEN 2 THEN 'Pants' ELSE 'Shoes'
            END
        ELSE 'General'
    END as subcategory,
    CASE (UNIFORM(1, 8, RANDOM()))
        WHEN 1 THEN 'TechCorp' WHEN 2 THEN 'FashionPlus' WHEN 3 THEN 'HomeStyle'
        WHEN 4 THEN 'ProBrand' WHEN 5 THEN 'EliteGoods' WHEN 6 THEN 'QualityFirst'
        WHEN 7 THEN 'BestChoice' ELSE 'TopTier'
    END as brand,
    ROUND(UNIFORM(10, 500, RANDOM()), 2) as cost_price,
    ROUND(UNIFORM(20, 1000, RANDOM()), 2) as selling_price,
    ROUND(UNIFORM(0.1, 10, RANDOM()), 3) as weight_kg
FROM (
    SELECT 'Electronics' as category FROM TABLE(GENERATOR(ROWCOUNT => 100))
    UNION ALL
    SELECT 'Clothing' as category FROM TABLE(GENERATOR(ROWCOUNT => 100))
    UNION ALL
    SELECT 'Home' as category FROM TABLE(GENERATOR(ROWCOUNT => 100))
    UNION ALL
    SELECT 'Books' as category FROM TABLE(GENERATOR(ROWCOUNT => 50))
    UNION ALL
    SELECT 'Sports' as category FROM TABLE(GENERATOR(ROWCOUNT => 50))
);

-- Generate sample orders
INSERT INTO orders (
    order_id, customer_id, order_date, order_status, payment_method,
    subtotal, tax_amount, shipping_cost, discount_amount, total_amount,
    shipping_address, billing_address
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as order_id,
    UNIFORM(1, 1000, RANDOM()) as customer_id,
    DATEADD('hour', -UNIFORM(1, 8760, RANDOM()), CURRENT_TIMESTAMP()) as order_date,
    CASE (UNIFORM(1, 5, RANDOM()))
        WHEN 1 THEN 'pending' WHEN 2 THEN 'confirmed'
        WHEN 3 THEN 'shipped' WHEN 4 THEN 'delivered'
        ELSE 'cancelled'
    END as order_status,
    CASE (UNIFORM(1, 4, RANDOM()))
        WHEN 1 THEN 'credit_card' WHEN 2 THEN 'debit_card'
        WHEN 3 THEN 'paypal' ELSE 'bank_transfer'
    END as payment_method,
    subtotal,
    ROUND(subtotal * 0.08, 2) as tax_amount,
    CASE WHEN subtotal > 50 THEN 0 ELSE 9.99 END as shipping_cost,
    ROUND(subtotal * UNIFORM(0, 0.2, RANDOM()), 2) as discount_amount,
    ROUND(subtotal + (subtotal * 0.08) + 
          CASE WHEN subtotal > 50 THEN 0 ELSE 9.99 END - 
          (subtotal * UNIFORM(0, 0.2, RANDOM())), 2) as total_amount,
    OBJECT_CONSTRUCT(
        'street', '123 Main St',
        'city', 'New York',
        'state', 'NY',
        'zip', '10001'
    ) as shipping_address,
    OBJECT_CONSTRUCT(
        'street', '123 Main St',
        'city', 'New York', 
        'state', 'NY',
        'zip', '10001'
    ) as billing_address
FROM (
    SELECT ROUND(UNIFORM(25, 2000, RANDOM()), 2) as subtotal
    FROM TABLE(GENERATOR(ROWCOUNT => 5000))
);

-- Generate order items
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price)
SELECT 
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as order_item_id,
    o.order_id,
    UNIFORM(1, 400, RANDOM()) as product_id,
    UNIFORM(1, 5, RANDOM()) as quantity,
    p.selling_price as unit_price
FROM orders o
CROSS JOIN (SELECT ROW_NUMBER() OVER (ORDER BY RANDOM()) as rn FROM TABLE(GENERATOR(ROWCOUNT => 3))) items
JOIN products p ON p.product_id = UNIFORM(1, 400, RANDOM())
WHERE UNIFORM(1, 10, RANDOM()) <= 7; -- 70% chance of multiple items
```

## 🔄 Step 3: Staging and Transformation Layer

### 3.1 Staging Tables and Views

```sql
-- Switch to staging schema
USE SCHEMA STAGING;

-- Customer dimension staging
CREATE OR REPLACE VIEW dim_customers AS
SELECT 
    c.customer_id,
    c.email,
    CONCAT(c.first_name, ' ', c.last_name) as full_name,
    c.first_name,
    c.last_name,
    c.phone,
    c.city,
    c.state,
    c.zip_code,
    c.country,
    c.registration_date,
    c.customer_segment,
    c.is_active,
    DATEDIFF('day', c.registration_date, CURRENT_DATE()) as days_since_registration,
    CASE 
        WHEN DATEDIFF('day', c.registration_date, CURRENT_DATE()) < 30 THEN 'New'
        WHEN DATEDIFF('day', c.registration_date, CURRENT_DATE()) < 365 THEN 'Active'
        ELSE 'Veteran'
    END as customer_tenure,
    c.created_at,
    c.updated_at
FROM RAW_DATA.customers c
WHERE c.is_active = TRUE;

-- Product dimension staging
CREATE OR REPLACE VIEW dim_products AS
SELECT 
    p.product_id,
    p.sku,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.cost_price,
    p.selling_price,
    ROUND((p.selling_price - p.cost_price) / p.selling_price * 100, 2) as margin_percentage,
    p.weight_kg,
    p.dimensions_cm,
    p.description,
    p.is_active,
    p.created_at,
    p.updated_at
FROM RAW_DATA.products p
WHERE p.is_active = TRUE;

-- Date dimension
CREATE OR REPLACE TABLE dim_date AS
SELECT 
    date_value,
    YEAR(date_value) as year,
    QUARTER(date_value) as quarter,
    MONTH(date_value) as month,
    DAY(date_value) as day,
    DAYOFWEEK(date_value) as day_of_week,
    DAYOFYEAR(date_value) as day_of_year,
    WEEKOFYEAR(date_value) as week_of_year,
    MONTHNAME(date_value) as month_name,
    DAYNAME(date_value) as day_name,
    CASE WHEN DAYOFWEEK(date_value) IN (1, 7) THEN TRUE ELSE FALSE END as is_weekend,
    CASE 
        WHEN MONTH(date_value) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(date_value) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(date_value) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END as season
FROM (
    SELECT DATEADD('day', ROW_NUMBER() OVER (ORDER BY NULL) - 1, '2020-01-01') as date_value
    FROM TABLE(GENERATOR(ROWCOUNT => 2000))
);

-- Sales fact staging
CREATE OR REPLACE VIEW fact_sales AS
SELECT 
    oi.order_item_id,
    o.order_id,
    o.customer_id,
    oi.product_id,
    DATE(o.order_date) as order_date,
    o.order_status,
    oi.quantity,
    oi.unit_price,
    oi.total_price,
    o.subtotal as order_subtotal,
    o.tax_amount,
    o.shipping_cost,
    o.discount_amount,
    o.total_amount as order_total,
    p.cost_price,
    (oi.unit_price - p.cost_price) * oi.quantity as gross_profit,
    o.payment_method,
    o.created_at as order_created_at
FROM RAW_DATA.orders o
JOIN RAW_DATA.order_items oi ON o.order_id = oi.order_id
JOIN RAW_DATA.products p ON oi.product_id = p.product_id
WHERE o.order_status NOT IN ('cancelled');
```

## 📈 Step 4: Analytics Layer

### 4.1 Business Metrics and KPIs

```sql
-- Switch to analytics schema
USE SCHEMA ANALYTICS;

-- Daily sales summary
CREATE OR REPLACE TABLE daily_sales_summary AS
SELECT 
    order_date,
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(total_price) as total_revenue,
    SUM(gross_profit) as total_profit,
    AVG(total_price) as avg_order_value,
    SUM(quantity) as total_units_sold
FROM STAGING.fact_sales
GROUP BY order_date;

-- Customer lifetime value analysis
CREATE OR REPLACE VIEW customer_ltv_analysis AS
WITH customer_metrics AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT order_id) as total_orders,
        SUM(total_price) as total_spent,
        AVG(total_price) as avg_order_value,
        MIN(order_date) as first_order_date,
        MAX(order_date) as last_order_date,
        DATEDIFF('day', MIN(order_date), MAX(order_date)) as customer_lifespan_days
    FROM STAGING.fact_sales
    GROUP BY customer_id
)
SELECT 
    cm.*,
    dc.customer_segment,
    dc.customer_tenure,
    CASE 
        WHEN total_orders = 1 THEN 'One-time'
        WHEN total_orders <= 3 THEN 'Occasional'
        WHEN total_orders <= 10 THEN 'Regular'
        ELSE 'Loyal'
    END as customer_loyalty_tier,
    CASE 
        WHEN customer_lifespan_days > 0 
        THEN total_spent / (customer_lifespan_days / 30.0)
        ELSE total_spent
    END as monthly_value
FROM customer_metrics cm
JOIN STAGING.dim_customers dc ON cm.customer_id = dc.customer_id;

-- Product performance analysis
CREATE OR REPLACE VIEW product_performance_analysis AS
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    COUNT(fs.order_item_id) as total_orders,
    SUM(fs.quantity) as total_units_sold,
    SUM(fs.total_price) as total_revenue,
    SUM(fs.gross_profit) as total_profit,
    AVG(fs.unit_price) as avg_selling_price,
    p.cost_price,
    p.margin_percentage,
    RANK() OVER (PARTITION BY p.category ORDER BY SUM(fs.total_price) DESC) as revenue_rank_in_category,
    RANK() OVER (ORDER BY SUM(fs.total_price) DESC) as overall_revenue_rank
FROM STAGING.dim_products p
LEFT JOIN STAGING.fact_sales fs ON p.product_id = fs.product_id
GROUP BY p.product_id, p.product_name, p.category, p.subcategory, 
         p.brand, p.cost_price, p.margin_percentage;

-- Cohort analysis
CREATE OR REPLACE VIEW cohort_analysis AS
WITH customer_cohorts AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', MIN(order_date)) as cohort_month,
        MIN(order_date) as first_order_date
    FROM STAGING.fact_sales
    GROUP BY customer_id
),
cohort_data AS (
    SELECT 
        cc.cohort_month,
        DATE_TRUNC('month', fs.order_date) as period_month,
        DATEDIFF('month', cc.cohort_month, DATE_TRUNC('month', fs.order_date)) as period_number,
        COUNT(DISTINCT fs.customer_id) as customers
    FROM customer_cohorts cc
    JOIN STAGING.fact_sales fs ON cc.customer_id = fs.customer_id
    GROUP BY cc.cohort_month, DATE_TRUNC('month', fs.order_date)
),
cohort_sizes AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_id) as cohort_size
    FROM customer_cohorts
    GROUP BY cohort_month
)
SELECT 
    cd.cohort_month,
    cd.period_number,
    cd.customers,
    cs.cohort_size,
    ROUND(cd.customers::FLOAT / cs.cohort_size * 100, 2) as retention_rate
FROM cohort_data cd
JOIN cohort_sizes cs ON cd.cohort_month = cs.cohort_month
ORDER BY cd.cohort_month, cd.period_number;
```

### 4.2 Advanced Analytics Functions

```sql
-- Sales forecasting preparation
CREATE OR REPLACE VIEW sales_forecasting_data AS
SELECT 
    order_date,
    SUM(total_price) as daily_revenue,
    AVG(SUM(total_price)) OVER (
        ORDER BY order_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as revenue_7day_ma,
    AVG(SUM(total_price)) OVER (
        ORDER BY order_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as revenue_30day_ma,
    LAG(SUM(total_price), 7) OVER (ORDER BY order_date) as revenue_7days_ago,
    LAG(SUM(total_price), 30) OVER (ORDER BY order_date) as revenue_30days_ago
FROM STAGING.fact_sales
GROUP BY order_date
ORDER BY order_date;

-- Market basket analysis
CREATE OR REPLACE VIEW market_basket_analysis AS
WITH order_products AS (
    SELECT 
        order_id,
        LISTAGG(product_id, ',') WITHIN GROUP (ORDER BY product_id) as product_combination,
        COUNT(DISTINCT product_id) as product_count
    FROM STAGING.fact_sales
    GROUP BY order_id
    HAVING COUNT(DISTINCT product_id) > 1
),
product_pairs AS (
    SELECT 
        fs1.product_id as product_a,
        fs2.product_id as product_b,
        COUNT(*) as co_occurrence_count
    FROM STAGING.fact_sales fs1
    JOIN STAGING.fact_sales fs2 ON fs1.order_id = fs2.order_id
    WHERE fs1.product_id < fs2.product_id
    GROUP BY fs1.product_id, fs2.product_id
    HAVING COUNT(*) >= 5
)
SELECT 
    pp.product_a,
    pa.product_name as product_a_name,
    pp.product_b,
    pb.product_name as product_b_name,
    pp.co_occurrence_count,
    ROUND(pp.co_occurrence_count::FLOAT / 
          (SELECT COUNT(DISTINCT order_id) FROM STAGING.fact_sales) * 100, 4) as support_percentage
FROM product_pairs pp
JOIN STAGING.dim_products pa ON pp.product_a = pa.product_id
JOIN STAGING.dim_products pb ON pp.product_b = pb.product_id
ORDER BY pp.co_occurrence_count DESC;
```

## 🚨 Step 5: Monitoring and Data Quality

### 5.1 Data Quality Framework

```sql
-- Switch to monitoring schema
USE SCHEMA MONITORING;

-- Data quality rules table
CREATE OR REPLACE TABLE data_quality_rules (
    rule_id NUMBER AUTOINCREMENT PRIMARY KEY,
    rule_name STRING,
    table_name STRING,
    rule_sql STRING,
    expected_result STRING,
    severity STRING, -- 'critical', 'warning', 'info'
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Insert data quality rules
INSERT INTO data_quality_rules (rule_name, table_name, rule_sql, expected_result, severity) VALUES
('No null customer IDs', 'RAW_DATA.ORDERS', 'SELECT COUNT(*) FROM RAW_DATA.ORDERS WHERE customer_id IS NULL', '0', 'critical'),
('No negative order amounts', 'RAW_DATA.ORDERS', 'SELECT COUNT(*) FROM RAW_DATA.ORDERS WHERE total_amount < 0', '0', 'critical'),
('No future order dates', 'RAW_DATA.ORDERS', 'SELECT COUNT(*) FROM RAW_DATA.ORDERS WHERE order_date > CURRENT_TIMESTAMP()', '0', 'warning'),
('Product prices are positive', 'RAW_DATA.PRODUCTS', 'SELECT COUNT(*) FROM RAW_DATA.PRODUCTS WHERE selling_price <= 0', '0', 'critical'),
('Customer emails are unique', 'RAW_DATA.CUSTOMERS', 'SELECT COUNT(*) - COUNT(DISTINCT email) FROM RAW_DATA.CUSTOMERS', '0', 'critical');

-- Data quality results table
CREATE OR REPLACE TABLE data_quality_results (
    result_id NUMBER AUTOINCREMENT PRIMARY KEY,
    rule_id NUMBER,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    actual_result STRING,
    expected_result STRING,
    status STRING, -- 'pass', 'fail'
    severity STRING,
    details STRING,
    FOREIGN KEY (rule_id) REFERENCES data_quality_rules(rule_id)
);

-- Data quality check procedure
CREATE OR REPLACE PROCEDURE run_data_quality_checks()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rule_cursor CURSOR FOR SELECT rule_id, rule_name, rule_sql, expected_result, severity FROM data_quality_rules WHERE is_active = TRUE;
    current_rule_id NUMBER;
    current_rule_name STRING;
    current_rule_sql STRING;
    current_expected STRING;
    current_severity STRING;
    actual_result STRING;
    check_status STRING;
    total_checks NUMBER DEFAULT 0;
    failed_checks NUMBER DEFAULT 0;
BEGIN
    FOR rule_record IN rule_cursor DO
        current_rule_id := rule_record.rule_id;
        current_rule_name := rule_record.rule_name;
        current_rule_sql := rule_record.rule_sql;
        current_expected := rule_record.expected_result;
        current_severity := rule_record.severity;
        
        -- Execute the rule
        EXECUTE IMMEDIATE current_rule_sql INTO actual_result;
        
        -- Determine status
        IF (actual_result = current_expected) THEN
            check_status := 'pass';
        ELSE
            check_status := 'fail';
            failed_checks := failed_checks + 1;
        END IF;
        
        -- Insert result
        INSERT INTO data_quality_results (rule_id, actual_result, expected_result, status, severity, details)
        VALUES (current_rule_id, actual_result, current_expected, check_status, current_severity, current_rule_name);
        
        total_checks := total_checks + 1;
    END FOR;
    
    RETURN 'Data quality check completed. Total checks: ' || total_checks || ', Failed: ' || failed_checks;
END;
$$;

-- Run data quality checks
CALL run_data_quality_checks();

-- View recent data quality results
SELECT 
    dqr.check_timestamp,
    dqr.rule_id,
    dql.rule_name,
    dql.table_name,
    dqr.status,
    dqr.severity,
    dqr.actual_result,
    dqr.expected_result
FROM data_quality_results dqr
JOIN data_quality_rules dql ON dqr.rule_id = dql.rule_id
WHERE dqr.check_timestamp >= DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY dqr.check_timestamp DESC;
```

### 5.2 Performance Monitoring

```sql
-- Query performance monitoring
CREATE OR REPLACE VIEW query_performance_monitor AS
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
    start_time,
    end_time,
    CASE 
        WHEN total_elapsed_time > 300000 THEN 'Slow'
        WHEN total_elapsed_time > 60000 THEN 'Medium'
        ELSE 'Fast'
    END as performance_category
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
  AND warehouse_name IS NOT NULL
ORDER BY total_elapsed_time DESC;

-- Table growth monitoring
CREATE OR REPLACE TABLE table_size_history (
    check_date DATE,
    table_catalog STRING,
    table_schema STRING,
    table_name STRING,
    row_count NUMBER,
    bytes NUMBER,
    size_gb NUMBER
);

-- Procedure to capture table sizes
CREATE OR REPLACE PROCEDURE capture_table_sizes()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO table_size_history
    SELECT 
        CURRENT_DATE() as check_date,
        table_catalog,
        table_schema,
        table_name,
        row_count,
        bytes,
        ROUND(bytes / (1024 * 1024 * 1024), 2) as size_gb
    FROM INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
      AND table_schema IN ('RAW_DATA', 'STAGING', 'ANALYTICS');
    
    RETURN 'Table sizes captured for ' || CURRENT_DATE();
END;
$$;

-- Run table size capture
CALL capture_table_sizes();
```

## 📊 Step 6: Executive Dashboard Queries

### 6.1 Key Business Metrics

```sql
-- Executive summary view
CREATE OR REPLACE VIEW executive_summary AS
WITH current_period AS (
    SELECT 
        COUNT(DISTINCT order_id) as total_orders,
        COUNT(DISTINCT customer_id) as active_customers,
        SUM(total_price) as total_revenue,
        SUM(gross_profit) as total_profit,
        AVG(total_price) as avg_order_value
    FROM STAGING.fact_sales
    WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE())
),
previous_period AS (
    SELECT 
        COUNT(DISTINCT order_id) as total_orders,
        COUNT(DISTINCT customer_id) as active_customers,
        SUM(total_price) as total_revenue,
        SUM(gross_profit) as total_profit,
        AVG(total_price) as avg_order_value
    FROM STAGING.fact_sales
    WHERE order_date >= DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()))
      AND order_date < DATE_TRUNC('month', CURRENT_DATE())
)
SELECT 
    'Current Month' as period,
    cp.total_orders,
    cp.active_customers,
    ROUND(cp.total_revenue, 2) as total_revenue,
    ROUND(cp.total_profit, 2) as total_profit,
    ROUND(cp.avg_order_value, 2) as avg_order_value,
    ROUND((cp.total_orders - pp.total_orders)::FLOAT / pp.total_orders * 100, 2) as orders_growth_pct,
    ROUND((cp.total_revenue - pp.total_revenue)::FLOAT / pp.total_revenue * 100, 2) as revenue_growth_pct
FROM current_period cp
CROSS JOIN previous_period pp;

-- Top performing products
CREATE OR REPLACE VIEW top_products_dashboard AS
SELECT 
    product_name,
    category,
    brand,
    total_revenue,
    total_units_sold,
    total_profit,
    revenue_rank_in_category,
    overall_revenue_rank
FROM ANALYTICS.product_performance_analysis
WHERE overall_revenue_rank <= 20
ORDER BY overall_revenue_rank;

-- Customer segment analysis
CREATE OR REPLACE VIEW customer_segment_dashboard AS
SELECT 
    dc.customer_segment,
    COUNT(DISTINCT cla.customer_id) as customer_count,
    AVG(cla.total_spent) as avg_customer_value,
    SUM(cla.total_spent) as segment_revenue,
    AVG(cla.total_orders) as avg_orders_per_customer,
    ROUND(AVG(cla.monthly_value), 2) as avg_monthly_value
FROM ANALYTICS.customer_ltv_analysis cla
JOIN STAGING.dim_customers dc ON cla.customer_id = dc.customer_id
GROUP BY dc.customer_segment
ORDER BY segment_revenue DESC;
```

## ✅ Lab Completion Checklist

- [ ] Built complete e-commerce data architecture
- [ ] Generated realistic sample data across all tables
- [ ] Created staging layer with dimensional modeling
- [ ] Implemented comprehensive analytics layer
- [ ] Built advanced analytics including cohort and market basket analysis
- [ ] Created data quality monitoring framework
- [ ] Implemented performance monitoring
- [ ] Developed executive dashboard queries
- [ ] Applied production-ready best practices

## 🎉 Congratulations!

You've completed a real-world data engineering project! You now have:
- A complete end-to-end analytics platform
- Production-ready data quality monitoring
- Advanced analytics capabilities
- Executive reporting dashboards
- Best practices for scalable data architecture

## 🚀 Next Steps and Enhancements

Consider these enhancements for further learning:
1. **Real-time streaming**: Implement Snowpipe for real-time data ingestion
2. **Machine learning**: Add ML models for customer segmentation and demand forecasting
3. **Data sharing**: Set up secure data sharing with external partners
4. **Cost optimization**: Implement automated warehouse scaling
5. **Advanced security**: Add row-level security and data masking

## 📚 Additional Resources

- [Snowflake Data Engineering Guide](https://docs.snowflake.com/en/user-guide-data-engineering.html)
- [Analytics Best Practices](https://docs.snowflake.com/en/user-guide/analytics.html)
- [Data Governance](https://docs.snowflake.com/en/user-guide/data-governance.html)