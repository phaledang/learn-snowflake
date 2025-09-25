-- Lab 02: Snowflake Architecture & Advanced SQL
-- SQL Scripts for hands-on practice

-- ============================================================================
-- SECTION 1: ARCHITECTURE EXPLORATION
-- ============================================================================

-- Set context
USE WAREHOUSE LEARN_WH;
USE DATABASE LEARN_SNOWFLAKE;
USE SCHEMA SANDBOX;

-- Explore account and environment
SELECT 
    CURRENT_ACCOUNT() as account,
    CURRENT_REGION() as region,
    CURRENT_DATABASE() as database,
    CURRENT_SCHEMA() as schema,
    CURRENT_WAREHOUSE() as warehouse,
    CURRENT_USER() as user,
    CURRENT_ROLE() as role;

-- Create warehouses of different sizes
CREATE WAREHOUSE IF NOT EXISTS SMALL_WH 
WITH WAREHOUSE_SIZE = 'SMALL' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE;

CREATE WAREHOUSE IF NOT EXISTS MEDIUM_WH 
WITH WAREHOUSE_SIZE = 'MEDIUM' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE;

-- Show warehouse information
SHOW WAREHOUSES;

-- ============================================================================
-- SECTION 2: ADVANCED DATA TYPES
-- ============================================================================

-- Optional: drop the table if you really want to reset it
-- DROP TABLE IF EXISTS data_types_demo;

CREATE TABLE IF NOT EXISTS data_types_demo (
    id         NUMBER,
    name       STRING,
    age        NUMBER(3),
    salary     NUMBER(10,2),
    is_active  BOOLEAN,
    hire_date  DATE,
    last_login TIMESTAMP_NTZ,
    metadata   VARIANT,
    skills     ARRAY,
    address    OBJECT
);

INSERT INTO data_types_demo
    (id, name, age, salary, is_active, hire_date, last_login, metadata, skills, address)
SELECT
    3, 'Charlie', 29, 70000, TRUE, TO_DATE('2023-05-01'),
    TO_TIMESTAMP_NTZ('2024-02-01 09:00:00'),
    PARSE_JSON('{"department":"Finance","level":"Senior"}'),
    PARSE_JSON('["Excel","SQL"]'),
    PARSE_JSON('{"street":"789 Pine Rd","city":"Austin","state":"TX"}');


-- Query semi-structured data
SELECT 
    name,
    metadata:department::STRING as department,
    metadata:level::STRING as level,
    metadata:team_size::NUMBER as team_size,
    metadata:projects as projects_array,
    skills,
    ARRAY_SIZE(skills) as skill_count,
    skills[0]::STRING as primary_skill,
    address:city::STRING as city,
    address:state::STRING as state
FROM data_types_demo;

-- ============================================================================
-- SECTION 3: WINDOW FUNCTIONS AND ANALYTICS
-- ============================================================================

-- Create sales data for window function examples
CREATE OR REPLACE TABLE sales_data (
    salesperson STRING,
    region STRING,
    quarter STRING,
    year NUMBER,
    sales_amount NUMBER
);

INSERT INTO sales_data VALUES
('John', 'North', 'Q1', 2023, 150000),
('John', 'North', 'Q2', 2023, 180000),
('John', 'North', 'Q3', 2023, 175000),
('John', 'North', 'Q4', 2023, 195000),
('Jane', 'South', 'Q1', 2023, 200000),
('Jane', 'South', 'Q2', 2023, 210000),
('Jane', 'South', 'Q3', 2023, 195000),
('Jane', 'South', 'Q4', 2023, 220000),
('Bob', 'East', 'Q1', 2023, 165000),
('Bob', 'East', 'Q2', 2023, 185000),
('Bob', 'East', 'Q3', 2023, 190000),
('Bob', 'East', 'Q4', 2023, 205000),
('Sarah', 'West', 'Q1', 2023, 175000),
('Sarah', 'West', 'Q2', 2023, 185000),
('Sarah', 'West', 'Q3', 2023, 200000),
('Sarah', 'West', 'Q4', 2023, 190000);

-- Comprehensive window function examples
SELECT 
    salesperson,
    region,
    quarter,
    sales_amount,
    -- Running totals
    SUM(sales_amount) OVER (PARTITION BY salesperson ORDER BY quarter) as running_total,
    SUM(sales_amount) OVER (PARTITION BY region ORDER BY quarter) as region_running_total,
    -- Rankings
    RANK() OVER (ORDER BY sales_amount DESC) as overall_rank,
    RANK() OVER (PARTITION BY region ORDER BY sales_amount DESC) as region_rank,
    DENSE_RANK() OVER (ORDER BY sales_amount DESC) as dense_rank,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales_amount DESC) as row_num,
    -- Percentiles
    PERCENT_RANK() OVER (ORDER BY sales_amount) as percent_rank,
    NTILE(4) OVER (ORDER BY sales_amount) as quartile,
    -- Moving averages
    AVG(sales_amount) OVER (PARTITION BY salesperson ORDER BY quarter 
                           ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as moving_avg_2q,
    AVG(sales_amount) OVER (PARTITION BY salesperson ORDER BY quarter 
                           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg_3q,
    -- Lead and Lag
    LAG(sales_amount, 1) OVER (PARTITION BY salesperson ORDER BY quarter) as prev_quarter,
    LEAD(sales_amount, 1) OVER (PARTITION BY salesperson ORDER BY quarter) as next_quarter,
    -- Percentage calculations
    ROUND(sales_amount / SUM(sales_amount) OVER (PARTITION BY salesperson) * 100, 2) as pct_of_person_total,
    ROUND(sales_amount / SUM(sales_amount) OVER () * 100, 2) as pct_of_grand_total
FROM sales_data
ORDER BY salesperson, quarter;

-- ============================================================================
-- SECTION 4: DATE AND TIME FUNCTIONS
-- ============================================================================

-- Create events table
CREATE OR REPLACE TABLE events (
    event_id NUMBER,
    event_name STRING,
    event_timestamp TIMESTAMP,
    event_date DATE,
    timezone STRING
);

INSERT INTO events VALUES
(1, 'User Login', '2024-01-15 08:30:00', '2024-01-15', 'UTC'),
(2, 'Purchase', '2024-01-15 10:45:30', '2024-01-15', 'UTC'),
(3, 'User Logout', '2024-01-15 17:20:15', '2024-01-15', 'UTC'),
(4, 'User Login', '2024-01-16 09:15:45', '2024-01-16', 'UTC'),
(5, 'Support Ticket', '2024-01-16 14:30:22', '2024-01-16', 'UTC'),
(6, 'Password Reset', '2024-01-17 11:22:33', '2024-01-17', 'UTC'),
(7, 'Account Update', '2024-01-18 16:45:18', '2024-01-18', 'UTC');

-- Comprehensive date/time operations
SELECT 
    event_name,
    event_timestamp,
    event_date,
    -- Extract components
    EXTRACT(YEAR FROM event_timestamp) as year,
    EXTRACT(MONTH FROM event_timestamp) as month,
    EXTRACT(DAY FROM event_timestamp) as day,
    EXTRACT(HOUR FROM event_timestamp) as hour,
    EXTRACT(MINUTE FROM event_timestamp) as minute,
    EXTRACT(DOW FROM event_timestamp) as day_of_week,
    EXTRACT(DOY FROM event_timestamp) as day_of_year,
    -- Date parts
    DATE_PART('week', event_timestamp) as week_of_year,
    DATE_PART('quarter', event_timestamp) as quarter,
    -- Formatting
    TO_CHAR(event_timestamp, 'YYYY-MM-DD HH24:MI:SS') as formatted_timestamp,
    TO_CHAR(event_timestamp, 'Day, Month DD, YYYY') as readable_date,
    TO_CHAR(event_timestamp, 'HH12:MI AM') as time_12hr,
    -- Date truncation
    DATE_TRUNC('day', event_timestamp) as day_start,
    DATE_TRUNC('week', event_timestamp) as week_start,
    DATE_TRUNC('month', event_timestamp) as month_start,
    -- Date arithmetic
    DATEADD('day', 7, event_date) as one_week_later,
    DATEADD('month', 1, event_date) as one_month_later,
    DATEADD('hour', -2, event_timestamp) as two_hours_earlier,
    -- Time differences
    DATEDIFF('day', event_date, CURRENT_DATE()) as days_since_event,
    DATEDIFF('hour', LAG(event_timestamp) OVER (ORDER BY event_timestamp), event_timestamp) as hours_since_last,
    -- Time zones
    CONVERT_TIMEZONE('UTC', 'America/New_York', event_timestamp) as eastern_time,
    CONVERT_TIMEZONE('UTC', 'Europe/London', event_timestamp) as london_time
FROM events
ORDER BY event_timestamp;

-- ============================================================================
-- SECTION 5: STRING FUNCTIONS
-- ============================================================================

-- Create customer data
CREATE OR REPLACE TABLE customers (
    customer_id NUMBER,
    full_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    notes STRING
);

INSERT INTO customers VALUES
(1, 'John Michael Smith Jr.', 'john.smith@email.com', '(555) 123-4567', '123 Main St, Apt 4B, Seattle, WA 98101', 'Preferred customer, VIP status'),
(2, 'Jane Marie Doe-Wilson', 'jane.doe@company.org', '555.987.6543', '456 Oak Avenue, Portland, OR 97201', 'Corporate account, bulk orders'),
(3, 'Robert James Wilson III', 'bob.wilson@domain.net', '555-456-7890', '789 Pine Road, San Francisco, CA 94102', 'Tech startup, fast-growing'),
(4, 'María González Rodriguez', 'maria.gonzalez@startup.io', '+1-555-321-9876', '321 Elm Street, Austin, TX 78701', 'Spanish-speaking, international');

-- Comprehensive string operations
SELECT 
    customer_id,
    full_name,
    -- Basic string functions
    UPPER(full_name) as name_upper,
    LOWER(full_name) as name_lower,
    INITCAP(full_name) as name_proper,
    LENGTH(full_name) as name_length,
    -- String extraction
    LEFT(full_name, 10) as first_10_chars,
    RIGHT(full_name, 10) as last_10_chars,
    SUBSTRING(full_name, 5, 10) as middle_chars,
    -- Name parsing
    SPLIT_PART(full_name, ' ', 1) as first_name,
    SPLIT_PART(full_name, ' ', 2) as middle_name,
    SPLIT_PART(full_name, ' ', -1) as last_name,
    -- Email processing
    email,
    LEFT(email, POSITION('@', email) - 1) as email_username,
    SPLIT_PART(email, '@', 2) as email_domain,
    CASE 
        WHEN email LIKE '%.com' THEN 'Commercial'
        WHEN email LIKE '%.org' THEN 'Organization'
        WHEN email LIKE '%.net' THEN 'Network'
        WHEN email LIKE '%.io' THEN 'Tech'
        ELSE 'Other'
    END as email_type,
    -- Phone number cleaning
    phone,
    REGEXP_REPLACE(phone, '[^0-9]', '') as clean_phone,
    REGEXP_REPLACE(phone, '\\+?1?[^0-9]*([0-9]{3})[^0-9]*([0-9]{3})[^0-9]*([0-9]{4})', '($1) $2-$3') as formatted_phone,
    -- Address parsing
    address,
    SPLIT_PART(address, ',', 1) as street_address,
    TRIM(SPLIT_PART(address, ',', -2)) as state,
    TRIM(SPLIT_PART(address, ',', -1)) as zip_code,
    -- Text search and replace
    CONTAINS(notes, 'VIP') as is_vip,
    CONTAINS(notes, 'corporate') as is_corporate,
    REPLACE(notes, 'customer', 'client') as updated_notes,
    -- Pattern matching
    REGEXP_COUNT(notes, '\\b\\w+\\b') as word_count,
    REGEXP_SUBSTR(full_name, '[A-Z][a-z]+') as first_word
FROM customers;

-- ============================================================================
-- SECTION 6: PERFORMANCE TESTING
-- ============================================================================

-- Create large dataset for performance testing
CREATE OR REPLACE TABLE large_sales (
    id NUMBER,
    product_category STRING,
    product_name STRING,
    sales_date DATE,
    amount NUMBER(10,2),
    customer_region STRING,
    customer_segment STRING,
    sales_rep STRING
);

-- Generate sample data (10,000 records)
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
    CASE (UNIFORM(1, 20, RANDOM()))
        WHEN 1 THEN 'Laptop' WHEN 2 THEN 'Phone' WHEN 3 THEN 'Tablet'
        WHEN 4 THEN 'Shirt' WHEN 5 THEN 'Pants' WHEN 6 THEN 'Shoes'
        WHEN 7 THEN 'Novel' WHEN 8 THEN 'Textbook' WHEN 9 THEN 'Magazine'
        WHEN 10 THEN 'Chair' WHEN 11 THEN 'Table' WHEN 12 THEN 'Lamp'
        WHEN 13 THEN 'Basketball' WHEN 14 THEN 'Tennis Racket'
        ELSE 'Generic Product'
    END as product_name,
    DATEADD('day', -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) as sales_date,
    ROUND(UNIFORM(10, 1000, RANDOM()), 2) as amount,
    CASE (UNIFORM(1, 4, RANDOM()))
        WHEN 1 THEN 'North'
        WHEN 2 THEN 'South'
        WHEN 3 THEN 'East'
        ELSE 'West'
    END as customer_region,
    CASE (UNIFORM(1, 3, RANDOM()))
        WHEN 1 THEN 'Enterprise'
        WHEN 2 THEN 'SMB'
        ELSE 'Consumer'
    END as customer_segment,
    CASE (UNIFORM(1, 8, RANDOM()))
        WHEN 1 THEN 'Alice' WHEN 2 THEN 'Bob' WHEN 3 THEN 'Carol'
        WHEN 4 THEN 'David' WHEN 5 THEN 'Eve' WHEN 6 THEN 'Frank'
        WHEN 7 THEN 'Grace' ELSE 'Henry'
    END as sales_rep
FROM TABLE(GENERATOR(ROWCOUNT => 10000));

-- Performance analysis queries
SELECT COUNT(*) as total_records FROM large_sales;

-- Complex analytical query
SELECT 
    customer_region,
    product_category,
    customer_segment,
    DATE_TRUNC('month', sales_date) as sales_month,
    COUNT(*) as transaction_count,
    SUM(amount) as total_sales,
    AVG(amount) as avg_transaction,
    MIN(amount) as min_sale,
    MAX(amount) as max_sale,
    STDDEV(amount) as stddev_amount,
    COUNT(DISTINCT sales_rep) as rep_count,
    SUM(amount) / COUNT(DISTINCT sales_rep) as sales_per_rep
FROM large_sales
WHERE sales_date >= DATEADD('month', -6, CURRENT_DATE())
GROUP BY customer_region, product_category, customer_segment, DATE_TRUNC('month', sales_date)
HAVING COUNT(*) > 5
ORDER BY sales_month DESC, total_sales DESC;

-- ============================================================================
-- SECTION 7: CONSTRAINTS AND RELATIONSHIPS
-- ============================================================================

-- Create products table with constraints
CREATE OR REPLACE TABLE products (
    product_id NUMBER NOT NULL,
    product_name STRING NOT NULL,
    category STRING NOT NULL,
    price NUMBER(10,2) NOT NULL CHECK (price > 0),
    cost NUMBER(10,2) CHECK (cost >= 0),
    margin_pct NUMBER(5,2) CHECK (margin_pct BETWEEN 0 AND 100),
    created_date DATE DEFAULT CURRENT_DATE(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    is_active BOOLEAN DEFAULT TRUE,
    inventory_count NUMBER DEFAULT 0 CHECK (inventory_count >= 0),
    CONSTRAINT pk_products PRIMARY KEY (product_id),
    CONSTRAINT chk_margin CHECK (price > cost)
);

-- Create orders table with foreign key
CREATE OR REPLACE TABLE orders (
    order_id NUMBER NOT NULL,
    product_id NUMBER NOT NULL,
    customer_name STRING NOT NULL,
    order_date DATE DEFAULT CURRENT_DATE(),
    quantity NUMBER NOT NULL CHECK (quantity > 0),
    unit_price NUMBER(10,2) NOT NULL,
    total_amount NUMBER(10,2) GENERATED ALWAYS AS (quantity * unit_price),
    status STRING DEFAULT 'Pending',
    CONSTRAINT pk_orders PRIMARY KEY (order_id),
    CONSTRAINT fk_orders_products FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Insert sample data
INSERT INTO products (product_id, product_name, category, price, cost, margin_pct, inventory_count) VALUES
(1, 'Laptop Pro 15"', 'Electronics', 1299.99, 800.00, 38.5, 25),
(2, 'Wireless Mouse', 'Electronics', 29.99, 15.00, 50.0, 150),
(3, 'Office Chair Deluxe', 'Furniture', 249.50, 150.00, 39.9, 12),
(4, 'LED Desk Lamp', 'Furniture', 79.99, 35.00, 56.3, 45),
(5, 'Bluetooth Headphones', 'Electronics', 159.99, 80.00, 50.0, 75);

INSERT INTO orders (order_id, product_id, customer_name, quantity, unit_price) VALUES
(101, 1, 'Alice Johnson', 1, 1299.99),
(102, 2, 'Bob Smith', 3, 29.99),
(103, 3, 'Carol Davis', 2, 249.50),
(104, 1, 'David Wilson', 1, 1299.99),
(105, 4, 'Eve Brown', 1, 79.99),
(106, 5, 'Frank Miller', 2, 159.99);

-- Complex join query with calculations
SELECT 
    o.order_id,
    o.customer_name,
    p.product_name,
    p.category,
    o.quantity,
    p.price as list_price,
    o.unit_price,
    o.total_amount,
    p.cost * o.quantity as total_cost,
    (o.total_amount - (p.cost * o.quantity)) as profit,
    ROUND(((o.total_amount - (p.cost * o.quantity)) / o.total_amount) * 100, 2) as profit_margin_pct,
    p.inventory_count,
    CASE 
        WHEN p.inventory_count < 20 THEN 'Low Stock'
        WHEN p.inventory_count < 50 THEN 'Medium Stock'
        ELSE 'High Stock'
    END as stock_status
FROM orders o
JOIN products p ON o.product_id = p.product_id
WHERE p.is_active = TRUE
ORDER BY o.order_date DESC, profit DESC;

-- ============================================================================
-- SECTION 8: METADATA EXPLORATION
-- ============================================================================

-- Explore database metadata
SELECT 
    table_catalog as database_name,
    table_schema as schema_name,
    table_name,
    table_type,
    row_count,
    bytes,
    created,
    last_altered
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'SANDBOX'
ORDER BY created DESC;

-- Column information for all tables
SELECT 
    table_name,
    column_name,
    ordinal_position,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    is_nullable,
    column_default,
    comment
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'SANDBOX'
ORDER BY table_name, ordinal_position;

-- View all constraints
SELECT 
    tc.constraint_name,
    tc.table_name,
    tc.constraint_type,
    kcu.column_name,
    tc.enforced,
    tc.deferrable,
    tc.initially_deferred
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
WHERE tc.table_schema = 'SANDBOX'
ORDER BY tc.table_name, tc.constraint_type, tc.constraint_name;
