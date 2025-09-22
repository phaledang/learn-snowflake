-- Lab 01: Getting Started with Snowflake
-- SQL Scripts for hands-on practice

-- ============================================================================
-- SECTION 1: ENVIRONMENT SETUP
-- ============================================================================

-- Create and configure virtual warehouse
CREATE WAREHOUSE IF NOT EXISTS LEARN_WH
WITH WAREHOUSE_SIZE = 'X-SMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
COMMENT = 'Warehouse for Snowflake learning labs';

-- Use the warehouse
USE WAREHOUSE LEARN_WH;

-- Create database for our labs
CREATE DATABASE IF NOT EXISTS LEARN_SNOWFLAKE
COMMENT = 'Database for Snowflake learning labs';

-- Use the database
USE DATABASE LEARN_SNOWFLAKE;

-- Create schema for practice
CREATE SCHEMA IF NOT EXISTS SANDBOX
COMMENT = 'Schema for practice and experimentation';

-- Use the schema
USE SCHEMA SANDBOX;

-- ============================================================================
-- SECTION 2: FIRST TABLE AND DATA
-- ============================================================================

-- Create employee table
CREATE OR REPLACE TABLE employees (
    id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    name STRING,
    department STRING,
    salary NUMBER,
    hire_date DATE
);

-- Insert sample data
INSERT INTO employees (name, department, salary, hire_date) VALUES
('John Smith', 'Engineering', 75000, '2023-01-15'),
('Jane Doe', 'Marketing', 65000, '2023-02-20'),
('Bob Johnson', 'Sales', 60000, '2023-03-10'),
('Alice Brown', 'Engineering', 80000, '2023-01-25'),
('Charlie Wilson', 'HR', 55000, '2023-04-05'),
('Diana Prince', 'Engineering', 85000, '2023-02-01'),
('Clark Kent', 'Marketing', 62000, '2023-03-15'),
('Bruce Wayne', 'Sales', 70000, '2023-01-20');

-- ============================================================================
-- SECTION 3: BASIC QUERIES
-- ============================================================================

-- View all employees
SELECT * FROM employees;

-- Count total employees
SELECT COUNT(*) as total_employees FROM employees;

-- Show table structure
DESCRIBE TABLE employees;

-- ============================================================================
-- SECTION 4: INTERMEDIATE QUERIES
-- ============================================================================

-- Count employees by department
SELECT department, COUNT(*) as employee_count
FROM employees
GROUP BY department
ORDER BY employee_count DESC;

-- Find highest paid employees
SELECT name, department, salary
FROM employees
WHERE salary > 70000
ORDER BY salary DESC;

-- Calculate salary statistics by department
SELECT department, 
       COUNT(*) as employee_count,
       AVG(salary) as avg_salary,
       MIN(salary) as min_salary,
       MAX(salary) as max_salary,
       SUM(salary) as total_salary
FROM employees
GROUP BY department
ORDER BY avg_salary DESC;

-- Find employees hired in 2023
SELECT name, department, hire_date
FROM employees
WHERE YEAR(hire_date) = 2023
ORDER BY hire_date;

-- ============================================================================
-- SECTION 5: WAREHOUSE AND SYSTEM QUERIES
-- ============================================================================

-- Check current context
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();

-- Show warehouses
SHOW WAREHOUSES;

-- Check warehouse status
SELECT "name", "state", "size", "auto_suspend", "auto_resume"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ============================================================================
-- SECTION 6: WAREHOUSE MANAGEMENT
-- ============================================================================

-- Suspend warehouse (save credits)
ALTER WAREHOUSE LEARN_WH SUSPEND;

-- Resume warehouse
ALTER WAREHOUSE LEARN_WH RESUME;

-- Check warehouse status
SHOW WAREHOUSES LIKE 'LEARN_WH';

-- ============================================================================
-- SECTION 7: CLEANUP (OPTIONAL)
-- ============================================================================

-- Uncomment the following lines if you want to clean up:
-- DROP TABLE IF EXISTS employees;
-- ALTER WAREHOUSE LEARN_WH SUSPEND;

-- Note: We keep the database and warehouse for future labs