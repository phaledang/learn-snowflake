# Lab 01 Syntax Reference

This document explains the Snowflake SQL syntax patterns demonstrated in `sql/lab01_getting_started.sql`.

## 1. Warehouse Management
### CREATE WAREHOUSE
```sql
CREATE WAREHOUSE IF NOT EXISTS LEARN_WH
WITH WAREHOUSE_SIZE = 'X-SMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
COMMENT = 'Warehouse for Snowflake learning labs';
```
**Purpose:** Provision a virtual warehouse used for compute (query execution, loading, etc.).

**Key Options:**
- `IF NOT EXISTS` – Avoids error if already created.
- `WAREHOUSE_SIZE` – Scale of compute cluster (e.g. X-SMALL, SMALL, MEDIUM...).
- `AUTO_SUSPEND` (seconds) – Idle time before Snowflake automatically suspends the warehouse to save credits.
- `AUTO_RESUME` – Automatically resumes when a query is issued.
- `COMMENT` – Metadata for documentation.

### USE WAREHOUSE
```sql
USE WAREHOUSE LEARN_WH;
```
Sets the active warehouse for subsequent queries.

### ALTER WAREHOUSE (Suspend / Resume)
```sql
ALTER WAREHOUSE LEARN_WH SUSPEND;  -- Manually pause
ALTER WAREHOUSE LEARN_WH RESUME;   -- Manually resume
```
**When to use:** To explicitly control credit usage outside of `AUTO_SUSPEND` behavior.

### SHOW WAREHOUSES / SHOW WAREHOUSES LIKE
```sql
SHOW WAREHOUSES;             -- List all accessible warehouses
SHOW WAREHOUSES LIKE 'LEARN_WH';  -- Filtered by name
```
Lists warehouse metadata (state, size, owner, scaling settings, etc.).

### RESULT_SCAN + LAST_QUERY_ID() Pattern
```sql
SELECT "name", "state", "size", "auto_suspend", "auto_resume"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
```
**Purpose:** Re-query the result set of the *previous* `SHOW WAREHOUSES` command to filter/project specific columns.

**Notes:**
- `LAST_QUERY_ID()` returns the ID of the immediately preceding statement.
- `RESULT_SCAN(<query_id>)` materializes the result for further SQL processing.

## 2. Database & Schema Management
### CREATE DATABASE
```sql
CREATE DATABASE IF NOT EXISTS LEARN_SNOWFLAKE
COMMENT = 'Database for Snowflake learning labs';
```
Creates a logical container for schemas and objects.

### USE DATABASE / USE SCHEMA
```sql
USE DATABASE LEARN_SNOWFLAKE;
USE SCHEMA SANDBOX;
```
Switches the active database / schema context.

### CREATE SCHEMA
```sql
CREATE SCHEMA IF NOT EXISTS SANDBOX
COMMENT = 'Schema for practice and experimentation';
```
Defines a namespace inside a database.

## 3. Table Definition & Data Loading
### CREATE OR REPLACE TABLE
```sql
CREATE OR REPLACE TABLE employees (
    id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    name STRING,
    department STRING,
    salary NUMBER,
    hire_date DATE
);
```
**Highlights:**
- `OR REPLACE` – Recreates table if it exists (drops previous data!).
- `AUTOINCREMENT START 1 INCREMENT 1` – Surrogate key generator.
- `STRING` – Alias for `VARCHAR` in Snowflake.
- `NUMBER` – Flexible numeric; can be unbounded precision or specified as `NUMBER(p,s)`.

### INSERT INTO (Multi-row)
```sql
INSERT INTO employees (name, department, salary, hire_date) VALUES
('John Smith', 'Engineering', 75000, '2023-01-15'),
('Jane Doe', 'Marketing', 65000, '2023-02-20'),
...;
```
Multi-row insert pattern for seeding sample data.

## 4. Basic Retrieval & Metadata
### SELECT *
```sql
SELECT * FROM employees;
```
Retrieves all columns (useful for exploration; avoid in production unless intentional).

### COUNT(*) Alias
```sql
SELECT COUNT(*) AS total_employees FROM employees;
```
Provides row count with a readable alias.

### DESCRIBE TABLE
```sql
DESCRIBE TABLE employees;
```
Displays column metadata, types, defaults, comments.

## 5. Aggregation & Grouping
### GROUP BY with ORDER BY
```sql
SELECT department, COUNT(*) AS employee_count
FROM employees
GROUP BY department
ORDER BY employee_count DESC;
```
Counts rows per department and orders by count (descending).

### Aggregated Salary Statistics
```sql
SELECT department, 
       COUNT(*) AS employee_count,
       AVG(salary) AS avg_salary,
       MIN(salary) AS min_salary,
       MAX(salary) AS max_salary,
       SUM(salary) AS total_salary
FROM employees
GROUP BY department
ORDER BY avg_salary DESC;
```
Demonstrates multiple aggregate functions in one pass.

## 6. Filtering & Conditional Logic
### WHERE with Comparison
```sql
SELECT name, department, salary
FROM employees
WHERE salary > 70000
ORDER BY salary DESC;
```
Filters on salary threshold.

### Date Filtering with YEAR()
```sql
SELECT name, department, hire_date
FROM employees
WHERE YEAR(hire_date) = 2023
ORDER BY hire_date;
```
Extracts year component from a `DATE` for filtering.

## 7. Context Inspection
### CURRENT Context Functions
```sql
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();
```
Returns active session context.

## 8. Optional Cleanup Pattern
```sql
-- DROP TABLE IF EXISTS employees;
-- ALTER WAREHOUSE LEARN_WH SUSPEND;
```
Commented cleanup steps for controlled teardown.

## 9. Common Variations / Tips
| Pattern | Variation | When to Use |
|---------|----------|-------------|
| `CREATE OR REPLACE TABLE` | `CREATE TABLE IF NOT EXISTS` | Preserve data if table may exist |
| `AUTOINCREMENT` | `IDENTITY(start, step)` | Alternate sequence syntax |
| `RESULT_SCAN(LAST_QUERY_ID())` | Store ID: `SET qid=LAST_QUERY_ID();` then reuse | When chaining multiple post-processing steps |
| `AVG()` | `ROUND(AVG(col),2)` | Presentation-friendly numeric output |
| Date filter with `YEAR()` | `hire_date BETWEEN '2023-01-01' AND '2023-12-31'` | SARGable range predicate |

## 10. Error Prevention Tips
- Always prefer `IF NOT EXISTS` during iterative learning to avoid accidental failures.
- Use `SHOW ...` + `RESULT_SCAN` instead of manual copying for metadata inspection.
- Avoid `OR REPLACE` unless you're okay losing existing data.
- Use explicit column lists in `INSERT` for clarity and schema safety.
- Add `ORDER BY` explicitly—Snowflake does not guarantee implicit ordering.

---
**Next:** Proceed to Lab 02 for analytic/window function patterns.
