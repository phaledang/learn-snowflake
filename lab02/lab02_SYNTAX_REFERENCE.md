# Lab 02 Syntax Reference – Advanced SQL & Analytics

This reference explains the Snowflake SQL constructs used in the Lab 02 scripts:
- `sql/lab02_advanced_sql.sql`
- `sql/setup_window_functions_demo.sql`
- `sql/apply_window_functions.sql`

---
## 1. Session & Context
```sql
USE WAREHOUSE LEARN_WH;
USE DATABASE LEARN_SNOWFLAKE;
USE SCHEMA SANDBOX;
```
Switches active compute + object resolution context.

### Current Context Introspection
```sql
SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_DATABASE(), CURRENT_SCHEMA(),
       CURRENT_WAREHOUSE(), CURRENT_USER(), CURRENT_ROLE();
```
Retrieves session metadata for reproducibility/auditing.

---
## 2. Warehouse Provisioning (Multiple Sizes)
```sql
CREATE WAREHOUSE IF NOT EXISTS SMALL_WH WITH WAREHOUSE_SIZE='SMALL' AUTO_SUSPEND=300 AUTO_RESUME=TRUE;
CREATE WAREHOUSE IF NOT EXISTS MEDIUM_WH WITH WAREHOUSE_SIZE='MEDIUM' AUTO_SUSPEND=300 AUTO_RESUME=TRUE;
```
Shows scaling strategy—multiple compute clusters for workload separation.

---
## 3. Data Types & Semi‑Structured Columns
### Table with Mixed Types
```sql
CREATE TABLE IF NOT EXISTS data_types_demo (
  id NUMBER,
  name STRING,
  age NUMBER(3),
  salary NUMBER(10,2),
  is_active BOOLEAN,
  hire_date DATE,
  last_login TIMESTAMP_NTZ,
  metadata VARIANT,
  skills ARRAY,
  address OBJECT
);
```
**Highlights:**
- `VARIANT` – Generic holder for JSON / semi‑structured data.
- `ARRAY` – Ordered collection; use `ARRAY_SIZE()` and `[index]` access.
- `OBJECT` – Key/value map; access via `object:key`.
- `TIMESTAMP_NTZ` – Timestamp without time zone normalization.

### Loading Semi‑Structured Data
```sql
PARSE_JSON('{"department":"Finance"}')  -- Produces VARIANT
```
### Accessors & Casting
```sql
metadata:department::STRING      -- Dot navigation + cast
skills[0]::STRING                -- Array index
address:city::STRING             -- Object field
ARRAY_SIZE(skills)               -- Cardinality
```

---
## 4. Window Functions (Analytics Over Partitions)
### Base Pattern
```sql
<agg_or_analytic>(expr) OVER (
  PARTITION BY <cols>
  ORDER BY <sort_cols>
  [ROWS BETWEEN X PRECEDING AND CURRENT ROW]
)
```
### Examples Used
| Category | Functions | Notes |
|----------|-----------|-------|
| Running totals | `SUM()` | Partition varies (salesperson vs region) |
| Ranking | `RANK`, `DENSE_RANK`, `ROW_NUMBER` | Tie handling differences |
| Percentile | `PERCENT_RANK`, `NTILE(4)` | `NTILE` supplies bucket id |
| Moving avg | `AVG ... ROWS BETWEEN n PRECEDING AND CURRENT ROW` | Rolling window |
| Lag/Lead | `LAG`, `LEAD` | Offset-based lookback/lookahead |
| Percent-of-total | `value / SUM(value) OVER (...)` | Window denominator |

### Moving Window Syntax
```sql
AVG(sales_amount) OVER (
  PARTITION BY salesperson ORDER BY quarter
  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
) AS moving_avg_3q
```

---
## 5. Date & Time Functions
| Purpose | Syntax Examples | Notes |
|---------|-----------------|-------|
| Extract parts | `EXTRACT(YEAR FROM ts)` / `DATE_PART('week', ts)` | Both supported |
| Formatting | `TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS')` | Uses Oracle-like tokens |
| Truncation | `DATE_TRUNC('month', ts)` | Normalizes to boundary |
| Arithmetic | `DATEADD('day', 7, date_col)` | Adds interval |
| Difference | `DATEDIFF('hour', ts1, ts2)` | Signed integer |
| Time zones | `CONVERT_TIMEZONE('UTC','America/New_York', ts)` | Creates localized view |

---
## 6. String Functions & Pattern Processing
| Category | Examples | Purpose |
|----------|----------|---------|
| Case / length | `UPPER`, `LOWER`, `INITCAP`, `LENGTH` | Normalization |
| Substrings | `LEFT`, `RIGHT`, `SUBSTRING(str, start, length)` | Partial extraction |
| Splitting | `SPLIT_PART(full_name,' ',1)` | Token extraction by ordinal |
| Position | `POSITION('@', email)` | Locate substring index |
| Conditional classification | `CASE WHEN email LIKE '%.com' THEN 'Commercial' ...` | Categorization |
| Regex clean | `REGEXP_REPLACE(phone,'[^0-9]','')` | Strip non-digits |
| Regex format | Complex `REGEXP_REPLACE` with capture groups | Structured formatting |
| Regex extraction | `REGEXP_SUBSTR(full_name,'[A-Z][a-z]+')` | Pattern sampling |
| Contains | `CONTAINS(notes,'VIP')` | Boolean search |
| Replace | `REPLACE(notes,'customer','client')` | Simple substitution |

---
## 7. Large Data Generation
### Generator Pattern
```sql
FROM TABLE(GENERATOR(ROWCOUNT => 10000))
```
Produces synthetic row streams—paired with expressions for randomized columns.

### Random Distribution Functions
| Function | Role |
|----------|------|
| `UNIFORM(min,max,RANDOM())` | Pseudo-random integer in range |
| `RANDOM()` | Seed source for uniform calls |
| `SEQ4()` | Monotonic increasing sequence |

---
## 8. Analytical Aggregation with Filters
```sql
SELECT customer_region, product_category,
       DATE_TRUNC('month', sales_date) AS sales_month,
       COUNT(*) AS transaction_count,
       SUM(amount) AS total_sales,
       AVG(amount) AS avg_transaction,
       HAVING COUNT(*) > 5
```
**Key Concepts:**
- `HAVING` applies post-aggregation filters.
- `DATE_TRUNC` used for temporal bucketing.
- `COUNT(DISTINCT sales_rep)` for unique actors.

---
## 9. Constraints & Generated Columns
### Example Table with Checks & Primary Key
```sql
CREATE OR REPLACE TABLE products (
  product_id NUMBER NOT NULL,
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
```
### Generated Column
```sql
NUMERIC_COL GENERATED ALWAYS AS (quantity * unit_price)
```
**Benefit:** Persisted calculated value—read performance & consistency.

### Foreign Key
```sql
CONSTRAINT fk_orders_products FOREIGN KEY (product_id) REFERENCES products(product_id)
```
(Relationship not enforced for performance by default—document semantics.)

---
## 10. Complex Join with Derived Metrics
```sql
SELECT o.order_id, p.product_name,
       (o.total_amount - (p.cost * o.quantity)) AS profit,
       ROUND(((o.total_amount - (p.cost * o.quantity)) / o.total_amount) * 100, 2) AS profit_margin_pct
FROM orders o
JOIN products p ON o.product_id = p.product_id
WHERE p.is_active = TRUE
ORDER BY o.order_date DESC, profit DESC;
```
**Highlights:** Financial margin computation + inventory classification via `CASE`.

---
## 11. Metadata Introspection via INFORMATION_SCHEMA
```sql
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_schema='SANDBOX';
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema='SANDBOX';
```
Plus constraints overview joining `TABLE_CONSTRAINTS` and `KEY_COLUMN_USAGE`.

---
## 12. Window-Based Data Quality & De‑duplication
### Setup Table
```sql
CREATE TABLE IF NOT EXISTS employee_events (...);
DELETE FROM employee_events;  -- Idempotent reset
```
### Exact Duplicate Detection
```sql
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY emp_id, name, department, event_date, salary
  ORDER BY event_date, emp_id
) > 1
```
### Safe Delete Using Derived Row Numbers
```sql
DELETE FROM employee_events t
USING (
  SELECT emp_id, name, department, event_date, salary,
         ROW_NUMBER() OVER (PARTITION BY emp_id, name, department, event_date, salary ORDER BY event_date, emp_id) AS rn
  FROM employee_events
) d
WHERE d.rn > 1
  AND t.emp_id = d.emp_id
  AND t.name IS NOT DISTINCT FROM d.name
  ...;
```
**Why `IS NOT DISTINCT FROM`?** Treats `NULL` equality safely in dedupe logic.

### Signature / HASH-Based Alternate
```sql
HASH(emp_id, name, department, event_date, salary)
```
Used for compact duplicate grouping key.

### Latest Record per Entity
```sql
ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY event_date DESC) = 1
```
Creates current snapshot.

### Trend / Delta Analysis
```sql
LAG(salary) OVER (PARTITION BY emp_id ORDER BY event_date) AS prev_salary
(salary - LAG(salary) ...) AS delta_from_prev
DATEDIFF('day', LAG(event_date) OVER (...), event_date) AS days_since_prev
```

---
## 13. QUALIFY Clause
```sql
SELECT *
FROM employee_events
QUALIFY ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY event_date DESC) = 1;
```
Filters on window function results **after** they are computed (Snowflake extension; avoids subquery nesting).

---
## 14. Performance & Optimization Notes
| Feature | Tip |
|---------|-----|
| Large synthetic sets | Use `GENERATOR` + randomized expressions |
| Window functions | Minimize partition columns for performance |
| Semi-structured | Cast early when needed for joins / grouping |
| Constraints | Snowflake stores but does not always enforce (FK) – rely on data stewardship |
| De-duplication | Prefer deterministic ordering in `ROW_NUMBER()` |

---
**Next:** Proceed to Lab 03 for bulk loading (`COPY INTO`) and file format handling.
