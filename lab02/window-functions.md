# Snowflake Window Functions

This Markdown file documents how window functions work in Snowflake and provides ready-to-run SQL for setup, analysis, de-duplication, and ranking.

---

## Why “Window” Functions?

A *window* is the set of rows a function can “see” when computing a value for the current row. You define that window using:

- `PARTITION BY` – splits rows into groups (the window partitions).
- `ORDER BY` – defines the order within each partition.

Unlike `GROUP BY` (which collapses rows), window functions keep **every row** and add calculated values per row.

---

## Functions Covered

- `ROW_NUMBER()` – sequential numbering within a partition (great for deduping).
- `RANK()` – ranking with gaps when ties occur.
- `DENSE_RANK()` – ranking without gaps.
- `LAG()` – access a previous row’s value.
- `LEAD()` – access a next row’s value.

---

## Setup Script

> File: `setup_window_functions_demo.sql`

```sql
-- Optional: reset the demo table
-- DROP TABLE IF EXISTS employee_events;

CREATE TABLE IF NOT EXISTS employee_events (
    emp_id      NUMBER,
    name        STRING,
    department  STRING,
    event_date  DATE,
    salary      NUMBER(10,2)
);

-- Clear existing rows for idempotent reruns (optional)
DELETE FROM employee_events;

-- Seed data (includes an intentional duplicate row)
INSERT INTO employee_events (emp_id, name, department, event_date, salary) VALUES
    -- IT dept
    (101, 'Alice Johnson', 'IT',        '2024-01-01', 70000.00),
    (101, 'Alice Johnson', 'IT',        '2024-06-01', 74000.00),
    (101, 'Alice Johnson', 'IT',        '2025-01-01', 78000.00),
    -- deliberate exact duplicate row (to dedupe)
    (101, 'Alice Johnson', 'IT',        '2024-01-01', 70000.00),

    (102, 'Bob Smith',     'IT',        '2024-02-15', 72000.00),
    (102, 'Bob Smith',     'IT',        '2024-09-01', 75000.00),

    (105, 'Eve Tran',      'IT',        '2024-03-10', 71000.00),
    (105, 'Eve Tran',      'IT',        '2025-03-10', 77000.00),

    -- Marketing dept
    (201, 'Carol Lee',     'Marketing', '2024-01-10', 68000.00),
    (201, 'Carol Lee',     'Marketing', '2024-10-10', 71000.00),

    (202, 'Dave Nguyen',   'Marketing', '2024-04-01', 69000.00),
    (202, 'Dave Nguyen',   'Marketing', '2025-02-01', 72000.00);
```
---

## Apply Script (Analytics + De-dup)

> File: `apply_window_functions.sql`

### A) Find exact duplicates (preview only)

```sql
SELECT *
FROM employee_events
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY emp_id, name, department, event_date, salary
  ORDER BY event_date ASC, emp_id ASC
) > 1;
```

### B) Delete only the second-and-beyond exact duplicates (keep the first)

```sql
DELETE FROM employee_events t
USING (
  SELECT
    emp_id, name, department, event_date, salary,
    ROW_NUMBER() OVER (
      PARTITION BY emp_id, name, department, event_date, salary
      ORDER BY event_date ASC, emp_id ASC
    ) AS rn
  FROM employee_events
) d
WHERE d.rn > 1
  AND t.emp_id = d.emp_id
  AND t.name IS NOT DISTINCT FROM d.name
  AND t.department IS NOT DISTINCT FROM d.department
  AND t.event_date IS NOT DISTINCT FROM d.event_date
  AND t.salary IS NOT DISTINCT FROM d.salary;
```

> **Why those comparisons?**  
> They map the duplicate rows identified in the subquery (`d`) back to the base table (`t`) using NULL-safe matching. Only rows with `rn > 1` are deleted.

### C) Alternative: Use a one-field signature for matching (HASH)

```sql
-- Preview duplicates by signature
SELECT *
FROM employee_events
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY HASH(emp_id, name, department, event_date, salary)
  ORDER BY event_date ASC, emp_id ASC
) > 1;

-- Delete duplicates using the same signature
DELETE FROM employee_events t
USING (
  SELECT
    HASH(emp_id, name, department, event_date, salary) AS sig,
    ROW_NUMBER() OVER (
      PARTITION BY HASH(emp_id, name, department, event_date, salary)
      ORDER BY event_date ASC, emp_id ASC
    ) AS rn
  FROM employee_events
) d
WHERE d.rn > 1
  AND HASH(t.emp_id, t.name, t.department, t.event_date, t.salary) = d.sig;
```

---

## Department Rankings (RANK vs DENSE_RANK)

```sql
-- Latest salary row per employee
WITH latest_per_emp AS (
  SELECT *
  FROM (
    SELECT
      emp_id, name, department, event_date, salary,
      ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY event_date DESC) AS rn
    FROM employee_events
  )
  WHERE rn = 1
)
SELECT
  department,
  name,
  salary,
  RANK()       OVER (PARTITION BY department ORDER BY salary DESC) AS rnk,
  DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS drnk
FROM latest_per_emp
ORDER BY department, rnk, name;
```

---

## Trend Analysis (LAG / LEAD)

```sql
SELECT
  emp_id,
  name,
  department,
  event_date,
  salary,
  LAG(salary) OVER (PARTITION BY emp_id ORDER BY event_date) AS prev_salary,
  LEAD(salary) OVER (PARTITION BY emp_id ORDER BY event_date) AS next_salary,
  (salary - LAG(salary) OVER (PARTITION BY emp_id ORDER BY event_date)) AS delta_from_prev,
  DATEDIFF('day',
           LAG(event_date) OVER (PARTITION BY emp_id ORDER BY event_date),
           event_date) AS days_since_prev
FROM employee_events
ORDER BY emp_id, event_date;
```

---

## Why the Name “Window Function”?

Each row computes a value by “looking at” a *window* of related rows defined by `PARTITION BY` (which rows are included) and `ORDER BY` (their sequence). The row keeps all its columns plus the calculated value—unlike `GROUP BY`, which collapses rows.

---

## Tips

- Keep the **same column list and order** when using `HASH(...)` for signatures.
- For NULL-safe equality, use `IS NOT DISTINCT FROM` (Snowflake’s equivalent to MySQL’s `<=>`).
- Wrap deletes in transactions during development:
  ```sql
  BEGIN;
  -- run DELETE here
  -- sanity check with SELECT COUNT(*) ...
  COMMIT; -- or ROLLBACK;
  ```

---

## Next Steps

- Add **Top-N per department** with `RANK()` and a `WHERE rnk <= N` filter.
- Try `SUM() OVER (...)`, `AVG() OVER (...)` for running totals/moving averages.
- Explore `QUALIFY` to filter on window results without subqueries.
