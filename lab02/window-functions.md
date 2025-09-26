# Snowflake Window Functions

## Why “Window” Functions?

A *window* is the set of rows a function can “see” when computing a value for the current row.  
Defined by:

- `PARTITION BY` – splits rows into groups (like little windows).  
- `ORDER BY` – defines order within each group.  

Unlike `GROUP BY`, window functions do not collapse rows. They keep each row and add an extra calculated value.

---

## Functions Covered

### 1. ROW_NUMBER()
Assigns a sequential number to rows in each partition.  
- ✅ Unique per row.  
- ✅ Useful for de-duplication, pagination.

**Example:**
```sql
SELECT emp_id, name, event_date, salary,
       ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY event_date ASC) AS rn
FROM employee_events;
```
👉 First row per employee has `rn = 1`. Later records get higher numbers.

---

### 2. RANK()
Assigns ranking numbers **with gaps** if ties occur.  
- ✅ Useful for competitions or leaderboards.

**Step-by-step Example:**  
| Name   | Salary | `RANK()` |
|--------|--------|----------|
| Alice  | 100000 | 1        |
| Bob    | 100000 | 1        | ← tie group of 2 rows  
| Carol  | 95000  | 3        | ← jump: 1 + 2 tied rows = 3  
| Dave   | 90000  | 4        |

👉 That’s why you see `1, 1, 3, 4`. Rank **jumps** after a tie.

**Race analogy (competition style):**
```
Podium (RANK)
   #1   #1   #3
 Alice Bob  Carol
```

---

### 3. DENSE_RANK()
Assigns ranking numbers **without gaps**.  
- ✅ Useful when continuity of ranking matters.

**Example:**  
| Name   | Salary | `DENSE_RANK()` |
|--------|--------|----------------|
| Alice  | 100000 | 1              |
| Bob    | 100000 | 1              | ← tie group  
| Carol  | 95000  | 2              | ← no gap  
| Dave   | 90000  | 3              |

**Race analogy (dense style):**
```
Podium (DENSE_RANK)
   #1   #1   #2
 Alice Bob  Carol
```

✅ Rule of thumb:  
- Use `RANK()` when you want “competition ranks” (with gaps).  
- Use `DENSE_RANK()` when you want “dense ranks” (no gaps).

---

### 4. LAG()
Fetches a value from the **previous row**.  
- ✅ Compare current vs. previous record.  
- ✅ Calculate changes/differences.

**Snowflake Example:**
```sql
SELECT emp_id, event_date, salary,
       LAG(salary) OVER (PARTITION BY emp_id ORDER BY event_date) AS prev_salary
FROM employee_events
WHERE emp_id = 101;
```

**Sample Output:**
| emp_id | event_date | salary | prev_salary |
|--------|------------|--------|-------------|
| 101    | 2024-01-01 | 70000  | NULL        |
| 101    | 2024-06-01 | 74000  | 70000       |
| 101    | 2025-01-01 | 78000  | 74000       |

👉 Pulls the salary from the earlier row.

---

### 5. LEAD()
Fetches a value from the **next row**.  
- ✅ Compare with upcoming data.  
- ✅ Forecast changes.

**Snowflake Example:**
```sql
SELECT emp_id, event_date, salary,
       LEAD(salary) OVER (PARTITION BY emp_id ORDER BY event_date) AS next_salary
FROM employee_events
WHERE emp_id = 101;
```

**Sample Output:**
| emp_id | event_date | salary | next_salary |
|--------|------------|--------|-------------|
| 101    | 2024-01-01 | 70000  | 74000       |
| 101    | 2024-06-01 | 74000  | 78000       |
| 101    | 2025-01-01 | 78000  | NULL        |

👉 Pulls the salary from the next row.

---

### 6. Combined LAG + LEAD
```sql
SELECT emp_id, event_date, salary,
       LAG(salary) OVER (PARTITION BY emp_id ORDER BY event_date) AS prev_salary,
       LEAD(salary) OVER (PARTITION BY emp_id ORDER BY event_date) AS next_salary,
       salary - LAG(salary) OVER (PARTITION BY emp_id ORDER BY event_date) AS delta_from_prev
FROM employee_events
WHERE emp_id = 101;
```

**Output:**
| emp_id | event_date | salary | prev_salary | next_salary | delta_from_prev |
|--------|------------|--------|-------------|-------------|-----------------|
| 101    | 2024-01-01 | 70000  | NULL        | 74000       | NULL            |
| 101    | 2024-06-01 | 74000  | 70000       | 78000       | 4000            |
| 101    | 2025-01-01 | 78000  | 74000       | NULL        | 4000            |

---

## SQL Server Comparison

Both `LAG()` and `LEAD()` are also available in **Microsoft SQL Server (since 2012)**.  
Syntax is nearly identical to Snowflake.

**SQL Server Syntax:**
```sql
LAG ( scalar_expression [ , offset , default ] ) 
  OVER ( [ PARTITION BY partition_expression ] ORDER BY order_expression )

LEAD ( scalar_expression [ , offset , default ] ) 
  OVER ( [ PARTITION BY partition_expression ] ORDER BY order_expression )
```

**Snowflake Syntax:**
```sql
LAG ( <expr> [ , <offset> , <default> ] ) 
  OVER ( [ PARTITION BY <expr1> ] ORDER BY <expr2> )

LEAD ( <expr> [ , <offset> , <default> ] ) 
  OVER ( [ PARTITION BY <expr1> ] ORDER BY <expr2> )
```

✅ Example query is **identical** in both systems:  
```sql
SELECT emp_id, event_date, salary,
       LAG(salary, 1, 0)  OVER (PARTITION BY emp_id ORDER BY event_date) AS prev_salary,
       LEAD(salary, 1, 0) OVER (PARTITION BY emp_id ORDER BY event_date) AS next_salary
FROM employee_events;
```

---

## Visual Summary

| Function     | Purpose                               | Ties Handling     | Typical Use Case                     |
|--------------|---------------------------------------|-------------------|--------------------------------------|
| ROW_NUMBER() | Sequential numbering                  | Always unique     | De-duplication, pagination           |
| RANK()       | Ranking with gaps on ties             | Skips ranks       | Competitions, leaderboards           |
| DENSE_RANK() | Ranking without gaps on ties          | No skips          | Trend analysis, dense ranking        |
| LAG()        | Value from a previous row             | N/A               | Compare with earlier data            |
| LEAD()       | Value from a following row            | N/A               | Forecast, compare with future values |

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
