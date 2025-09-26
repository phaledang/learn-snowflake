
/* ============================
 A) De-duplication with ROW_NUMBER
    Rule: rows are duplicates if ALL columns match.
    Keep the FIRST row per duplicate group (earliest event_date, then emp_id).
============================ */

-- Query data first
 SELECT emp_id, name, department, event_date, salary, rn
  FROM (
    SELECT emp_id, name, department, event_date, salary,
           ROW_NUMBER() OVER (
             PARTITION BY emp_id, name, department, event_date, salary
             ORDER BY event_date ASC, emp_id ASC
           ) AS rn
    FROM employee_events
  )

-- Preview which rows would be deleted (rn > 1)
SELECT *
FROM employee_events
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY emp_id, name, department, event_date, salary
  ORDER BY event_date ASC, emp_id ASC
) > 1;

-- Delete only exact duplicates, keeping the first
-- Preview which rows would be deleted (rn > 1)
SELECT *
FROM employee_events
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY emp_id, name, department, event_date, salary
  ORDER BY event_date ASC, emp_id ASC
) > 1;

-- Delete only the second-and-beyond identical rows
DELETE FROM employee_events t
USING (
  SELECT emp_id, name, department, event_date, salary
  FROM (
    SELECT
      emp_id, name, department, event_date, salary,
      ROW_NUMBER() OVER (
        PARTITION BY emp_id, name, department, event_date, salary
        ORDER BY event_date ASC, emp_id ASC
      ) AS rn
    FROM employee_events
  )
  WHERE rn > 1
) d
WHERE t.emp_id = d.emp_id
  AND (t.name       IS NOT DISTINCT FROM d.name)
  AND (t.department IS NOT DISTINCT FROM d.department)
  AND (t.event_date IS NOT DISTINCT FROM d.event_date)
  AND (t.salary     IS NOT DISTINCT FROM d.salary);


-- Verify no duplicates remain
SELECT emp_id, name, department, event_date, salary, COUNT(*) AS cnt
FROM employee_events
GROUP BY 1,2,3,4,5
HAVING COUNT(*) > 1;


/* ============================
 B) Department rankings with RANK and DENSE_RANK
    We want the latest salary per employee, then rank by salary within department.
============================ */

-- Latest salary row per employee
WITH latest_per_emp AS (
  SELECT *
  FROM (
    SELECT
      emp_id, name, department, event_date, salary,
      ROW_NUMBER() OVER (
        PARTITION BY emp_id
        ORDER BY event_date DESC
      ) AS rn
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


/* ============================
 C) Trend analysis with LAG / LEAD
    For each employee, show previous/next salary and deltas over time.
============================ */

SELECT
  emp_id,
  name,
  department,
  event_date,
  salary,
  LAG(salary) OVER (PARTITION BY emp_id ORDER BY event_date) AS prev_salary,
  LEAD(salary) OVER (PARTITION BY emp_id ORDER BY event_date) AS next_salary,
  -- salary change from previous event
  (salary - LAG(salary) OVER (PARTITION BY emp_id ORDER BY event_date)) AS delta_from_prev,
  -- days since previous event
  DATEDIFF('day',
           LAG(event_date) OVER (PARTITION BY emp_id ORDER BY event_date),
           event_date) AS days_since_prev
FROM employee_events
ORDER BY emp_id, event_date;


/* ============================
 D) (Optional) Top-N per department example
    Get top 2 earners per department using RANK on latest salaries.
============================ */

WITH latest_per_emp AS (
  SELECT *
  FROM (
    SELECT
      emp_id, name, department, event_date, salary,
      ROW_NUMBER() OVER (
        PARTITION BY emp_id
        ORDER BY event_date DESC
      ) AS rn
    FROM employee_events
  )
  WHERE rn = 1
)
SELECT *
FROM (
  SELECT
    department,
    name,
    salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk
  FROM latest_per_emp
)
WHERE rnk <= 2
ORDER BY department, rnk, name;

-- query data again
 SELECT emp_id, name, department, event_date, salary, rn
  FROM (
    SELECT emp_id, name, department, event_date, salary,
           ROW_NUMBER() OVER (
             PARTITION BY emp_id, name, department, event_date, salary
             ORDER BY event_date ASC, emp_id ASC
           ) AS rn
    FROM employee_events
  )
