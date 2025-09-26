-- setup_window_functions_demo.sql
-- Optional: reset the demo table completely
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
