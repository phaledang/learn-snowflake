-- 03_verify_sample_data.sql
-- Post-load validation & profiling queries

SELECT COUNT(*) AS row_count FROM sample_data;

SELECT MIN(purchase_date) AS min_purchase_date,
       MAX(purchase_date) AS max_purchase_date,
       COUNT(DISTINCT customer_id) AS distinct_customers,
       SUM(amount) AS total_revenue
FROM sample_data;

SELECT product_category,
       COUNT(*) AS row_count,
       SUM(amount) AS revenue,
       ROUND(AVG(amount),2) AS avg_amount
FROM sample_data
GROUP BY 1
ORDER BY 1;

SELECT region,
       COUNT(*) AS row_count,
       SUM(amount) AS revenue
FROM sample_data
GROUP BY 1
ORDER BY 1;

SELECT returned_flag,
       COUNT(*) AS row_count,
       SUM(amount) AS revenue
FROM sample_data
GROUP BY 1
ORDER BY 1;
