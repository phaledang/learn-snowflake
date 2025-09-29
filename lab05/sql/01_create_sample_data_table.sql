-- 01_create_sample_data_table.sql
-- DDL for sample_data table used in Lab05

CREATE OR REPLACE TABLE sample_data (
    customer_id        INT,
    customer_name      STRING,
    email              STRING,
    region             STRING,
    product_category   STRING,
    purchase_date      TIMESTAMP_NTZ,
    amount             NUMBER(10,2),
    channel            STRING,
    payment_type       STRING,
    quantity           INT,
    discount_pct       FLOAT,
    returned_flag      BOOLEAN
);
