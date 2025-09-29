-- Snowflake requires the WITH clause to be part of the statement; attach it to INSERT.
INSERT INTO sample_data
WITH
nums AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS n
    FROM TABLE(GENERATOR(ROWCOUNT=>1000))
),
base AS (
    SELECT
        n,
        ( (n - 1) % 160 ) + 1 AS customer_id_raw,
        CASE ((n-1) % 5)
            WHEN 0 THEN 'North'
            WHEN 1 THEN 'South'
            WHEN 2 THEN 'East'
            WHEN 3 THEN 'West'
            ELSE 'Central'
        END AS region,
        CASE ((n-1) % 8)
            WHEN 0 THEN 'Electronics'
            WHEN 1 THEN 'Books'
            WHEN 2 THEN 'Home'
            WHEN 3 THEN 'Sports'
            WHEN 4 THEN 'Clothing'
            WHEN 5 THEN 'Grocery'
            WHEN 6 THEN 'Toys'
            ELSE 'Health'
        END AS product_category,
        TO_TIMESTAMP_NTZ(
            DATEADD(
                minute, (n*7) % (24*60),
                DATEADD(
                    day, ((n*13) % 28),
                    DATEADD(month, ((n-1) % 12), DATE '2024-01-01')
                )
            )
        ) AS purchase_date,
        ((
            CASE ((n-1) % 8)
                WHEN 0 THEN 100 + ((n*17) % 500)
                WHEN 1 THEN 10 + ((n*23) % 35)
                WHEN 2 THEN 20 + ((n*19) % 160)
                WHEN 3 THEN 15 + ((n*13) % 200)
                WHEN 4 THEN 15 + ((n*11) % 130)
                WHEN 5 THEN 5  + ((n*29) % 50)
                WHEN 6 THEN 8  + ((n*31) % 80)
                ELSE 8 + ((n*37) % 110)
            END
        ) * (CASE WHEN (n % 9)=0 THEN 3 WHEN (n % 5)=0 THEN 2 ELSE 1 END))::NUMBER(10,2) AS gross_amount,
        CASE (n % 10)
            WHEN 0 THEN 0.20
            WHEN 1 THEN 0.15
            WHEN 2 THEN 0.10
            WHEN 3 THEN 0.05
            ELSE 0.00
        END AS discount_pct,
        CASE ((n-1) % 4)
            WHEN 0 THEN 'web'
            WHEN 1 THEN 'mobile'
            WHEN 2 THEN 'store'
            ELSE 'partner'
        END AS channel,
        CASE ((n-1) % 4)
            WHEN 0 THEN 'card'
            WHEN 1 THEN 'ach'
            WHEN 2 THEN 'wallet'
            ELSE 'cash'
        END AS payment_type,
        (CASE WHEN (n % 9)=0 THEN 3 WHEN (n % 5)=0 THEN 2 ELSE 1 END) AS quantity,
        CASE WHEN (n % 17)=0 THEN TRUE ELSE FALSE END AS returned_flag
    FROM nums
),
calc AS (
    SELECT
        customer_id_raw AS customer_id,
        CONCAT('Customer ', LPAD(customer_id_raw::STRING, 3, '0')) AS customer_name,
        CONCAT('customer', LPAD(customer_id_raw::STRING,3,'0'), '@example.com') AS email,
        region,
        product_category,
        purchase_date,
        ROUND(gross_amount * (1 - discount_pct), 2) AS amount,
        channel,
        payment_type,
        quantity,
        discount_pct,
        returned_flag
    FROM base
)
SELECT * FROM calc;
