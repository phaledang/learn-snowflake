SELECT 
            s.*,
            EXTRACT(MONTH FROM purchase_date) AS purchase_month,
            EXTRACT(YEAR  FROM purchase_date) AS purchase_year,
            EXTRACT(DOW   FROM purchase_date) AS day_of_week
        FROM sample_data s
        ORDER BY purchase_date