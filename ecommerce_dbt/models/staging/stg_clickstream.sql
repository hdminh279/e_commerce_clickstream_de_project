WITH raw_clickstream AS (
    SELECT * FROM {{ source ('s3_datalake', 'raw_clickstream')}}
)

SELECT 
    -- User Session
    session_id ,
    client_id ,
    user_id ,
    ip_address ,
    device_category ,
    os_browser ,
    utm_source ,

    -- Event Click
    event_id ,
    ts AS "event_time",
    event_name ,
    page_url ,

    -- Product
    NULLIF(product_id, '') AS product_id,
    NULLIF(category, '') AS category,
    CAST(price AS FLOAT) AS price,
    CAST(quantity AS INTEGER) AS quantity,
    
    cart_total,
    NULLIF(transaction_id, '') AS transaction_id,
    event_date 
FROM raw_clickstream
WHERE
    session_id IS NOT NULL
    