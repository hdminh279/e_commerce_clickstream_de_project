{{ config(materialized='table') }}

WITH fact_purchases AS (
    SELECT * FROM {{ ref('stg_clickstream') }}
)

SELECT
    session_id,
    user_id,
    transaction_id,
    product_id,
    quantity,
    cart_total,
    event_time,
    event_name
FROM fact_purchases
WHERE
    event_name = 'purchase'
