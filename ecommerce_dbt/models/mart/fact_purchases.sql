{{ config(
    materialized='incremental',
    unique_key = 'event_id'
) }}

WITH fact_purchases AS (
    SELECT * FROM {{ ref('stg_clickstream') }}
    WHERE 1 = 1
    {% if is_incremental() %}
        AND event_time > (SELECT MAX(event_time) FROM {{ this }})
    {% endif %}
)

SELECT
    session_id,
    event_id,
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
