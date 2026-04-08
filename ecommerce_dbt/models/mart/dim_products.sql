{{ config(materialized='table') }}

WITH products_dim AS (
    SELECT * FROM {{ ref('stg_clickstream') }}
)

SELECT 
    product_id,
    category,
    event_time AS latest_time,
    price

FROM products_dim
WHERE product_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY event_time DESC) = 1