{{ config(materialized='table') }}

WITH user_dim AS (
    SELECT * FROM {{ ref('stg_clickstream') }}
)

SELECT 
    client_id,
    user_id,
    ip_address,
    device_category,
    os_browser,
    utm_source
FROM user_dim
WHERE user_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY event_time DESC) = 1