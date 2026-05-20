{{ config(materialized='view') }}

WITH raw_total_revenue AS (
    SELECT * FROM {{ source ('s3_datalake', 'raw_total_revenue') }}
)

SELECT
    window_start,
    window_end,
    user_id,
    total_revenue,
    event_date
FROM raw_total_revenue
WHERE
    user_id IS NOT NULL
