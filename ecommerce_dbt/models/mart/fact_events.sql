{{ config(materialized='table') }}

WITH fact_events AS (
    SELECT * FROM {{ ref('stg_clickstream') }}
)

SELECT
    session_id,
    user_id,
    client_id,
    event_id,
    event_time,
    event_name

FROM fact_events
WHERE
    event_id IS NOT NULL
AND
    session_id IS NOT NULL

