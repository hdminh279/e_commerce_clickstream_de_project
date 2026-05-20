{{ config(
    materialized = 'incremental',
    unique_key = 'event_id'
) }}

WITH fact_events AS (
    SELECT * FROM {{ ref('stg_clickstream') }}
    WHERE 1 = 1
    {% if is_incremental() %}
        AND event_time > (SELECT MAX(event_time) FROM {{ this }})
    {% endif %}
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
