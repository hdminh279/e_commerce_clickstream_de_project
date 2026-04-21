{{ config(materialized='table') }}

WITH dim_user AS (
    SELECT * FROM {{ ref('dim_user') }}
),

fact_events AS (
    SELECT * FROM {{ ref('fact_events') }}
)

SELECT 
    du.utm_source,
    COUNT(DISTINCT fe.session_id) AS total_sessions,
    COUNT(DISTINCT CASE WHEN fe.event_name = 'view_item' THEN fe.session_id END) AS sessions_with_views,
    COUNT(DISTINCT CASE WHEN fe.event_name = 'add_to_cart' THEN fe.session_id END) AS sessions_with_cart,
    COUNT(DISTINCT CASE WHEN fe.event_name = 'purchase' THEN fe.session_id END) AS sessions_with_purchase
FROM 
    fact_events fe
JOIN 
    dim_user du
ON 
    fe.client_id = du.client_id
GROUP BY
    du.utm_source

    