{% snapshot dim_user %}

{{
    config(
        target_schema = 'main',
        unique_key = 'user_id',
        strategy = 'check',
        check_cols = 'all'
    )
}}

SELECT 
    client_id,
    user_id,
    ip_address,
    device_category,
    os_browser,
    utm_source
FROM {{ ref('stg_clickstream') }}
WHERE user_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY event_time DESC) = 1

{% endsnapshot %}