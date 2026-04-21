{% snapshot dim_products %}

{{
    config(
        target_schema = 'main',
        unique_key = 'product_id',
        strategy = 'check',
        check_cols = ['category', 'price']
    )
}}

WITH products_dim AS (
    SELECT * FROM {{ ref('stg_clickstream') }}
)

SELECT 
    product_id,
    category,
    price

FROM products_dim
WHERE product_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY event_time DESC) = 1

{% endsnapshot %}