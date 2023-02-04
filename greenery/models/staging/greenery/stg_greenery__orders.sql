WITH source AS (
    SELECT * FROM {{ source('greenery', 'orders') }}
)

, final AS (
    SELECT 
       order_id
        , user_id
        , promo_id
        , address_id
        , CAST(created_at AS TIMESTAMP) AS created_at
        , order_cost
        , shipping_cost
        , order_total
        , tracking_id
        , shipping_service
        , estimated_delivery_at
        , delivered_at
        , status
    FROM source
)

SELECT * 
FROM final