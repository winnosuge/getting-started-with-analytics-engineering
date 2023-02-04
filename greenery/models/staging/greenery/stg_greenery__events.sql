WITH source AS (
    SELECT * FROM {{ source('greenery', 'events') }}
)

, final AS (
    SELECT 
       event_id
        , session_id
        , user_id
        , page_url
        , created_at
        , event_type
        , order_id
        , product_id
    FROM source
)

SELECT * 
FROM final