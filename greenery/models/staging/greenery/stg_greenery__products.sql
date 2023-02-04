WITH source AS (
    SELECT * FROM {{ source('greenery', 'products') }}
)

, final AS (
    SELECT 
       product_id
        , name
        , price
        , inventory
    FROM source
)

SELECT * 
FROM final