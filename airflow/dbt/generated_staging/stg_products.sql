{{ config(materialized='view') }}

select
    product_id,
    product_category_name,
    product_name_lenght::int64 as product_name_lenght,
    product_description_lenght::int64 as product_description_lenght,
    product_photos_qty::int64 as product_photos_qty,
    product_weight_g::int64 as product_weight_g,
    product_length_cm::int64 as product_length_cm,
    product_height_cm::int64 as product_height_cm,
    product_width_cm::int64 as product_width_cm,
    current_timestamp as _loaded_at
from {{ source('olist_raw', 'olist_products_dataset') }}
where product_id is not null