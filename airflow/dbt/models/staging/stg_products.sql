{{ config(materialized='table') }}

select
    product_id,
    product_category_name,
    product_name_lenght,  -- Note: mantendo typo original do dataset
    product_description_lenght,  -- Note: mantendo typo original do dataset
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    current_timestamp as _loaded_at
from {{ source('olist_raw', 'olist_products_dataset') }}
where product_id is not null
