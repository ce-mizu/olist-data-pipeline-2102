{{ config(materialized='view') }}

select
    order_id,
    order_item_id::int64 as order_item_id,
    product_id,
    seller_id,
    shipping_limit_date::timestamp as shipping_limit_date,
    price::decimal(15,2) as price,
    freight_value::decimal(15,2) as freight_value,
    current_timestamp as _loaded_at
from {{ source('olist_raw', 'olist_order_items_dataset') }}
where order_id is not null