{{ config(materialized='table') }}

select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    current_timestamp as _loaded_at
from {{ source('olist_raw', 'olist_sellers_dataset') }}
where seller_id is not null
