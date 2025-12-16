{{ config(materialized='view') }}

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix::int64 as customer_zip_code_prefix,
    customer_city,
    customer_state,
    current_timestamp as _loaded_at
from {{ source('olist_raw', 'olist_customers_dataset') }}
where customer_id is not null