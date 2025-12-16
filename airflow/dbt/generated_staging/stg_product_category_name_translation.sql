{{ config(materialized='view') }}

select
    string_field_0,
    string_field_1,
    current_timestamp as _loaded_at
from {{ source('olist_raw', 'product_category_name_translation') }}