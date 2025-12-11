{{ config(materialized='view') }}

select
    geolocation_zip_code_prefix::int64 as geolocation_zip_code_prefix,
    geolocation_lat::decimal(15,2) as geolocation_lat,
    geolocation_lng::decimal(15,2) as geolocation_lng,
    geolocation_city,
    geolocation_state,
    current_timestamp as _loaded_at
from {{ source('olist_raw', 'olist_geolocation_dataset') }}