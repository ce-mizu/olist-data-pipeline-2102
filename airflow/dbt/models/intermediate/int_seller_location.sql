{{ config(materialized='table') }}

select
  s.seller_id,
  g.geolocation_city,
  g.geolocation_state
from {{ ref('stg_sellers') }} as s
inner join {{ ref('stg_geolocation') }} as g
  on g.geolocation_zip_code_prefix = s.seller_zip_code_prefix
