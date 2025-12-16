
  
    

    create or replace table `olist-data-pipeline`.`staging`.`stg_geolocation`
      
    
    

    OPTIONS()
    as (
      

select
    cast(geolocation_zip_code_prefix as int64) as geolocation_zip_code_prefix,
    cast(geolocation_lat as float64) as geolocation_lat,
    cast(geolocation_lng as float64) as geolocation_lng,
    geolocation_city,
    geolocation_state,
    current_timestamp as _loaded_at
from `olist-data-pipeline`.`olist_data_pipeline`.`olist_geolocation_dataset`
where geolocation_zip_code_prefix is not null
    );
  