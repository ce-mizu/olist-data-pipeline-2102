
  
    

    create or replace table `olist-data-pipeline`.`intermediate`.`int_seller_location`
      
    
    

    OPTIONS()
    as (
      

select
  s.seller_id,
  g.geolocation_city,
  g.geolocation_state
from `olist-data-pipeline`.`staging`.`stg_sellers` as s
inner join `olist-data-pipeline`.`staging`.`stg_geolocation` as g
  on g.geolocation_zip_code_prefix = s.seller_zip_code_prefix
    );
  