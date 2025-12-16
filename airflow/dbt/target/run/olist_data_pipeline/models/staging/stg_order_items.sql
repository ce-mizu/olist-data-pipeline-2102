
  
    

    create or replace table `olist-data-pipeline`.`staging`.`stg_order_items`
      
    
    

    OPTIONS()
    as (
      

select
    order_id,
    cast(order_item_id as int64) as order_item_id,
    product_id,
    seller_id,
    cast(shipping_limit_date as timestamp) as shipping_limit_date,
    cast(price as numeric) as price,
    cast(freight_value as numeric) as freight_value,
    current_timestamp as _loaded_at
from `olist-data-pipeline`.`olist_data_pipeline`.`olist_order_items_dataset`
where order_id is not null and price is not null
    );
  