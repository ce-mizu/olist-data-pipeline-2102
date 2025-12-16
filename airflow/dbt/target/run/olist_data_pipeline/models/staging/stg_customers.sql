
  
    

    create or replace table `olist-data-pipeline`.`staging`.`stg_customers`
      
    
    

    OPTIONS()
    as (
      

select
    customer_id,
    customer_unique_id,
    cast(customer_zip_code_prefix as int64) as customer_zip_code_prefix,
    customer_city,
    customer_state,
    current_timestamp as _loaded_at
from `olist-data-pipeline`.`olist_data_pipeline`.`olist_customers_dataset`
where customer_id is not null
    );
  