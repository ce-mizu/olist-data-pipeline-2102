
    
    

with dbt_test__target as (

  select customer_id as unique_field
  from `olist-data-pipeline`.`olist_data_pipeline`.`olist_customers_dataset`
  where customer_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


