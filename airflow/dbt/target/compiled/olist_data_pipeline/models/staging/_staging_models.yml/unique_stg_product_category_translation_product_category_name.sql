
    
    

with dbt_test__target as (

  select product_category_name as unique_field
  from `olist-data-pipeline`.`staging`.`stg_product_category_translation`
  where product_category_name is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


