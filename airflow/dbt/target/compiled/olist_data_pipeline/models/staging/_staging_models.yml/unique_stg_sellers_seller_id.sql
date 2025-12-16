
    
    

with dbt_test__target as (

  select seller_id as unique_field
  from `olist-data-pipeline`.`staging`.`stg_sellers`
  where seller_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


