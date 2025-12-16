
    
    

with all_values as (

    select
        delivery_performance as value_field,
        count(*) as n_records

    from `olist-data-pipeline`.`marts`.`logistical_mart`
    group by delivery_performance

)

select *
from all_values
where value_field not in (
    'Fast Delivery','Standard Delivery','Slow Delivery','Pending Delivery'
)


