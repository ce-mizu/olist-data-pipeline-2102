
    
    

with all_values as (

    select
        supplier_leg_performance as value_field,
        count(*) as n_records

    from `olist-data-pipeline`.`marts`.`logistical_mart`
    group by supplier_leg_performance

)

select *
from all_values
where value_field not in (
    'Fast Pickup','Standard Pickup','Slow Pickup','Unknown Pickup'
)


