
    
    

with all_values as (

    select
        carrier_leg_performance as value_field,
        count(*) as n_records

    from `olist-data-pipeline`.`marts`.`logistical_mart`
    group by carrier_leg_performance

)

select *
from all_values
where value_field not in (
    'Fast Transit','Standard Transit','Slow Transit','Unknown Transit'
)


