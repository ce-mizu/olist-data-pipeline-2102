
    
    

with all_values as (

    select
        delivery_status as value_field,
        count(*) as n_records

    from `olist-data-pipeline`.`marts`.`logistical_mart`
    group by delivery_status

)

select *
from all_values
where value_field not in (
    'Delivered','In Transit','Processing','Pending'
)


