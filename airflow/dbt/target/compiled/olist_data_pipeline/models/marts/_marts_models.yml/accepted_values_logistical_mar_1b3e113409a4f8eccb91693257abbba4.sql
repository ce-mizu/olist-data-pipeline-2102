
    
    

with all_values as (

    select
        seller_region as value_field,
        count(*) as n_records

    from `olist-data-pipeline`.`marts`.`logistical_mart`
    group by seller_region

)

select *
from all_values
where value_field not in (
    'Sudeste','Sul','Centro-Oeste','Nordeste','Norte','Outros'
)


