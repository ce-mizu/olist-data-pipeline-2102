
    
    

with all_values as (

    select
        abc_class_category as value_field,
        count(*) as n_records

    from `olist-data-pipeline`.`marts`.`commercial_mart`
    group by abc_class_category

)

select *
from all_values
where value_field not in (
    'A','B','C'
)


