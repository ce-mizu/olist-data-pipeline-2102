
    
    

with all_values as (

    select
        transaction_size as value_field,
        count(*) as n_records

    from `olist-data-pipeline`.`marts`.`financial_mart`
    group by transaction_size

)

select *
from all_values
where value_field not in (
    'Small Transaction','Medium Transaction','Large Transaction','Premium Transaction','Unknown'
)


