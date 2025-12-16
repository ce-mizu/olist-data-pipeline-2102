
    
    

with all_values as (

    select
        payment_category as value_field,
        count(*) as n_records

    from `olist-data-pipeline`.`marts`.`financial_mart`
    group by payment_category

)

select *
from all_values
where value_field not in (
    'Credit Payment','Debit Payment','Bank Slip','Voucher Payment','Other Payment'
)


