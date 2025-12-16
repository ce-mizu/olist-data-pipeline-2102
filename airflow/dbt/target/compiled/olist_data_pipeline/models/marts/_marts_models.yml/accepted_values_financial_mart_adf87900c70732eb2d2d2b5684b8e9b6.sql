
    
    

with all_values as (

    select
        installment_category as value_field,
        count(*) as n_records

    from `olist-data-pipeline`.`marts`.`financial_mart`
    group by installment_category

)

select *
from all_values
where value_field not in (
    'Single Payment','Short Term Installment','Medium Term Installment','Long Term Installment','Unknown Installment'
)


