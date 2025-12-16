
    
    

with child as (
    select customer_id as from_field
    from `olist-data-pipeline`.`olist_data_pipeline`.`olist_orders_dataset`
    where customer_id is not null
),

parent as (
    select customer_id as to_field
    from `olist-data-pipeline`.`olist_data_pipeline`.`olist_customers_dataset`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


