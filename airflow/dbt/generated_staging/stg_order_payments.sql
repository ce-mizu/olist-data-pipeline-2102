{{ config(materialized='view') }}

select
    order_id,
    payment_sequential::int64 as payment_sequential,
    payment_type,
    payment_installments::int64 as payment_installments,
    payment_value::decimal(15,2) as payment_value,
    current_timestamp as _loaded_at
from {{ source('olist_raw', 'olist_order_payments_dataset') }}
where order_id is not null