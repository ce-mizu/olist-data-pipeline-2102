{{ config(materialized='table') }}

select
    order_id,
    cast(payment_sequential as int64) as payment_sequential,
    payment_type,
    cast(payment_installments as int64) as payment_installments,
    cast(payment_value as numeric) as payment_value,
    current_timestamp as _loaded_at
from {{ source('olist_raw', 'olist_order_payments_dataset') }}
where order_id is not null
