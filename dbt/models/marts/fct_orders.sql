{{ config(materialized='table') }}

select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    delivery_days,
    total_items,
    total_order_value,
    total_freight_value,
    avg_item_price,
    delivery_performance,
    case
        when total_order_value >= 500 then 'High Value'
        when total_order_value >= 100 then 'Medium Value'
        else 'Low Value'
    end as order_value_category,
    case
        when delivery_days <= 7 then 'Fast'
        when delivery_days <= 14 then 'Normal'
        else 'Slow'
    end as delivery_speed_category
from {{ ref('int_order_enriched') }}
