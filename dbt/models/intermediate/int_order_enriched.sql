{{ config(materialized='view') }}

with order_details as (
  select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    {{ calculate_days_between('order_purchase_timestamp', 'order_delivered_customer_date') }} as delivery_days,
    {{ calculate_days_between('order_delivered_customer_date', 'order_estimated_delivery_date') }} as days_after_estimated
  from {{ ref('stg_orders') }} o
),

order_items_agg as (
  select
    order_id,
    count(*) as total_items,
    sum(price) as total_order_value,
    sum(freight_value) as total_freight_value,
    avg(price) as avg_item_price
  from {{ ref('stg_order_items') }}
  group by order_id
)

select
  od.*,
  oia.total_items,
  oia.total_order_value,
  oia.total_freight_value,
  oia.avg_item_price,
  case
    when od.days_after_estimated <= 0 then 'On Time'
    when od.days_after_estimated between 1 and 7 then 'Slightly Late'
    when od.days_after_estimated > 7 then 'Very Late'
    else 'Unknown'
  end as delivery_performance
from order_details od
left join order_items_agg oia on od.order_id = oia.order_id
