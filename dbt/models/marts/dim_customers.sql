{{ config(materialized='table') }}

select
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    count(distinct o.order_id) as total_orders,
    sum(o.total_order_value) as lifetime_value,
    avg(o.total_order_value) as avg_order_value,
    min(o.order_purchase_timestamp) as first_order_date,
    max(o.order_purchase_timestamp) as last_order_date,
    avg(o.delivery_days) as avg_delivery_days,
    sum(case when o.delivery_performance = 'On Time' then 1 else 0 end) as on_time_deliveries,
    round(
      sum(case when o.delivery_performance = 'On Time' then 1 else 0 end) * 100.0 /
      count(distinct o.order_id), 2
    ) as on_time_delivery_rate
from {{ ref('stg_customers') }} c
left join {{ ref('int_order_enriched') }} o on c.customer_id = o.customer_id
group by
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state
