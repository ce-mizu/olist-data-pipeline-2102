

select
  o.order_id,
  o.order_status,
  o.customer_id,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  sum(i.price) as order_billing,
  avg(i.freight_value) as order_freight_value
from `olist-data-pipeline`.`staging`.`stg_orders` as o
inner join `olist-data-pipeline`.`staging`.`stg_order_items` as i
  on o.order_id = i.order_id
group by
  o.order_id,
  o.order_status,
  o.customer_id,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date