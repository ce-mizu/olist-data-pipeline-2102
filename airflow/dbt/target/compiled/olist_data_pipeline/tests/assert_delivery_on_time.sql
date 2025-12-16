-- Test to check if order delivery is within estimated date
select
    order_id,
    order_delivered_customer_date,
    order_estimated_delivery_date
from `olist-data-pipeline`.`staging`.`stg_orders`
where order_delivered_customer_date > order_estimated_delivery_date
  and order_delivered_customer_date is not null
  and order_estimated_delivery_date is not null