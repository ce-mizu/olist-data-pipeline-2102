



select
    order_id,
    customer_id,
    order_status,
    cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp,
    cast(order_approved_at as timestamp) as order_approved_at,
    cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier_date,
    cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date,
    cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date,
    current_timestamp as _loaded_at
from `olist-data-pipeline`.`olist_data_pipeline`.`olist_orders_dataset`
where order_id is not null


  

  
  

  and cast(order_purchase_timestamp as timestamp) > timestamp('2018-10-17 17:30:18+00:00')
  and cast(order_purchase_timestamp as timestamp) <= timestamp_add(timestamp('2018-10-17 17:30:18+00:00'), interval 1 hour)


order by order_purchase_timestamp