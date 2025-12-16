
  
    

    create or replace table `olist-data-pipeline`.`intermediate`.`int_order_enriched`
      
    
    

    OPTIONS()
    as (
      

select
  o.order_id,
  o.order_status,
  o.customer_id,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  i.freight_value as order_freight_value,
  i.product_id,
  i.seller_id,
  i.price as item_price,
  p.product_category_name as item_category
from `olist-data-pipeline`.`staging`.`stg_orders` o
join `olist-data-pipeline`.`staging`.`stg_order_items` i
  on o.order_id = i.order_id
left join `olist-data-pipeline`.`staging`.`stg_products` p
  on i.product_id = p.product_id
    );
  