
  
    

    create or replace table `olist-data-pipeline`.`intermediate`.`int_payment_enriched`
      
    
    

    OPTIONS()
    as (
      

SELECT
p.*,
o.order_approved_at,
CASE
  WHEN p.payment_type = 'boleto' THEN DATE_ADD(o.order_approved_at, INTERVAL 2 DAY)
  WHEN p.payment_type = 'credit_card' THEN DATE_ADD(o.order_approved_at, INTERVAL 30 DAY)
  WHEN p.payment_type = 'debit_card' THEN DATE_ADD(o.order_approved_at, INTERVAL 5 DAY)
  WHEN p.payment_type = 'voucher' THEN DATE_ADD(o.order_approved_at, INTERVAL 5 DAY)
  ELSE NULL
END AS receipt_date
FROM `olist-data-pipeline`.`staging`.`stg_order_payments` p
INNER JOIN `olist-data-pipeline`.`staging`.`stg_orders` o ON o.order_id = p.order_id
WHERE p.payment_type <> 'not_defined' AND o.order_approved_at IS NOT NULL
    );
  