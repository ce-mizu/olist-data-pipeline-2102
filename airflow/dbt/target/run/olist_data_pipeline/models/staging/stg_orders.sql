-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `olist-data-pipeline`.`staging`.`stg_orders` as DBT_INTERNAL_DEST
        using (
        select
        * from `olist-data-pipeline`.`staging`.`stg_orders__dbt_tmp`
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id))

    
    when matched then update set
        `order_id` = DBT_INTERNAL_SOURCE.`order_id`,`customer_id` = DBT_INTERNAL_SOURCE.`customer_id`,`order_status` = DBT_INTERNAL_SOURCE.`order_status`,`order_purchase_timestamp` = DBT_INTERNAL_SOURCE.`order_purchase_timestamp`,`order_approved_at` = DBT_INTERNAL_SOURCE.`order_approved_at`,`order_delivered_carrier_date` = DBT_INTERNAL_SOURCE.`order_delivered_carrier_date`,`order_delivered_customer_date` = DBT_INTERNAL_SOURCE.`order_delivered_customer_date`,`order_estimated_delivery_date` = DBT_INTERNAL_SOURCE.`order_estimated_delivery_date`,`_loaded_at` = DBT_INTERNAL_SOURCE.`_loaded_at`
    

    when not matched then insert
        (`order_id`, `customer_id`, `order_status`, `order_purchase_timestamp`, `order_approved_at`, `order_delivered_carrier_date`, `order_delivered_customer_date`, `order_estimated_delivery_date`, `_loaded_at`)
    values
        (`order_id`, `customer_id`, `order_status`, `order_purchase_timestamp`, `order_approved_at`, `order_delivered_carrier_date`, `order_delivered_customer_date`, `order_estimated_delivery_date`, `_loaded_at`)


    