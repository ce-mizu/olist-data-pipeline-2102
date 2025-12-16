
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`source_unique_olist_raw_olist_orders_dataset_order_id`
    
      
    ) dbt_internal_test