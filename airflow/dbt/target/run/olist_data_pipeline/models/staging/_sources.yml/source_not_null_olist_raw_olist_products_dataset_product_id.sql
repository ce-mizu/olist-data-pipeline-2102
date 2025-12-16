
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`source_not_null_olist_raw_olist_products_dataset_product_id`
    
      
    ) dbt_internal_test