
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`not_null_stg_product_category_translation_product_category_name`
    
      
    ) dbt_internal_test