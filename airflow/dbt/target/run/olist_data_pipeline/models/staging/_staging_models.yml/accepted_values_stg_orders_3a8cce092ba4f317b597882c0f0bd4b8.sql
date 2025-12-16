
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`accepted_values_stg_orders_3a8cce092ba4f317b597882c0f0bd4b8`
    
      
    ) dbt_internal_test