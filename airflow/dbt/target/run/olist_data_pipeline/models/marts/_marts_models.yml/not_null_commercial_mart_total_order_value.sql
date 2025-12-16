
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`not_null_commercial_mart_total_order_value`
    
      
    ) dbt_internal_test