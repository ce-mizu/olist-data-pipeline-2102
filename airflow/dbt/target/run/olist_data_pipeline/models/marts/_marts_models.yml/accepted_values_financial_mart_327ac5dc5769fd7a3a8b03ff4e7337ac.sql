
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`accepted_values_financial_mart_327ac5dc5769fd7a3a8b03ff4e7337ac`
    
      
    ) dbt_internal_test