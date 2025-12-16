
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`accepted_values_commercial_mar_0d27b69766f97d80c04cc51d92031142`
    
      
    ) dbt_internal_test