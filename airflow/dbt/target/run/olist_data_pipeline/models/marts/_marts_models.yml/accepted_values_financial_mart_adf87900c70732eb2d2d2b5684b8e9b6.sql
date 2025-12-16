
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`accepted_values_financial_mart_adf87900c70732eb2d2d2b5684b8e9b6`
    
      
    ) dbt_internal_test