
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`accepted_values_financial_mart_b2e73f9561a440cd58945902142d1232`
    
      
    ) dbt_internal_test