
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`accepted_values_financial_mart_23247625d68b7f1e3ef1ff1d72bf2a6b`
    
      
    ) dbt_internal_test