
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`accepted_values_logistical_mar_428d2e181a06941796a710faef2ea2fe`
    
      
    ) dbt_internal_test