
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`dbt_utils_accepted_range_finan_43cf9485c31f535adb109f4d4ff5c21d`
    
      
    ) dbt_internal_test