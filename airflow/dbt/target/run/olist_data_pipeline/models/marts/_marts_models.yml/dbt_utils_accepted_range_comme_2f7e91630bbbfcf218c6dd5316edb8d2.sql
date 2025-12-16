
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`dbt_utils_accepted_range_comme_2f7e91630bbbfcf218c6dd5316edb8d2`
    
      
    ) dbt_internal_test