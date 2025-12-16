
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`dbt_utils_accepted_range_comme_cac4c31532a981839175f1d9900e2bb1`
    
      
    ) dbt_internal_test