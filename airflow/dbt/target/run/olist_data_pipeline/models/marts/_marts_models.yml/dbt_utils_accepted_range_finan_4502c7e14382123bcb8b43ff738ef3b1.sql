
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`dbt_utils_accepted_range_finan_4502c7e14382123bcb8b43ff738ef3b1`
    
      
    ) dbt_internal_test