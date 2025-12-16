
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`dbt_utils_accepted_range_int_p_14f4919d92df59b0712110b384497da8`
    
      
    ) dbt_internal_test