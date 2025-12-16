
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`accepted_values_logistical_mar_79c293c72593fea08d364f3ab09fb888`
    
      
    ) dbt_internal_test