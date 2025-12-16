
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`accepted_values_logistical_mar_cebec7c88dfdc52f1b971f9b0b39187c`
    
      
    ) dbt_internal_test