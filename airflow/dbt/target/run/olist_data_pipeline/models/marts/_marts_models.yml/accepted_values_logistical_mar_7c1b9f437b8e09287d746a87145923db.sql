
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`accepted_values_logistical_mar_7c1b9f437b8e09287d746a87145923db`
    
      
    ) dbt_internal_test