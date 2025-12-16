
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`accepted_values_logistical_mar_e305885a42e98266ae8e826a5259205d`
    
      
    ) dbt_internal_test