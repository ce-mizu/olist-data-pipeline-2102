
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`accepted_values_int_payment_en_55142f78c01fcc592acf37eb903cd950`
    
      
    ) dbt_internal_test