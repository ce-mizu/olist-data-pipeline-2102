
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`not_null_stg_geolocation_geolocation_zip_code_prefix`
    
      
    ) dbt_internal_test