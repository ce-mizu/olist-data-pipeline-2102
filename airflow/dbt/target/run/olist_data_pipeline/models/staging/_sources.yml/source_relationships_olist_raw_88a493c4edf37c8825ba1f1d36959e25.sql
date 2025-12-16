
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`source_relationships_olist_raw_88a493c4edf37c8825ba1f1d36959e25`
    
      
    ) dbt_internal_test