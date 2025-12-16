
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`dbt_utils_accepted_range_int_order_enriched_item_price__True__0`
    
      
    ) dbt_internal_test