
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `olist-data-pipeline`.`test_failures`.`dbt_utils_accepted_range_int_o_042057811c85e9ffea343a8e381aaa4e`
    
      
    ) dbt_internal_test