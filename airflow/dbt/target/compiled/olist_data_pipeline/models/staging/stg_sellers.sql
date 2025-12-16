

select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    current_timestamp as _loaded_at
from `olist-data-pipeline`.`olist_data_pipeline`.`olist_sellers_dataset`
where seller_id is not null