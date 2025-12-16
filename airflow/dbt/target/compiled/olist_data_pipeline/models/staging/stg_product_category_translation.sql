

select
    string_field_0 as product_category_name,
    string_field_1 as product_category_name_english,
    current_timestamp as _loaded_at
from `olist-data-pipeline`.`olist_data_pipeline`.`product_category_name_translation`
where string_field_0 is not null