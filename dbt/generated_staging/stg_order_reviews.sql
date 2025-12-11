{{ config(materialized='view') }}

select
    review_id,
    order_id,
    review_score::int64 as review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    current_timestamp as _loaded_at
from {{ source('olist_raw', 'olist_order_reviews_dataset') }}
where review_id is not null