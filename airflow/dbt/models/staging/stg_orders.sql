{{
  config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='fail'
  )
}}

{% set batch_hours = var('batch_hours', 1) %}

select
    order_id,
    customer_id,
    order_status,
    cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp,
    cast(order_approved_at as timestamp) as order_approved_at,
    cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier_date,
    cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date,
    cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date,
    current_timestamp as _loaded_at
from {{ source('olist_raw', 'olist_orders_dataset') }}
where order_id is not null

{% if is_incremental() %}
  {% set max_timestamp_query %}
    select coalesce(max(order_purchase_timestamp), timestamp('2000-01-01')) from {{ this }}
  {% endset %}

  {% set max_timestamp = run_query(max_timestamp_query) %}
  {% set max_timestamp_value = max_timestamp.columns[0].values()[0] %}

  and cast(order_purchase_timestamp as timestamp) > timestamp('{{ max_timestamp_value }}')
  and cast(order_purchase_timestamp as timestamp) <= timestamp_add(timestamp('{{ max_timestamp_value }}'), interval {{ batch_hours }} hour)
{% endif %}

order by order_purchase_timestamp
