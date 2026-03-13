{{ config(
    materialized='incremental',
    unique_key='order_id',
    partition_by={'field': 'order_date', 'data_type': 'date'},
    cluster_by=['customer_id'],
    require_partition_filter=false,
    labels={'env': 'test', 'managed_by': 'gcpx'},
    description='Daily fact table of customer orders'
) }}
SELECT
  o.order_id,
  o.customer_id,
  o.order_status,
  {{ cents_to_dollars('o.amount_cents') }} AS amount_dollars,
  o.order_date
FROM {{ ref('stg_orders') }} o