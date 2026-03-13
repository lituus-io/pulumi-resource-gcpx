{{ config(materialized='view') }}
SELECT
  order_id,
  customer_id,
  order_status,
  amount_cents,
  order_date
FROM {{ source('raw', 'raw_orders') }}