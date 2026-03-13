{{ config(materialized='view') }}
SELECT
  order_id,
  user_id,
  amount_cents,
  status,
  created_at
FROM {{ source('raw', 'orders') }}
WHERE status != 'cancelled'
