{{ config(materialized='ephemeral') }}
SELECT
  order_id,
  user_id,
  {{ cents_to_dollars('amount_cents') }} AS amount_dollars,
  status,
  created_at
FROM {{ ref('stg_orders') }}
