SELECT
  order_id,
  user_id,
  amount_cents,
  status,
  created_at
FROM {{ source('raw_src', 'orders') }}
WHERE status != 'cancelled'
