{{ config(materialized='view') }}
SELECT
  event_ts AS order_ts,
  user_id,
  amount_cents,
  region
FROM {{ source('raw', 'raw_events') }}
WHERE event_type = 'purchase'
