{{ config(materialized='incremental', unique_key='event_id') }}
SELECT
  event_id,
  user_id,
  event_name,
  event_ts
FROM UNNEST([
  STRUCT(1 AS event_id, 'u1' AS user_id, 'click' AS event_name, CURRENT_TIMESTAMP() AS event_ts),
  STRUCT(2, 'u2', 'view', CURRENT_TIMESTAMP())
])
