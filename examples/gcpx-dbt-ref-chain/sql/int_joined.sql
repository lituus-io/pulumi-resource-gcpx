{{ config(materialized='ephemeral') }}
SELECT
  e.order_id,
  e.user_id,
  u.region,
  e.amount_dollars,
  e.created_at
FROM {{ ref('int_enriched') }} e
JOIN {{ ref('stg_users') }} u ON e.user_id = u.user_id
