SELECT
  u.region,
  DATE_TRUNC(o.created_at, MONTH) AS month,
  COUNT(DISTINCT o.user_id) AS unique_buyers,
  SUM({{ cents_to_dollars('o.amount_cents') }}) AS total_revenue
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_users') }} u ON o.user_id = u.user_id
GROUP BY u.region, DATE_TRUNC(o.created_at, MONTH)
