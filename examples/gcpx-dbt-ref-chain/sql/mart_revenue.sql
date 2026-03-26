{{ config(materialized='table') }}
SELECT
  region,
  DATE_TRUNC(created_at, MONTH) AS month,
  COUNT(DISTINCT user_id) AS unique_buyers,
  SUM(amount_dollars) AS total_revenue,
  {{ safe_divide('SUM(amount_dollars)', 'COUNT(DISTINCT user_id)') }} AS revenue_per_buyer
FROM {{ ref('int_joined') }}
GROUP BY region, DATE_TRUNC(created_at, MONTH)
