{{ config(materialized='table') }}
SELECT
  c.customer_id,
  c.full_name,
  c.email,
  c.customer_created_at
FROM {{ ref('stg_customers') }} c