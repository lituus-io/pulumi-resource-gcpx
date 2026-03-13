{{ config(materialized='view') }}
SELECT
  customer_id,
  full_name,
  email,
  created_at AS customer_created_at
FROM {{ source('raw', 'raw_customers') }}