{{ config(materialized='view') }}
SELECT
  user_id,
  email,
  region,
  created_at
FROM {{ source('raw', 'users') }}
WHERE email IS NOT NULL
