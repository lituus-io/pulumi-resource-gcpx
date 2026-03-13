{{ config(materialized='view') }}
SELECT DISTINCT
  user_id,
  region
FROM {{ source('raw', 'raw_events') }}
