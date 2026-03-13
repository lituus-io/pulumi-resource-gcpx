SELECT
  user_id,
  email,
  region,
  created_at
FROM {{ source('raw_src', 'users') }}
WHERE email IS NOT NULL
