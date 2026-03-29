SELECT *
FROM {{ ref('stg_medicare_enrollment_annual') }}
WHERE geo_level = 'National'
  AND total_benes IS NULL