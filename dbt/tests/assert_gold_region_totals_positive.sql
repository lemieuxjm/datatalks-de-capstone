SELECT *
FROM {{ ref('mart_medicare_enrollment_by_region') }}
WHERE total_benes <= 0
   OR original_medicare_benes <= 0
   OR ma_other_benes <= 0