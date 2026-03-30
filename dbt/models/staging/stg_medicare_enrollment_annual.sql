{{ config(
    materialized='view',
    schema='capstone_silver'
) }}

{% set clean_float = 'REPLACE({col}, \'*\', \'0\')' %}

with source as (
    select * from {{ source('capstone_raw', 'medicare_monthly_enrollment_raw') }}
),

renamed as (
    select
        -- Time dimension
        CAST(YEAR AS INT64)                                                                    as enrollment_year,

        -- Geography
        BENE_GEO_LVL                                                                           as geo_level,
        BENE_STATE_ABRVTN                                                                      as state_abbr,
        BENE_STATE_DESC                                                                        as state_name,
        BENE_COUNTY_DESC                                                                       as county_name,
        BENE_FIPS_CD                                                                           as fips_code,

        -- Total enrollment
        SAFE_CAST(NULLIF(REPLACE(TOT_BENES, '*', '0'), '') AS FLOAT64)                        as total_benes,
        SAFE_CAST(NULLIF(REPLACE(ORGNL_MDCR_BENES, '*', '0'), '') AS FLOAT64)                 as original_medicare_benes,
        SAFE_CAST(NULLIF(REPLACE(MA_AND_OTH_BENES, '*', '0'), '') AS FLOAT64)                 as ma_other_benes,

        -- Age & disability
        SAFE_CAST(NULLIF(REPLACE(AGED_TOT_BENES, '*', '0'), '') AS FLOAT64)                   as aged_total_benes,
        SAFE_CAST(NULLIF(REPLACE(AGED_ESRD_BENES, '*', '0'), '') AS FLOAT64)                  as aged_esrd_benes,
        SAFE_CAST(NULLIF(REPLACE(AGED_NO_ESRD_BENES, '*', '0'), '') AS FLOAT64)               as aged_no_esrd_benes,
        SAFE_CAST(NULLIF(REPLACE(DSBLD_TOT_BENES, '*', '0'), '') AS FLOAT64)                  as disabled_total_benes,
        SAFE_CAST(NULLIF(REPLACE(DSBLD_ESRD_AND_ESRD_ONLY_BENES, '*', '0'), '') AS FLOAT64)   as disabled_esrd_benes,
        SAFE_CAST(NULLIF(REPLACE(DSBLD_NO_ESRD_BENES, '*', '0'), '') AS FLOAT64)              as disabled_no_esrd_benes,

        -- Gender
        SAFE_CAST(NULLIF(REPLACE(MALE_TOT_BENES, '*', '0'), '') AS FLOAT64)                   as male_benes,
        SAFE_CAST(NULLIF(REPLACE(FEMALE_TOT_BENES, '*', '0'), '') AS FLOAT64)                 as female_benes,

        -- Race & ethnicity
        SAFE_CAST(NULLIF(REPLACE(WHITE_TOT_BENES, '*', '0'), '') AS FLOAT64)                  as white_benes,
        SAFE_CAST(NULLIF(REPLACE(BLACK_TOT_BENES, '*', '0'), '') AS FLOAT64)                  as black_benes,
        SAFE_CAST(NULLIF(REPLACE(API_TOT_BENES, '*', '0'), '') AS FLOAT64)                    as api_benes,
        SAFE_CAST(NULLIF(REPLACE(HSPNC_TOT_BENES, '*', '0'), '') AS FLOAT64)                  as hispanic_benes,
        SAFE_CAST(NULLIF(REPLACE(NATIND_TOT_BENES, '*', '0'), '') AS FLOAT64)                 as native_indian_benes,
        SAFE_CAST(NULLIF(REPLACE(OTHR_TOT_BENES, '*', '0'), '') AS FLOAT64)                   as other_race_benes,

        -- Age bands
        SAFE_CAST(NULLIF(REPLACE(AGE_LT_25_BENES, '*', '0'), '') AS FLOAT64)                  as age_lt_25_benes,
        SAFE_CAST(NULLIF(REPLACE(AGE_25_TO_44_BENES, '*', '0'), '') AS FLOAT64)               as age_25_to_44_benes,
        SAFE_CAST(NULLIF(REPLACE(AGE_45_TO_64_BENES, '*', '0'), '') AS FLOAT64)               as age_45_to_64_benes,
        SAFE_CAST(NULLIF(REPLACE(AGE_65_TO_69_BENES, '*', '0'), '') AS FLOAT64)               as age_65_to_69_benes,
        SAFE_CAST(NULLIF(REPLACE(AGE_70_TO_74_BENES, '*', '0'), '') AS FLOAT64)               as age_70_to_74_benes,
        SAFE_CAST(NULLIF(REPLACE(AGE_75_TO_79_BENES, '*', '0'), '') AS FLOAT64)               as age_75_to_79_benes,
        SAFE_CAST(NULLIF(REPLACE(AGE_80_TO_84_BENES, '*', '0'), '') AS FLOAT64)               as age_80_to_84_benes,
        SAFE_CAST(NULLIF(REPLACE(AGE_85_TO_89_BENES, '*', '0'), '') AS FLOAT64)               as age_85_to_89_benes,
        SAFE_CAST(NULLIF(REPLACE(AGE_90_TO_94_BENES, '*', '0'), '') AS FLOAT64)               as age_90_to_94_benes,
        SAFE_CAST(NULLIF(REPLACE(AGE_GT_94_BENES, '*', '0'), '') AS FLOAT64)                  as age_gt_94_benes,

        -- Dual eligibility
        SAFE_CAST(NULLIF(REPLACE(DUAL_TOT_BENES, '*', '0'), '') AS FLOAT64)                   as dual_total_benes,
        SAFE_CAST(NULLIF(REPLACE(FULL_DUAL_TOT_BENES, '*', '0'), '') AS FLOAT64)              as full_dual_benes,
        SAFE_CAST(NULLIF(REPLACE(PART_DUAL_TOT_BENES, '*', '0'), '') AS FLOAT64)              as partial_dual_benes,
        SAFE_CAST(NULLIF(REPLACE(NODUAL_TOT_BENES, '*', '0'), '') AS FLOAT64)                 as no_dual_benes,
        SAFE_CAST(NULLIF(REPLACE(QMB_ONLY_BENES, '*', '0'), '') AS FLOAT64)                   as qmb_only_benes,
        SAFE_CAST(NULLIF(REPLACE(QMB_PLUS_BENES, '*', '0'), '') AS FLOAT64)                   as qmb_plus_benes,
        SAFE_CAST(NULLIF(REPLACE(SLMB_ONLY_BENES, '*', '0'), '') AS FLOAT64)                  as slmb_only_benes,
        SAFE_CAST(NULLIF(REPLACE(SLMB_PLUS_BENES, '*', '0'), '') AS FLOAT64)                  as slmb_plus_benes,
        SAFE_CAST(NULLIF(REPLACE(QDWI_QI_BENES, '*', '0'), '') AS FLOAT64)                    as qdwi_qi_benes,
        SAFE_CAST(NULLIF(REPLACE(OTHR_FULL_DUAL_MDCD_BENES, '*', '0'), '') AS FLOAT64)        as other_full_dual_benes,

        -- Parts A & B
        SAFE_CAST(NULLIF(REPLACE(A_B_TOT_BENES, '*', '0'), '') AS FLOAT64)                    as part_ab_total_benes,
        SAFE_CAST(NULLIF(REPLACE(A_B_ORGNL_MDCR_BENES, '*', '0'), '') AS FLOAT64)             as part_ab_original_medicare_benes,
        SAFE_CAST(NULLIF(REPLACE(A_B_MA_AND_OTH_BENES, '*', '0'), '') AS FLOAT64)             as part_ab_ma_other_benes,
        SAFE_CAST(NULLIF(REPLACE(A_TOT_BENES, '*', '0'), '') AS FLOAT64)                      as part_a_total_benes,
        SAFE_CAST(NULLIF(REPLACE(A_ORGNL_MDCR_BENES, '*', '0'), '') AS FLOAT64)               as part_a_original_medicare_benes,
        SAFE_CAST(NULLIF(REPLACE(A_MA_AND_OTH_BENES, '*', '0'), '') AS FLOAT64)               as part_a_ma_other_benes,
        SAFE_CAST(NULLIF(REPLACE(B_TOT_BENES, '*', '0'), '') AS FLOAT64)                      as part_b_total_benes,
        SAFE_CAST(NULLIF(REPLACE(B_ORGNL_MDCR_BENES, '*', '0'), '') AS FLOAT64)               as part_b_original_medicare_benes,
        SAFE_CAST(NULLIF(REPLACE(B_MA_AND_OTH_BENES, '*', '0'), '') AS FLOAT64)               as part_b_ma_other_benes,

        -- Part D
        SAFE_CAST(NULLIF(REPLACE(PRSCRPTN_DRUG_TOT_BENES, '*', '0'), '') AS FLOAT64)          as part_d_total_benes,
        SAFE_CAST(NULLIF(REPLACE(PRSCRPTN_DRUG_PDP_BENES, '*', '0'), '') AS FLOAT64)          as part_d_pdp_benes,
        SAFE_CAST(NULLIF(REPLACE(PRSCRPTN_DRUG_MAPD_BENES, '*', '0'), '') AS FLOAT64)         as part_d_mapd_benes,
        SAFE_CAST(NULLIF(REPLACE(PRSCRPTN_DRUG_DEEMED_ELIGIBLE_FULL_LIS_BENES, '*', '0'), '') AS FLOAT64) as part_d_deemed_full_lis_benes,
        SAFE_CAST(NULLIF(REPLACE(PRSCRPTN_DRUG_FULL_LIS_BENES, '*', '0'), '') AS FLOAT64)     as part_d_full_lis_benes,
        SAFE_CAST(NULLIF(REPLACE(PRSCRPTN_DRUG_PARTIAL_LIS_BENES, '*', '0'), '') AS FLOAT64)  as part_d_partial_lis_benes,
        SAFE_CAST(NULLIF(REPLACE(PRSCRPTN_DRUG_NO_LIS_BENES, '*', '0'), '') AS FLOAT64)       as part_d_no_lis_benes

    from source
    where YEAR is not null
      and MONTH = 'Year'
)

select * from renamed