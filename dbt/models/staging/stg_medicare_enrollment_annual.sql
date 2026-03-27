{{ config(
    materialized='view',
    schema='capstone_silver'
) }}

with source as (
    select * from {{ source('capstone_raw', 'medicare_monthly_enrollment_20251231') }}
),

renamed as (
    select
        -- Time dimension
        CAST(YEAR AS INT64)                                          as enrollment_year,

        -- Geography
        BENE_GEO_LVL                                                 as geo_level,
        BENE_STATE_ABRVTN                                            as state_abbr,
        BENE_STATE_DESC                                              as state_name,
        BENE_COUNTY_DESC                                             as county_name,
        BENE_FIPS_CD                                                 as fips_code,

        -- Total enrollment
        SAFE_CAST(TOT_BENES AS INT64)                                as total_benes,
        SAFE_CAST(ORGNL_MDCR_BENES AS INT64)                        as original_medicare_benes,
        SAFE_CAST(MA_AND_OTH_BENES AS INT64)                        as ma_other_benes,

        -- Age & disability
        SAFE_CAST(AGED_TOT_BENES AS INT64)                          as aged_total_benes,
        SAFE_CAST(AGED_ESRD_BENES AS INT64)                         as aged_esrd_benes,
        SAFE_CAST(AGED_NO_ESRD_BENES AS INT64)                      as aged_no_esrd_benes,
        SAFE_CAST(DSBLD_TOT_BENES AS INT64)                         as disabled_total_benes,
        SAFE_CAST(DSBLD_ESRD_AND_ESRD_ONLY_BENES AS INT64)          as disabled_esrd_benes,
        SAFE_CAST(DSBLD_NO_ESRD_BENES AS INT64)                     as disabled_no_esrd_benes,

        -- Gender
        SAFE_CAST(MALE_TOT_BENES AS INT64)                          as male_benes,
        SAFE_CAST(FEMALE_TOT_BENES AS INT64)                        as female_benes,

        -- Race & ethnicity
        SAFE_CAST(WHITE_TOT_BENES AS INT64)                         as white_benes,
        SAFE_CAST(BLACK_TOT_BENES AS INT64)                         as black_benes,
        SAFE_CAST(API_TOT_BENES AS INT64)                           as api_benes,
        SAFE_CAST(HSPNC_TOT_BENES AS INT64)                         as hispanic_benes,
        SAFE_CAST(NATIND_TOT_BENES AS INT64)                        as native_indian_benes,
        SAFE_CAST(OTHR_TOT_BENES AS INT64)                          as other_race_benes,

        -- Age bands
        SAFE_CAST(AGE_LT_25_BENES AS INT64)                         as age_lt_25_benes,
        SAFE_CAST(AGE_25_TO_44_BENES AS INT64)                      as age_25_to_44_benes,
        SAFE_CAST(AGE_45_TO_64_BENES AS INT64)                      as age_45_to_64_benes,
        SAFE_CAST(AGE_65_TO_69_BENES AS INT64)                      as age_65_to_69_benes,
        SAFE_CAST(AGE_70_TO_74_BENES AS INT64)                      as age_70_to_74_benes,
        SAFE_CAST(AGE_75_TO_79_BENES AS INT64)                      as age_75_to_79_benes,
        SAFE_CAST(AGE_80_TO_84_BENES AS INT64)                      as age_80_to_84_benes,
        SAFE_CAST(AGE_85_TO_89_BENES AS INT64)                      as age_85_to_89_benes,
        SAFE_CAST(AGE_90_TO_94_BENES AS INT64)                      as age_90_to_94_benes,
        SAFE_CAST(AGE_GT_94_BENES AS INT64)                         as age_gt_94_benes,

        -- Dual eligibility
        SAFE_CAST(DUAL_TOT_BENES AS INT64)                          as dual_total_benes,
        SAFE_CAST(FULL_DUAL_TOT_BENES AS INT64)                     as full_dual_benes,
        SAFE_CAST(PART_DUAL_TOT_BENES AS INT64)                     as partial_dual_benes,
        SAFE_CAST(NODUAL_TOT_BENES AS INT64)                        as no_dual_benes,
        SAFE_CAST(QMB_ONLY_BENES AS INT64)                          as qmb_only_benes,
        SAFE_CAST(QMB_PLUS_BENES AS INT64)                          as qmb_plus_benes,
        SAFE_CAST(SLMB_ONLY_BENES AS INT64)                         as slmb_only_benes,
        SAFE_CAST(SLMB_PLUS_BENES AS INT64)                         as slmb_plus_benes,
        SAFE_CAST(QDWI_QI_BENES AS INT64)                           as qdwi_qi_benes,
        SAFE_CAST(OTHR_FULL_DUAL_MDCD_BENES AS INT64)               as other_full_dual_benes,

        -- Parts A & B
        SAFE_CAST(A_B_TOT_BENES AS INT64)                           as part_ab_total_benes,
        SAFE_CAST(A_B_ORGNL_MDCR_BENES AS INT64)                    as part_ab_original_medicare_benes,
        SAFE_CAST(A_B_MA_AND_OTH_BENES AS INT64)                    as part_ab_ma_other_benes,
        SAFE_CAST(A_TOT_BENES AS INT64)                             as part_a_total_benes,
        SAFE_CAST(A_ORGNL_MDCR_BENES AS INT64)                      as part_a_original_medicare_benes,
        SAFE_CAST(A_MA_AND_OTH_BENES AS INT64)                      as part_a_ma_other_benes,
        SAFE_CAST(B_TOT_BENES AS INT64)                             as part_b_total_benes,
        SAFE_CAST(B_ORGNL_MDCR_BENES AS INT64)                      as part_b_original_medicare_benes,
        SAFE_CAST(B_MA_AND_OTH_BENES AS INT64)                      as part_b_ma_other_benes,

        -- Part D
        SAFE_CAST(PRSCRPTN_DRUG_TOT_BENES AS INT64)                 as part_d_total_benes,
        SAFE_CAST(PRSCRPTN_DRUG_PDP_BENES AS INT64)                 as part_d_pdp_benes,
        SAFE_CAST(PRSCRPTN_DRUG_MAPD_BENES AS INT64)                as part_d_mapd_benes,
        SAFE_CAST(PRSCRPTN_DRUG_DEEMED_ELIGIBLE_FULL_LIS_BENES AS INT64) as part_d_deemed_full_lis_benes,
        SAFE_CAST(PRSCRPTN_DRUG_FULL_LIS_BENES AS INT64)            as part_d_full_lis_benes,
        SAFE_CAST(PRSCRPTN_DRUG_PARTIAL_LIS_BENES AS INT64)         as part_d_partial_lis_benes,
        SAFE_CAST(PRSCRPTN_DRUG_NO_LIS_BENES AS INT64)              as part_d_no_lis_benes

    from source
    where YEAR is not null
      and MONTH = 'Year'
)

select * from renamed
