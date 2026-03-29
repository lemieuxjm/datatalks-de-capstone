{{ config(
    materialized='table',
    schema='capstone_gold'
) }}

with state_data as (
    select *
    from {{ ref('stg_medicare_enrollment_annual') }}
    where geo_level = 'State'
      and state_abbr is not null
),

region_lookup as (
    select * from {{ ref('hhs_region_lookup') }}
),

with_region as (
    select
        s.enrollment_year,
        s.state_abbr,
        s.state_name,
        r.hhs_region,

        -- Total enrollment
        s.total_benes,
        s.original_medicare_benes,
        s.ma_other_benes,

        -- Race/ethnicity
        s.white_benes,
        s.black_benes,
        s.api_benes,
        s.hispanic_benes,
        s.native_indian_benes,
        s.other_race_benes

    from state_data s
    inner join region_lookup r
        on s.state_abbr = r.state_abbr
),

aggregated as (
    select
        enrollment_year,
        hhs_region,
        SUM(total_benes)             as total_benes,
        SUM(original_medicare_benes) as original_medicare_benes,
        SUM(ma_other_benes)          as ma_other_benes,
        SUM(white_benes)             as white_benes,
        SUM(black_benes)             as black_benes,
        SUM(api_benes)               as api_benes,
        SUM(hispanic_benes)          as hispanic_benes,
        SUM(native_indian_benes)     as native_indian_benes,
        SUM(other_race_benes)        as other_race_benes
    from with_region
    group by enrollment_year, hhs_region
)

select * from aggregated