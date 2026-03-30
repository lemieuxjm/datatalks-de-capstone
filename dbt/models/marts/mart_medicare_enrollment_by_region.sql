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

inflation as (
    select * from {{ ref('inflation_annual') }}
),

with_region as (
    select
        s.enrollment_year,
        s.state_abbr,
        s.state_name,
        r.hhs_region,
        s.total_benes,
        s.original_medicare_benes,
        s.ma_other_benes,
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
        w.enrollment_year,
        w.hhs_region,
        SUM(w.total_benes)             as total_benes,
        SUM(w.original_medicare_benes) as original_medicare_benes,
        SUM(w.ma_other_benes)          as ma_other_benes,
        SUM(w.white_benes)             as white_benes,
        SUM(w.black_benes)             as black_benes,
        SUM(w.api_benes)               as api_benes,
        SUM(w.hispanic_benes)          as hispanic_benes,
        SUM(w.native_indian_benes)     as native_indian_benes,
        SUM(w.other_race_benes)        as other_race_benes,
        i.inflation_rate
    from with_region w
    left join inflation i
        on w.enrollment_year = i.enrollment_year
    group by w.enrollment_year, w.hhs_region, i.inflation_rate
),

baseline as (
    select
        a.hhs_region,
        a.total_benes             as base_total_benes,
        a.original_medicare_benes as base_original_medicare_benes,
        a.ma_other_benes          as base_ma_other_benes,
        i.inflation_rate          as base_inflation_rate
    from aggregated a
    cross join (
        select inflation_rate 
        from inflation 
        where enrollment_year = 2013
    ) i
    where a.enrollment_year = 2013
),

indexed as (
    select
        a.enrollment_year,
        a.hhs_region,

        -- Raw enrollment columns (kept for classification tile)
        a.total_benes,
        a.original_medicare_benes,
        a.ma_other_benes,
        a.white_benes,
        a.black_benes,
        a.api_benes,
        a.hispanic_benes,
        a.native_indian_benes,
        a.other_race_benes,

        -- Raw inflation rate (kept for reference)
        a.inflation_rate,

        -- Indexed enrollment (2013 = 100)
        ROUND((a.total_benes / b.base_total_benes) * 100, 2)
            as total_benes_index,
        ROUND((a.original_medicare_benes / b.base_original_medicare_benes) * 100, 2)
            as original_medicare_index,
        ROUND((a.ma_other_benes / b.base_ma_other_benes) * 100, 2)
            as ma_other_index,

        -- Indexed inflation rate (2013 = 100)
        ROUND((a.inflation_rate / b.base_inflation_rate) * 100, 2)
            as inflation_rate_index

    from aggregated a
    left join baseline b
        on a.hhs_region = b.hhs_region
)

select * from indexed