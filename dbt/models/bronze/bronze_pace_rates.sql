{{ config(materialized="view") }}


with
    first as (

        select
            *,
            split(rating_period, '-')[offset(0)] as rp_start_str,  -- Get rating period start and end dates as strings
            split(rating_period, '-')[offset(1)] as rp_end_str
        from {{ source("raw", "raw_CA_PACE_Rates_2022") }}
    ),

    second as (
        select
            *,
            split(rating_period, '-')[offset(0)] as rp_start_str,  -- Get rating period start and end dates as strings
            split(rating_period, '-')[offset(1)] as rp_end_str
        from {{ source("raw", "raw_CA_PACE_Rates_2023") }}
    ),

    third as (
        select
            *,
            split(rating_period, '-')[offset(0)] as rp_start_str,  -- Get rating period start and end dates as strings
            split(rating_period, '-')[offset(1)] as rp_end_str
        from {{ source("raw", "raw_CA_PACE_Rates_2024") }}
    ),
    all_rates as (

        select *
        from first
        union all
        select *
        from second
        union all
        select *
        from third

    )

select
    -- add unique ID
    md5(concat(county, '-', calendar_year, '-', rating_period)) as row_id,

    -- preserve metadata
    _airbyte_extracted_at as createdate,

    -- clean up text columns
    regexp_replace(trim(pace_organization), r'\s+', ' ') as organization,
    regexp_replace(trim(county), r'\s+', ' ') as county,
    regexp_replace(trim(category_of_aid), r'\s+', ' ') as category,
    trim(rating_period) as rating_period_raw,

    -- parse rating period dates
    parse_date('%m/%Y', rp_start_str) as rating_period_startmonth,
    parse_date('%m/%Y', rp_end_str) as rating_period_endmonth,

    -- cast numbers
    safe_cast(calendar_year as int) as year,
    safe_cast(regexp_replace(awop, r'[^0-9\.]', '') as numeric) as awop,
    safe_cast(regexp_replace(midpoint, r'[^0-9\.]', '') as numeric) as midpoint,
    safe_cast(regexp_replace(lower_bound, r'[^0-9\.]', '') as numeric) as lower_bound,
    safe_cast(regexp_replace(upper_bound, r'[^0-9\.]', '') as numeric) as upper_bound,

from all_rates
