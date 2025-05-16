{{ config(materialized='view') }}


WITH first as (

select *, 
    split(rating_period, '-')[offset(0)]   as rp_start_str, -- Get rating period start and end dates as strings
    split(rating_period, '-')[offset(1)]   as rp_end_str 
    from {{ source('raw', 'raw_CA_PACE_Rates_2022')}}
),

second as
(
  select * , 
    split(rating_period, '-')[offset(0)]   as rp_start_str, -- Get rating period start and end dates as strings
    split(rating_period, '-')[offset(1)]   as rp_end_str 
  from {{ source('raw', 'raw_CA_PACE_Rates_2023')}}
),

third as (
  select * , 
    split(rating_period, '-')[offset(0)]   as rp_start_str, -- Get rating period start and end dates as strings
    split(rating_period, '-')[offset(1)]   as rp_end_str 
  from {{ source('raw', 'raw_CA_PACE_Rates_2024')}}
)
,
all_rates as (

select * from first
UNION ALL
select * from second
UNION ALL
select * from third

)

select 
-- add unique ID
md5(concat(county, '-', Calendar_Year, '-', Rating_Period)) as row_ID,

--preserve metadata
  _airbyte_extracted_at as createdate, 
  
--clean up text columns
  regexp_replace(trim(PACE_organization), r'\s+', ' ') as organization,
  regexp_replace(trim(county),        r'\s+', ' ') as county,
  regexp_replace(trim(category_of_aid),        r'\s+', ' ') as category,
  trim(Rating_Period) as rating_period_raw,

--parse rating period dates
  parse_date('%m/%Y', rp_start_str)    as rating_period_startmonth,
  parse_date('%m/%Y', rp_end_str)    as rating_period_endmonth,

-- cast numbers
  SAFE_CAST(Calendar_Year as INT) as year,
  SAFE_CAST(REGEXP_REPLACE(AWOP, r'[^0-9\.]', '') as numeric) as AWOP,
  SAFE_CAST(REGEXP_REPLACE(Midpoint, r'[^0-9\.]', '') as numeric) as midpoint,
  SAFE_CAST(REGEXP_REPLACE(Lower_Bound, r'[^0-9\.]', '') as numeric) as lower_bound,
  SAFE_CAST(REGEXP_REPLACE(Upper_Bound, r'[^0-9\.]', '') as numeric) as upper_bound,

from all_rates