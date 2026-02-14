{{ config(materialized='table', tags=['marts', 'dimension']) }}

with venue_master as (
  select
    nullif(trim(venue_id), '') as venue_id
    , nullif(trim(canonical_venue), '') as venue_name
    , nullif(trim(canonical_city), '') as city
    , nullif(trim(canonical_country), '') as country
  from {{ ref('venue_master_mapping') }}
  where nullif(trim(venue_id), '') is not null
)

, usage_stats as (
  select
    venue_id
    , count(*) as match_count
    , min(match_id) as example_match_id
  from {{ ref('int_cricket__match_venues') }}
  where venue_id is not null
  group by 1
)

select
  vm.venue_id
  , vm.venue_name
  , vm.city
  , vm.country
  , coalesce(us.match_count, 0) as match_count
  , us.example_match_id
from venue_master as vm
left join usage_stats as us
  on vm.venue_id = us.venue_id
