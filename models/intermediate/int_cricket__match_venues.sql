{{ config(materialized='view', tags=['intermediate']) }}

with base as (
  select
    match_id
    , info.venue as venue_from_source
    , info.city as city_from_source
  from {{ ref('stg_cricket__raw_json') }}
)

, alias_mapping as (
  select
    nullif(trim(alias_venue), '') as alias_venue
    , nullif(trim(alias_city), '') as alias_city
    , nullif(trim(canonical_venue), '') as canonical_venue
    , nullif(trim(canonical_city), '') as canonical_city
    , nullif(trim(canonical_country), '') as canonical_country
    , lower(trim(alias_venue)) as alias_venue_key
    , lower(trim(coalesce(alias_city, ''))) as alias_city_key
  from {{ ref('venue_alias_mapping') }}
  where
    nullif(trim(alias_venue), '') is not null
    and nullif(trim(canonical_venue), '') is not null
)

, base_with_alias as (
  select
    b.match_id
    , b.venue_from_source
    , b.city_from_source
    , coalesce(am.canonical_venue, b.venue_from_source) as venue_for_mapping
    , coalesce(am.canonical_city, b.city_from_source) as city_for_mapping
    , am.canonical_country as country_from_alias
  from base as b
  left join alias_mapping as am
    on
      lower(trim(coalesce(b.venue_from_source, ''))) = am.alias_venue_key
      and (
        am.alias_city_key = ''
        or lower(trim(coalesce(b.city_from_source, ''))) = am.alias_city_key
      )
  qualify row_number() over (
    partition by b.match_id
    order by
      case
        when am.alias_city_key <> '' and lower(trim(coalesce(b.city_from_source, ''))) = am.alias_city_key then 2
        when am.alias_city_key = '' then 1
        else 0
      end desc
      , case when am.canonical_country is not null then 1 else 0 end desc
  ) = 1
)

, seed_mapping as (
  select
    nullif(trim(venue), '') as venue_seed
    , nullif(trim(city), '') as city_seed
    , nullif(trim(country), '') as country_seed
    , lower(trim(venue)) as venue_key
    , lower(trim(coalesce(city, ''))) as city_key
  from {{ ref('venue_country_mapping') }}
  where nullif(trim(venue), '') is not null
)

, venue_master as (
  select
    nullif(trim(venue_id), '') as venue_id
    , nullif(trim(canonical_venue), '') as canonical_venue
    , nullif(trim(canonical_city), '') as canonical_city
    , nullif(trim(canonical_country), '') as canonical_country
    , lower(trim(canonical_venue)) as canonical_venue_key
    , lower(trim(coalesce(canonical_city, ''))) as canonical_city_key
    , lower(trim(coalesce(canonical_country, ''))) as canonical_country_key
  from {{ ref('venue_master_mapping') }}
  where nullif(trim(venue_id), '') is not null
)

, seed_venue_stats as (
  select
    venue_key
    , count(distinct coalesce(city_seed, '') || '|' || coalesce(country_seed, '')) as distinct_city_country_pairs
  from seed_mapping
  group by 1
)

, match_venue_resolution as (
  select
    b.match_id
    , b.venue_from_source
    , b.city_from_source
    , b.venue_for_mapping
    , b.city_for_mapping
    , b.country_from_alias
    , sm.venue_seed
    , sm.city_seed
    , sm.country_seed
    , case
      when sm.venue_seed is null
        then 0
      when b.city_for_mapping is not null and sm.city_seed is not null
        and lower(trim(b.city_for_mapping)) = sm.city_key
        then 3
      when svs.distinct_city_country_pairs = 1
        then 2
      else 0
    end as match_confidence
  from base_with_alias as b
  left join seed_mapping as sm
    on lower(trim(coalesce(b.venue_for_mapping, ''))) = sm.venue_key
  left join seed_venue_stats as svs
    on sm.venue_key = svs.venue_key
  qualify row_number() over (
    partition by b.match_id
    order by
      match_confidence desc
      , case when sm.country_seed is not null then 1 else 0 end desc
  ) = 1
)

, resolved as (
  select
    match_id
    , coalesce(case when match_confidence > 0 then venue_seed end, venue_for_mapping) as venue
    , venue_from_source
    , city_from_source
    , coalesce(case when match_confidence > 0 then city_seed end, city_for_mapping) as city_mapped_or_source
    , coalesce(country_from_alias, case when match_confidence > 0 then country_seed end) as match_country
  from match_venue_resolution
)

select
  r.match_id
  , vm.venue_id
  , r.venue
  , r.venue_from_source
  , r.city_from_source
  , r.city_mapped_or_source
  , r.match_country
from resolved as r
left join venue_master as vm
  on
    lower(trim(coalesce(r.venue, ''))) = vm.canonical_venue_key
    and lower(trim(coalesce(r.city_mapped_or_source, ''))) = vm.canonical_city_key
    and lower(trim(coalesce(r.match_country, ''))) = vm.canonical_country_key
