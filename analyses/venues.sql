-- Venue exploration for curated mapping + persistent IDs
-- Focus: health checks, integrity checks, and drill-downs.

-- 1) High-level status: seeds + resolved model coverage
with seed as (
  select
    nullif(trim(venue), '') as venue
    , nullif(trim(city), '') as city
    , nullif(trim(country), '') as country
  from read_csv_auto('seeds/venue_country_mapping.csv', header=true)
)

, resolved as (
  select
    match_id
    , venue_from_source
    , city_from_source
    , venue
    , city_mapped_or_source
    , match_country
    , venue_id
  from int_cricket__match_venues
)

select
  (select count(*) from seed) as seed_rows
  , (select count(*) from seed where city is null) as seed_missing_city
  , (select count(*) from seed where country is null) as seed_missing_country
  , (select count(*) from resolved) as resolved_match_rows
  , (select count(*) from resolved where match_country is null) as resolved_missing_country
  , (select count(*) from resolved where venue_id is null) as resolved_missing_venue_id
;

-- 2) Seed distribution by country (top 25)
select
  coalesce(country, '<NULL>') as country
  , count(*) as venue_city_rows
from read_csv_auto('seeds/venue_country_mapping.csv', header=true)
group by 1
order by venue_city_rows desc, country
limit 25
;

-- 3) Rows still incomplete in curated seed (prioritized)
select
  venue
  , city
  , country
from read_csv_auto('seeds/venue_country_mapping.csv', header=true)
where nullif(trim(city), '') is null or nullif(trim(country), '') is null
order by venue, city
;

-- 4) Alias seed quality checks (missing canonicals, duplicates)
with alias_seed as (
  select
    nullif(trim(alias_venue), '') as alias_venue
    , nullif(trim(alias_city), '') as alias_city
    , nullif(trim(canonical_venue), '') as canonical_venue
    , nullif(trim(canonical_city), '') as canonical_city
    , nullif(trim(canonical_country), '') as canonical_country
  from read_csv_auto('seeds/venue_alias_mapping.csv', header=true)
)

select
  count(*) as alias_rows
  , count(*) filter (where alias_venue is null) as alias_missing_alias_venue
  , count(*) filter (where canonical_venue is null) as alias_missing_canonical_venue
  , count(*) filter (where canonical_country is null) as alias_missing_canonical_country
  , count(*) - count(distinct lower(coalesce(alias_venue, '')) || '|' || lower(coalesce(alias_city, ''))) as alias_duplicate_keys
from alias_seed
;

-- 5) Venue master quality checks (ID uniqueness + nulls)
with vm as (
  select
    nullif(trim(venue_id), '') as venue_id
    , nullif(trim(canonical_venue), '') as canonical_venue
    , nullif(trim(canonical_city), '') as canonical_city
    , nullif(trim(canonical_country), '') as canonical_country
  from read_csv_auto('seeds/venue_master_mapping.csv', header=true)
)

select
  count(*) as master_rows
  , count(*) filter (where venue_id is null) as master_missing_venue_id
  , count(*) - count(distinct venue_id) as master_duplicate_venue_id
  , count(*) filter (where canonical_venue is null) as master_missing_canonical_venue
  , count(*) filter (where canonical_country is null) as master_missing_canonical_country
from vm
;

-- 6) Resolved matches with null venue_id (should trend to zero)
select
  venue_from_source
  , city_from_source
  , venue
  , city_mapped_or_source
  , match_country
  , count(*) as match_count
from int_cricket__match_venues
where venue_id is null
group by 1, 2, 3, 4, 5
order by match_count desc, venue_from_source
limit 100
;

-- 7) Most-used venues by venue_id (sanity + analyst convenience)
select
  venue_id
  , venue
  , city_mapped_or_source as city
  , match_country as country
  , count(*) as match_count
from int_cricket__match_venues
where venue_id is not null
group by 1, 2, 3, 4
order by match_count desc, venue
limit 100
;