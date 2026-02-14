copy (
  with raw_distinct as (
    select distinct
      nullif(trim(info.venue), '') as venue,
      nullif(trim(info.city), '') as city
    from stg_cricket__raw_json
    where nullif(trim(info.venue), '') is not null
  ),
  existing_seed as (
    select
      nullif(trim(venue), '') as venue,
      nullif(trim(city), '') as city,
      nullif(trim(country), '') as country,
      lower(trim(venue)) as venue_key,
      lower(trim(coalesce(city, ''))) as city_key
    from read_csv_auto('seeds/venue_country_mapping.csv', header=true)
    where nullif(trim(venue), '') is not null
  ),
  seed_pair_country as (
    select venue_key, city_key, max(country) as country
    from existing_seed
    group by 1, 2
  ),
  seed_unique_venue_country as (
    select
      venue_key,
      case when count(distinct country) = 1 then max(country) end as unique_country
    from existing_seed
    where country is not null
    group by 1
  )
  select
    r.venue,
    r.city,
    coalesce(spc.country, case when r.city is null then suvc.unique_country end) as country
  from raw_distinct r
  left join seed_pair_country spc
    on lower(trim(r.venue)) = spc.venue_key
   and lower(trim(coalesce(r.city, ''))) = spc.city_key
  left join seed_unique_venue_country suvc
    on lower(trim(r.venue)) = suvc.venue_key
  order by r.venue, r.city nulls last
) to 'seeds/venue_country_mapping_new.csv' (header, delimiter ',', quote '"');
