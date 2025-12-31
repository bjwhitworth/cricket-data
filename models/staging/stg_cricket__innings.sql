{{ config(materialized='view', tags=['stage']) }}

with expanded_innings as (
  select
    match_id
    , ingested_at
    , innings_number
    , inning_struct
    , is_super_over
  from {{ ref('int_cricket__innings_flattened') }}
)

, over_stats as (
  select
    expanded_innings.match_id
    , expanded_innings.innings_number
    , len(expanded_innings.inning_struct.overs) as recorded_over_count
  from expanded_innings
)

, delivery_stats as (
  select
    d.match_id
    , d.innings_number
    , sum(coalesce(cast(d.runs_total as integer), 0))  as runs_total
    , sum(coalesce(cast(d.runs_extras as integer), 0)) as runs_extras
    , count_if(d.is_wicket)                            as wickets
  from {{ ref('int_cricket__deliveries_flattened') }} as d
  group by 1, 2
)

select
  ei.match_id
  , ei.innings_number
  , ei.inning_struct.team                       as batting_team
  , ei.is_super_over
  , ei.ingested_at
  , try_cast(os.recorded_over_count as integer) as recorded_over_count
  , try_cast(ds.runs_total as integer)          as runs_total
  , try_cast(ds.runs_extras as integer)         as runs_extras
  , try_cast(ds.wickets as integer)             as wickets_fallen
from expanded_innings as ei
left join over_stats as os
  on
    ei.match_id = os.match_id
    and ei.innings_number = os.innings_number
left join delivery_stats as ds
  on
    ei.match_id = ds.match_id
    and ei.innings_number = ds.innings_number
