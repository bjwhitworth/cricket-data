{{ config(materialized='view', tags=['intermediate']) }}

with innings as (
  select
    match_id
    , innings_number
    , inning_struct
    , batting_team
    , ingested_at
    , miscounted_overs
  from {{ ref('int_cricket__innings_flattened') }}
)

select
  innings.match_id
  , innings.ingested_at
  , innings.innings_number
  , innings.batting_team
  , o.over_idx - 1 as over_idx  -- Convert from 1-based ordinality to 0-based over number
  , o.over_struct
  -- Check miscounted using 0-based over number
  , map_contains(coalesce(innings.miscounted_overs, map()), cast(o.over_idx - 1 as varchar)) as is_miscounted_over_from_data
from innings
cross join unnest(innings.inning_struct.overs) with ordinality as o (over_struct, over_idx)
