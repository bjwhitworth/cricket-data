{{ config(materialized='view', tags=['intermediate']) }}

with innings as (
  select
    match_id
    , innings_number
    , inning_struct
    , batting_team
    , ingested_at
  from {{ ref('int_cricket__innings_flattened') }}
)

select
  innings.match_id
  , innings.ingested_at
  , innings.innings_number
  , innings.batting_team
  , o.over_idx
  , o.over_struct
from innings
cross join unnest(innings.inning_struct.overs) with ordinality as o (over_struct, over_idx)
