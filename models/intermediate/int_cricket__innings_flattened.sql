{{ config(materialized='view', tags=['intermediate']) }}

with raw as (
  select
    match_id
    , innings
    , ingested_at
  from {{ ref('stg_cricket__raw_json') }}
)

select
  raw.match_id
  , t.inning_idx                                as innings_number
  , t.inning_struct
  , t.inning_struct.team                        as batting_team
  , raw.ingested_at
  , coalesce(t.inning_struct.super_over, false) as is_super_over
  , try_cast(t.inning_struct.target.overs as integer) as target_overs
  , try_cast(trim(both '"' from t.inning_struct.target.runs::varchar) as integer) as target_runs
  , t.inning_struct.miscounted_overs as miscounted_overs
from raw
cross join unnest(raw.innings) with ordinality as t (inning_struct, inning_idx)
