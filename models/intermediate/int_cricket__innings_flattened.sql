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
  , t.inning_idx         as innings_number
  , t.inning_struct
  , coalesce(t.inning_struct.super_over, false) as is_super_over
  , t.inning_struct.team as batting_team
  , raw.ingested_at
from raw
cross join unnest(raw.innings) with ordinality as t (inning_struct, inning_idx)
