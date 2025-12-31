{{ config(
    materialized='table',
    tags=['stage', 'raw'],
    pre_hook=[
        "SET preserve_insertion_order = false",
        "SET threads = 8"
    ]
) }}

with raw_json as (
  select
    payload.*
    , payload.filename as source_file_path
    , regexp_replace(
      coalesce(regexp_extract(payload.filename, '([^/]+)\.json$'), payload.filename)
      , '\.json$'
      , ''
    )                  as file_stub
  from read_json_auto(
    '{{ cricket_raw_json_glob() }}'
    , maximum_depth = 6
    , ignore_errors = true
    , filename = true
    , union_by_name = true
  ) as payload
)

select
  file_stub::varchar          as match_id
  , source_file_path
  , meta
  , info
  , innings
  , try_cast(meta as json)    as meta_json
  , try_cast(info as json)    as info_json
  , try_cast(innings as json) as innings_json
  , now()                     as ingested_at
from raw_json
