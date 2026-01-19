{{
  config(
    materialized='view',
    tags=['cricket', 'narratives'],
  )
}}

-- Parse raw JSON narratives into typed columns
-- Validates that required fields are present

with parsed as (
  select
    raw_narrative_id
    , loaded_at
    , narrative_json
    ->> 'match_id'
      as match_id
    , narrative_json
    ->> 'description_type'
      as description_type
    , narrative_json
    ->> 'description'
      as description
    , narrative_json
    ->> 'generated_at'
      as generated_at_raw
    , narrative_json
    ->> 'model'
      as model
    , ROW_NUMBER()
      over (partition by narrative_json ->> 'match_id', narrative_json ->> 'description_type' order by loaded_at desc)
      as row_num
  from {{ source('raw_source', 'raw_narratives') }}
  where narrative_json is not NULL
)

, validated as (
  select
    raw_narrative_id
    , match_id
    , description_type
    , description
    , model
    , loaded_at
    , row_num
    , CAST(generated_at_raw as TIMESTAMP) as generated_at
  from parsed
  where
    match_id is not NULL
    and description_type in ('brief', 'full')
    and description is not NULL
    and LENGTH(description) > 0
)

select * from validated
