{{
  config(
    materialized='table',
    tags=['cricket', 'narratives', 'intermediate'],
  )
}}

-- Intermediate layer: Handle versioning and enrich with match context
-- Marks most recent narrative for each match_id + description_type

with ranked_narratives as (
  select
    raw_narrative_id
    , match_id
    , description_type
    , description
    , generated_at
    , model
    , loaded_at
    , ROW_NUMBER() over (partition by match_id, description_type order by generated_at desc) as version_num
  from {{ ref('stg_cricket__narratives') }}
)

, narratives as (
  select
    *
    , COALESCE(version_num = 1, false) as is_most_recent
  from ranked_narratives
  where version_num = 1
)

select
  raw_narrative_id
  , match_id
  , description_type
  , description
  , generated_at
  , model
  , loaded_at
  , version_num
  , is_most_recent
  , ROW_NUMBER() over (order by generated_at desc) as created_row_id
from narratives
