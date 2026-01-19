{{
  config(
    materialized='table',
    tags=['cricket', 'narratives', 'intermediate'],
  )
}}

-- Intermediate layer: Handle versioning and enrich with match context
-- Marks most recent narrative for each match_id + description_type

WITH narratives AS (
  SELECT
    raw_narrative_id,
    match_id,
    description_type,
    description,
    generated_at,
    model,
    loaded_at,
    ROW_NUMBER() OVER (PARTITION BY match_id, description_type ORDER BY generated_at DESC) AS version_num,
    CASE WHEN ROW_NUMBER() OVER (PARTITION BY match_id, description_type ORDER BY generated_at DESC) = 1
      THEN true ELSE false
    END AS is_most_recent
  FROM {{ ref('stg_cricket__narratives') }}
  WHERE is_most_recent = true
)

SELECT
  raw_narrative_id,
  match_id,
  description_type,
  description,
  generated_at,
  model,
  loaded_at,
  version_num,
  is_most_recent,
  ROW_NUMBER() OVER (ORDER BY generated_at DESC) AS created_row_id
FROM narratives
