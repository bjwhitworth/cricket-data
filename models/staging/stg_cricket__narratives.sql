{{
  config(
    materialized='view',
    tags=['cricket', 'narratives'],
  )
}}

-- Parse raw JSON narratives into typed columns
-- Validates that required fields are present

WITH parsed AS (
  SELECT
    raw_narrative_id,
    narrative_json->>'match_id' AS match_id,
    narrative_json->>'description_type' AS description_type,
    narrative_json->>'description' AS description,
    narrative_json->>'generated_at' AS generated_at_raw,
    narrative_json->>'model' AS model,
    loaded_at,
    ROW_NUMBER() OVER (PARTITION BY narrative_json->>'match_id', narrative_json->>'description_type' ORDER BY loaded_at DESC) AS row_num
  FROM {{ source('raw', 'raw_narratives') }}
  WHERE narrative_json IS NOT NULL
),

validated AS (
  SELECT
    raw_narrative_id,
    match_id,
    description_type,
    description,
    CAST(generated_at_raw AS TIMESTAMP) AS generated_at,
    model,
    loaded_at,
    row_num
  FROM parsed
  WHERE 
    match_id IS NOT NULL
    AND description_type IN ('brief', 'full')
    AND description IS NOT NULL
    AND LENGTH(description) > 0
)

SELECT * FROM validated
