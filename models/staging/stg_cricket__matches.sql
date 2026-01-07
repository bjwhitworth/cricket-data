{{ config(materialized='view', tags=['stage']) }}

select * 
exclude (
  source_file_path
  , meta_json
  , info_json
  , data_version
  , data_revision
  , data_created_date
  )
from {{ ref('int_cricket__matches_flattened') }}
