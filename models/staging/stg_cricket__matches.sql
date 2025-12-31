{{ config(materialized='view', tags=['stage']) }}

select *
from {{ ref('int_cricket__matches_flattened') }}
