{{ config(materialized='table', tags=['intermediate', 'batting']) }}

-- Determine batting order/position for each batter in each innings
-- Based on the delivery number when they first faced a ball

with first_ball_faced as (
  select
    match_id
    , innings_number
    , batter
    , min(delivery_idx) as first_delivery_idx
  from {{ ref('int_cricket__deliveries_flattened') }}
  group by 1, 2, 3
)

, batting_order as (
  select
    match_id
    , innings_number
    , batter
    , first_delivery_idx
    , row_number() over (
      partition by match_id, innings_number
      order by first_delivery_idx asc
    ) as batting_position
  from first_ball_faced
)

select
  match_id
  , innings_number
  , batter
  , batting_position
from batting_order
