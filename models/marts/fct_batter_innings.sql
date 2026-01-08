{{ config(materialized='table', tags=['marts', 'batting']) }}

with deliveries as (
  select
    match_id
    , innings_number
    , batting_team
    , batter
    , runs_batter
    , is_wicket
    , wicket_player_out
  from {{ ref('int_cricket__deliveries_flattened') }}
)

, matches as (
  select
    match_id
    , season
    , match_type
    , match_start_date
    , venue
    , city
    , event_name
    , event_match_number
    , toss_winner
    , winner
  from {{ ref('int_cricket__matches_flattened') }}
)

, batter_innings_stats as (
  select
    d.match_id
    , d.innings_number
    , d.batting_team
    , d.batter
    , sum(d.runs_batter)                                                              as runs_scored
    , count(*)                                                                        as balls_faced
    , count_if(d.runs_batter = 4)                                                     as fours
    , count_if(d.runs_batter = 6)                                                     as sixes
    , max(case when d.is_wicket and d.wicket_player_out = d.batter then 1 else 0 end) as was_out -- TODO: fix this, not working correctly
  from deliveries as d
  group by 1, 2, 3, 4
)

select
  bi.match_id
  , bi.innings_number
  , bi.batter
  , bi.batting_team
  , bi.runs_scored
  , bi.balls_faced
  , bi.fours
  , bi.sixes
  , bi.was_out
  , m.season
  , m.match_type
  , m.match_start_date
  , m.venue
  , m.city
  , m.event_name
  , m.event_match_number
  , m.toss_winner
  , m.winner
  , round(bi.runs_scored * 100.0 / nullif(bi.balls_faced, 0), 2) as strike_rate
from batter_innings_stats as bi
left join matches as m
  on bi.match_id = m.match_id
