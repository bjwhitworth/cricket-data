{{ config(materialized='table', tags=['marts', 'batting', 'analytics']) }}

-- Enhanced batter innings with context and performance metrics
-- Includes batting position, partnership info, match situation, and performance vs expectations

with base_innings as (
  select * from {{ ref('fct_batter_innings') }}
)

, batting_positions as (
  select
    match_id
    , innings_number
    , batter
    , batting_position
  from {{ ref('int_cricket__batting_order') }}
)

-- Get partnership information for each batter
, partnerships as (
  select
    match_id
    , innings_number
    , batter_1      as batter
    , batter_2      as partner
    , partnership_number
    , partnership_runs
    , partnership_balls
    , partnership_run_rate
    , batter_1_runs as batter_runs_in_partnership
    , batter_2_runs as partner_runs_in_partnership
  from {{ ref('int_cricket__partnerships') }}
  union all
  select
    match_id
    , innings_number
    , batter_2      as batter
    , batter_1      as partner
    , partnership_number
    , partnership_runs
    , partnership_balls
    , partnership_run_rate
    , batter_2_runs as batter_runs_in_partnership
    , batter_1_runs as partner_runs_in_partnership
  from {{ ref('int_cricket__partnerships') }}
)

, innings_context as (
  select
    match_id
    , innings_number
    , innings_context
    , target_runs
    , runs_short_of_target
    , successfully_chased
    , innings_team_won
  from {{ ref('int_cricket__innings_context') }}
)

-- Calculate each batter's historical average (excluding current match)
-- TODO: this should probably be partitioned at least by type of match
, batter_historical_avg as (
  select
    batter
    , match_id
    , avg(runs_scored) over (
      partition by batter
      order by match_start_date
      rows between unbounded preceding and 1 preceding
    ) as career_avg_before_match
    , stddev(runs_scored) over (
      partition by batter
      order by match_start_date
      rows between unbounded preceding and 1 preceding
    ) as career_stddev_before_match
  from base_innings
  where balls_faced >= 5  -- Exclude very short innings from average
  qualify row_number() over (partition by batter, match_id order by innings_number asc) = 1
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
  , bi.strike_rate
  , bi.season
  , bi.match_type
  , bi.match_start_date
  , bi.venue_id
  , bi.venue
  , bi.city
  , bi.match_country
  , bi.event_name
  , bi.toss_winner
  , bi.winner
  , bi.batting_team_nation_type

  -- Batting position enrichment
  , bp.batting_position

  -- Partnership enrichment
  , p.partnership_number
  , p.partner
  , p.partnership_runs
  , p.partnership_balls
  , p.partnership_run_rate
  , p.batter_runs_in_partnership
  , p.partner_runs_in_partnership

  -- Match context
  , ic.innings_context
  , ic.target_runs
  , ic.runs_short_of_target
  , ic.successfully_chased
  , ic.innings_team_won

  -- Performance metrics
  , bha.career_avg_before_match
  , bha.career_stddev_before_match

  -- Historical comparison
  , coalesce(bi.batting_team = bi.winner, false)                                       as batter_team_won
  , round(p.batter_runs_in_partnership * 1.0 / nullif(p.partnership_runs, 0) * 100, 1) as pct_of_partnership
  , round(bi.runs_scored - bha.career_avg_before_match, 1)                             as runs_above_career_avg
  , case
    when bha.career_stddev_before_match > 0
      then
        round((bi.runs_scored - bha.career_avg_before_match) / bha.career_stddev_before_match, 2)
  end                                                                                  as std_devs_above_avg

  -- Performance indicators
  -- Again, needs to be refined based on match type, opposition, conditions, etc. if we are including this
  , case
    when bi.runs_scored >= 300 then 'triple_hundred'
    when bi.runs_scored >= 200 then 'double_hundred'
    when bi.runs_scored >= 150 then 'hundred_and_fifty'
    when bi.runs_scored >= 100 then 'hundred'
    when bi.runs_scored >= 50 then 'fifty'
    else null
  end                                                                                  as innings_category

from base_innings as bi
left join batting_positions as bp
  on
    bi.match_id = bp.match_id
    and bi.innings_number = bp.innings_number
    and bi.batter = bp.batter
left join partnerships as p
  on
    bi.match_id = p.match_id
    and bi.innings_number = p.innings_number
    and bi.batter = p.batter
left join innings_context as ic
  on
    bi.match_id = ic.match_id
    and bi.innings_number = ic.innings_number
left join batter_historical_avg as bha
  on
    bi.batter = bha.batter
    and bi.match_id = bha.match_id
