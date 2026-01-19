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
    , batter_1 as batter
    , batter_2 as partner
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
    , batter_1 as partner
    , batter_2 as batter
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
  , bi.venue
  , bi.city
  , bi.event_name
  , bi.toss_winner
  , bi.winner

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
  , coalesce(bi.batting_team = bi.winner, false)                         as batter_team_won
  , round(p.batter_runs_in_partnership * 1.0 / nullif(p.partnership_runs, 0) * 100, 1) as pct_of_partnership
  , round(bi.runs_scored - bha.career_avg_before_match, 1)               as runs_above_career_avg
  , case
    when bha.career_stddev_before_match > 0
      then
        round((bi.runs_scored - bha.career_avg_before_match) / bha.career_stddev_before_match, 2)
  end                                                                    as std_devs_above_avg

  -- Performance indicators
  , case
    when bi.runs_scored >= 100 then 'century'
    when bi.runs_scored >= 50 then 'half_century'
    when bi.runs_scored >= 30 then 'substantial'
    when bi.runs_scored >= 10 then 'start'
    else 'low_score'
  end                                                                    as innings_category

  -- Match impact scoring
  , case
    when bi.batting_team = bi.winner and bi.runs_scored >= 50 then 'match_winning'
    when bi.batting_team = bi.winner and bi.runs_scored >= 30 then 'important_contribution'
    when p.partnership_runs > 100 and p.partnership_number <= 3 then 'foundation_partnership'
    when
      ic.innings_context = 'chasing' and ic.successfully_chased = true and bi.runs_scored >= 30
      then 'successful_chase_contribution'
    else 'standard'
  end                                                                    as contribution_type

  -- Context-weighted runs (higher value in pressure situations)
  , round(
    bi.runs_scored
    * (1 + coalesce(p.partnership_number / 10.0, 0))  -- More value when batting lower in order
    * case when ic.innings_context = 'chasing' then 1.2 else 1.0 end  -- More value when chasing
    * case when bi.batting_team = bi.winner then 1.3 else 1.0 end  -- More value in wins
    , 1
  )                                                                      as context_weighted_runs

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
