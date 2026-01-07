{{ config(materialized='table', tags=['marts', 'bowling']) }}

with deliveries as (
  select
    match_id
    , innings_number
    , batting_team
    , bowler
    , runs_batter
    , runs_extras
    , extras_noballs
    , extras_wides
    , is_wicket
    , try_cast(wicket_kind as varchar) as wicket_kind
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
    , participating_teams
    , team_1
    , team_2
  from {{ ref('int_cricket__matches_flattened') }}
)

, match_innings_stats as (
  select
    i.match_id
    , i.innings_number
    , i.runs_total
    , i.wickets_fallen
    , i.recorded_over_count
    , i.is_super_over
  from {{ ref('stg_cricket__innings') }} as i
  group by 1, 2
)

, bowler_innings_stats as (
  select
    d.match_id
    , d.innings_number
    , d.batting_team
    , d.bowler
    , count(*)
    - count_if(d.extras_wides > 0 or d.extras_noballs > 0) as legal_deliveries
    , sum(
      d.runs_batter + d.runs_extras
    )                                                      as runs_conceded
    , count_if(
      d.is_wicket
      and coalesce(d.wicket_kind, '') not in ('run out', 'retired hurt', 'retired out', 'obstructing the field')
    )                                                      as wickets
    , count_if(
      d.extras_wides > 0
    )                                                      as wides
    , count_if(
      d.extras_noballs > 0
    )                                                      as noballs
    , count_if(
      d.runs_batter = 4
    )                                                      as fours_conceded
    , count_if(
      d.runs_batter = 6
    )                                                      as sixes_conceded
  from deliveries as d
  group by 1, 2, 3, 4
)

select
  bi.match_id
  , bi.innings_number
  , bi.bowler
  , bi.batting_team
  , bi.legal_deliveries
  , bi.runs_conceded
  , bi.wickets
  , bi.wides
  , bi.noballs
  , bi.fours_conceded
  , bi.sixes_conceded
  , m.season
  , m.match_type
  , m.match_start_date
  , m.venue
  , m.city
  , m.event_name
  , m.event_match_number
  , m.toss_winner
  , m.winner
  , m.participating_teams
  , i.runs_total                                                                             as innings_runs_total
  , i.wickets_fallen                                                                         as innings_wickets_fallen
  , i.recorded_over_count
    as innings_recorded_over_count
  , i.is_super_over                                                                          as innings_is_super_over
  , dense_rank() over (partition by bi.match_id, bi.batting_team order by bi.innings_number) as bowling_innings_rank
  , if(bi.batting_team = m.team_1, m.team_2, m.team_1)                                       as bowling_team
  , round(bi.legal_deliveries / 6.0, 2)                                                      as overs_bowled
  , round(bi.runs_conceded * 1.0 / nullif(bi.wickets, 0), 2)                                 as bowling_average
  , round(bi.runs_conceded * 6.0 / nullif(bi.legal_deliveries, 0), 2)                        as economy_rate
  , round(bi.legal_deliveries * 1.0 / nullif(bi.wickets, 0), 2)                              as bowling_strike_rate
from bowler_innings_stats as bi
left join matches as m
  on bi.match_id = m.match_id
left join match_innings_stats as i
  on
    bi.match_id = i.match_id
    and bi.innings_number = i.innings_number

-- TODO: add innings statistics like total runs, wickets, overs etc
