{{ config(materialized='table', tags=['intermediate', 'match_context']) }}

-- Calculate match situation and context for each innings
-- Handles both Limited Overs (2 innings) and Test cricket (4+ innings)
-- For Test: Team A innings 1 & 3, Team B innings 2 & 4
-- For Limited Overs: Team A innings 1, Team B innings 2 (chasing)

with match_info as (
  select
    match_id,
    match_type,
    team_1,
    team_2
  from {{ ref('int_cricket__matches_flattened') }}
),
innings_totals as (
  select
    match_id,
    innings_number,
    batting_team,
    sum(runs_total) as innings_total,
    count(distinct case when is_wicket then wicket_player_out end) as wickets_lost
  from {{ ref('int_cricket__deliveries_flattened') }}
  group by 1, 2, 3
),
innings_with_previous as (
  select
    i.match_id,
    i.innings_number,
    i.batting_team,
    i.innings_total,
    i.wickets_lost,
    m.match_type,
    lag(i.innings_total) over (
      partition by i.match_id
      order by i.innings_number
    ) as previous_innings_total,
    lag(i.batting_team) over (
      partition by i.match_id
      order by i.innings_number
    ) as previous_batting_team
  from innings_totals i
  left join match_info m
    on i.match_id = m.match_id
),

-- Assign team innings sequence (1st inning for team, 2nd inning for team, etc.)
innings_with_team_sequence as (
  select
    iwp.match_id,
    iwp.innings_number,
    iwp.batting_team,
    iwp.innings_total,
    iwp.wickets_lost,
    iwp.match_type,
    iwp.previous_innings_total,
    iwp.previous_batting_team,
    -- Rank this team's innings
    row_number() over (
      partition by iwp.match_id, iwp.batting_team
      order by iwp.innings_number
    ) as team_innings_number
  from innings_with_previous iwp
),
-- Determine if this innings is chasing or batting first
innings_with_context as (
  select
    iwts.match_id,
    iwts.innings_number,
    iwts.batting_team,
    iwts.innings_total,
    iwts.wickets_lost,
    iwts.team_innings_number,
    iwts.match_type,
    case
      when iwts.team_innings_number = 1 then 'batting_first'
      when iwts.match_type in ('ODI', 'T20') and iwts.team_innings_number = 2 then 'chasing'
      when iwts.match_type = 'Test' and iwts.team_innings_number = 2 then 'chasing'
      when iwts.match_type = 'Test' and iwts.team_innings_number >= 3 then 'chasing'
      else 'batting_again'
    end as innings_context,
    -- Target is the opponent's most recent innings total + 1
    case
      when iwts.team_innings_number > 1 then iwts.previous_innings_total + 1
      else null
    end as target_runs
  from innings_with_team_sequence iwts
),

match_outcomes as (
  select
    match_id,
    winner,
    result_type
  from {{ ref('int_cricket__matches_flattened') }}
)

select
  iwc.match_id,
  iwc.innings_number,
  iwc.batting_team,
  iwc.innings_total,
  iwc.wickets_lost,
  iwc.match_type,
  iwc.team_innings_number,
  iwc.innings_context,
  iwc.target_runs,
  case
    when iwc.target_runs is null then null
    when iwc.innings_total >= iwc.target_runs then 0  -- Target met or exceeded
    else iwc.target_runs - iwc.innings_total
  end as runs_short_of_target,
  case
    when iwc.innings_context in ('chasing', 'batting_again') and iwc.innings_total >= iwc.target_runs then true
    when iwc.innings_context in ('chasing', 'batting_again') and iwc.innings_total < iwc.target_runs then false
    else null
  end as successfully_chased,
  mo.winner,
  mo.result_type,
  case when iwc.batting_team = mo.winner then true else false end as innings_team_won
from innings_with_context iwc
left join match_outcomes mo
  on iwc.match_id = mo.match_id
