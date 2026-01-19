{{ config(materialized='table', tags=['intermediate', 'match_context']) }}

-- Calculate required run rate and match pressure at each delivery
-- Only relevant for chasing innings (2nd innings in limited overs)

with match_config as (
  select
    match_id
    , match_type
    , scheduled_overs
    , balls_per_over
  from {{ ref('int_cricket__matches_flattened') }}
)

, innings_context as (
  select
    match_id
    , innings_number
    , batting_team
    , target_runs
    , innings_context
  from {{ ref('int_cricket__innings_context') }}
  where innings_number >= 2  -- Only chasing innings
)

, deliveries_with_running_totals as (
  select
    d.match_id
    , d.innings_number
    , d.batting_team
    , d.delivery_idx
    , d.over_number
    , d.batter
    , d.non_striker
    , d.bowler
    , d.runs_total
    , d.runs_batter
    , d.is_wicket
    -- Running totals
    , sum(d.runs_total) over (
      partition by d.match_id, d.innings_number
      order by d.delivery_idx
      rows between unbounded preceding and current row
    ) as runs_so_far
    , count(*) over (
      partition by d.match_id, d.innings_number
      order by d.delivery_idx
      rows between unbounded preceding and current row
    ) as balls_so_far
    , count_if(d.is_wicket) over (
      partition by d.match_id, d.innings_number
      order by d.delivery_idx
      rows between unbounded preceding and current row
    ) as wickets_so_far
  from {{ ref('int_cricket__deliveries_flattened') }} as d
)

select
  dwrt.match_id
  , dwrt.innings_number
  , dwrt.batting_team
  , dwrt.delivery_idx
  , dwrt.over_number
  , dwrt.batter
  , dwrt.non_striker
  , dwrt.bowler
  , dwrt.runs_total
  , dwrt.runs_batter
  , dwrt.is_wicket
  , dwrt.runs_so_far
  , dwrt.balls_so_far
  , dwrt.wickets_so_far
  , ic.target_runs
  , ic.target_runs - dwrt.runs_so_far                            as runs_required
  , mc.scheduled_overs * mc.balls_per_over                       as total_balls_in_innings
  , (mc.scheduled_overs * mc.balls_per_over) - dwrt.balls_so_far as balls_remaining
  -- Required run rate: runs needed per over
  , round(
    (ic.target_runs - dwrt.runs_so_far) * 6.0
    / nullif((mc.scheduled_overs * mc.balls_per_over) - dwrt.balls_so_far, 0)
    , 2
  )                                                              as required_run_rate
  -- Current run rate
  , round(
    dwrt.runs_so_far * 6.0 / nullif(dwrt.balls_so_far, 0)
    , 2
  )                                                              as current_run_rate
  -- Pressure indicator: high when RRR much higher than current RR
  , case
    when ic.target_runs - dwrt.runs_so_far <= 0 then 'won'
    when dwrt.wickets_so_far >= 10 then 'all_out'
    when
      (ic.target_runs - dwrt.runs_so_far) * 6.0
      / nullif((mc.scheduled_overs * mc.balls_per_over) - dwrt.balls_so_far, 0) > 12 then 'extreme_pressure'
    when
      (ic.target_runs - dwrt.runs_so_far) * 6.0
      / nullif((mc.scheduled_overs * mc.balls_per_over) - dwrt.balls_so_far, 0) > 9 then 'high_pressure'
    when
      (ic.target_runs - dwrt.runs_so_far) * 6.0
      / nullif((mc.scheduled_overs * mc.balls_per_over) - dwrt.balls_so_far, 0) > 6 then 'moderate_pressure'
    else 'low_pressure'
  end                                                            as pressure_situation
from deliveries_with_running_totals as dwrt
left join innings_context as ic
  on
    dwrt.match_id = ic.match_id
    and dwrt.innings_number = ic.innings_number
left join match_config as mc
  on dwrt.match_id = mc.match_id
where ic.innings_context = 'chasing'  -- Only for chasing innings
