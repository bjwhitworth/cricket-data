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
    , d.is_miscounted_over_from_data
    , sum(d.runs_total) over (
      partition by d.match_id, d.innings_number
      order by d.over_number, d.delivery_idx
      rows between unbounded preceding and current row
    )                                                                         as runs_so_far
    , count(*) over (
      partition by d.match_id, d.innings_number
      order by d.over_number, d.delivery_idx
      rows between unbounded preceding and current row
    )                                                                         as balls_so_far
    -- Cumulative count of wides and no balls (which count as additional deliveries)
    , count_if(d.is_wicket) over (
      partition by d.match_id, d.innings_number
      order by d.over_number, d.delivery_idx
      rows between unbounded preceding and current row
    )                                                                         as wickets_so_far
    -- Legal delivery indicator (excludes wides and no balls)
    , count_if(d.extras_wides > 0 or d.extras_noballs > 0) over (
      partition by d.match_id, d.innings_number
      order by d.over_number, d.delivery_idx
      rows between unbounded preceding and current row
    )                                                                         as extras_deliveries_so_far
    , (coalesce(d.extras_wides, 0) = 0 and coalesce(d.extras_noballs, 0) = 0) as is_legal_delivery
    -- Sequential count of legal deliveries within the over
    , sum(case when coalesce(d.extras_wides, 0) = 0 and coalesce(d.extras_noballs, 0) = 0 then 1 else 0 end) over (
      partition by d.match_id, d.innings_number, d.over_number
      order by d.delivery_idx
      rows between unbounded preceding and current row
    )                                                                         as legal_delivery_seq_in_over
    -- Total legal deliveries in this over
    , sum(case when coalesce(d.extras_wides, 0) = 0 and coalesce(d.extras_noballs, 0) = 0 then 1 else 0 end) over (
      partition by d.match_id, d.innings_number, d.over_number
    )                                                                         as legal_deliveries_in_over_total
    -- Total deliveries in this over (including wides/no balls)
    , count(*) over (
      partition by d.match_id, d.innings_number, d.over_number
    )                                                                         as total_deliveries_in_over
    -- Cumulative count of legal deliveries (excluding wides and no balls)
    , count_if(coalesce(d.extras_wides, 0) = 0 and coalesce(d.extras_noballs, 0) = 0) over (
      partition by d.match_id, d.innings_number
      order by d.over_number, d.delivery_idx
      rows between unbounded preceding and current row
    )                                                                         as legal_deliveries_so_far
    -- Sequential delivery number in the innings
    , row_number() over (
      partition by d.match_id, d.innings_number
      order by d.over_number, d.delivery_idx
    )                                                                         as delivery_number_in_innings
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
  , dwrt.extras_deliveries_so_far
  , dwrt.legal_deliveries_so_far
  , dwrt.is_legal_delivery
  , dwrt.legal_delivery_seq_in_over
  , dwrt.legal_deliveries_in_over_total
  , dwrt.total_deliveries_in_over
  , ic.target_runs
  , mc.scheduled_overs
  , mc.balls_per_over
  , dwrt.is_miscounted_over_from_data
  , ic.target_runs - dwrt.runs_so_far                                                as runs_required
  , mc.scheduled_overs
  * mc.balls_per_over
    as total_balls_in_innings
  , (mc.scheduled_overs * mc.balls_per_over) - dwrt.legal_deliveries_so_far          as balls_remaining
  , dwrt.delivery_number_in_innings = max(dwrt.delivery_number_in_innings)
    over (
      partition by dwrt.match_id, dwrt.innings_number
    )
    as is_last_delivery_of_innings
  -- Miscount flags: from raw data and computed at over-level
  , dwrt.runs_so_far >= ic.target_runs                                               as target_reached
  , case
    when max(target_reached) over (partition by dwrt.match_id, dwrt.innings_number, dwrt.over_number) = 1
      then false
    else (dwrt.legal_deliveries_in_over_total <> mc.balls_per_over)
  end                                                                                as is_miscounted_over_computed
  , (dwrt.is_legal_delivery and dwrt.legal_delivery_seq_in_over > mc.balls_per_over) as is_miscounted_delivery_computed
  -- Verify: data flag matches computed
  , dwrt.is_miscounted_over_from_data = is_miscounted_over_computed                  as miscount_check_passed
  -- Required run rate: runs needed per over
  , round(
    (ic.target_runs - dwrt.runs_so_far) * 6.0
    / nullif((mc.scheduled_overs * mc.balls_per_over) - dwrt.legal_deliveries_so_far, 0)
    , 2
  )                                                                                  as required_run_rate
  -- Current run rate
  , round(
    dwrt.runs_so_far * 6.0 / nullif(dwrt.balls_so_far, 0)
    , 2
  )                                                                                  as current_run_rate
from deliveries_with_running_totals as dwrt
left join innings_context as ic
  on
    dwrt.match_id = ic.match_id
    and dwrt.innings_number = ic.innings_number
left join match_config as mc
  on dwrt.match_id = mc.match_id
where ic.innings_context = 'chasing'  -- Only for chasing innings
