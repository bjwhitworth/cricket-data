{{ config(materialized='table', tags=['intermediate', 'batting']) }}

-- Calculate partnerships between batters
-- A partnership ends when either batter gets out or innings ends

with deliveries_with_wickets as (
  select
    match_id,
    innings_number,
    over_idx,
    delivery_idx,
    batting_team,
    batter,
    non_striker,
    runs_total,
    is_wicket,
    wicket_player_out,
    -- Partnership ends when either current batter or non-striker gets out
    case
      when is_wicket and wicket_player_out in (batter, non_striker) then 1
      else 0
    end as partnership_ending_wicket
  from {{ ref('int_cricket__deliveries_flattened') }}
),

partnership_boundaries as (
  select
    *,
    coalesce(
      sum(partnership_ending_wicket) over (
        partition by match_id, innings_number
        order by over_idx, delivery_idx
        rows between unbounded preceding and 1 preceding
      ),
      0
    ) + 1 
    as partnership_number
  from deliveries_with_wickets
),

partnerships as (
  select
    match_id,
    innings_number,
    batting_team,
    partnership_number,
    min(delivery_idx) as partnership_start_delivery,
    max(delivery_idx) as partnership_end_delivery,
    -- Get the two batters involved (taking the most common pairing in this partnership)
    mode(batter) as batter_1,
    mode(non_striker) as batter_2,
    sum(runs_total) as partnership_runs,
    count(*) as partnership_balls,
    max(is_wicket) as partnership_ended_in_wicket,
    max(case when is_wicket then wicket_player_out end) as dismissed_batter
  from partnership_boundaries
  group by 1, 2, 3, 4
)

select
  match_id,
  innings_number,
  batting_team,
  partnership_number,
  batter_1,
  batter_2,
  partnership_runs,
  partnership_balls,
  round(partnership_runs * 100.0 / nullif(partnership_balls, 0), 2) as partnership_run_rate,
  partnership_ended_in_wicket,
  dismissed_batter,
  partnership_start_delivery,
  partnership_end_delivery
from partnerships
