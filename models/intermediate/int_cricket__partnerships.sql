{{ config(materialized='table', tags=['intermediate', 'batting']) }}

-- Calculate partnerships between batters
-- A partnership ends when either batter gets out or innings ends

with deliveries_with_wickets as (
  select
    match_id
    , innings_number
    , over_idx
    , delivery_idx
    , batting_team
    , batter
    , non_striker
    , runs_batter
    , runs_extras
    , runs_total
    , is_wicket
    , wicket_player_out
    , wicket_kind
    -- Partnership ends when either current batter or non-striker gets out
    , case
      when is_wicket and wicket_player_out in (batter, non_striker) then 1
      else 0
    end as partnership_ending_wicket
  from {{ ref('int_cricket__deliveries_flattened') }}
)

, partnership_boundaries as (
  select
    *
    , coalesce(
      sum(partnership_ending_wicket)
        over (
          partition by match_id, innings_number
          order by over_idx, delivery_idx
          rows between unbounded preceding and 1 preceding
        )
      , 0
    ) + 1
      as partnership_number
  from deliveries_with_wickets
)

, partnership_base as (
  select
    match_id
    , innings_number
    , batting_team
    , partnership_number
    , min(delivery_idx)                                   as partnership_start_delivery
    , max(delivery_idx)                                   as partnership_end_delivery
    , sum(runs_total)                                     as partnership_runs
    , sum(runs_extras)                                    as partnership_extras
    , count(*)                                            as partnership_balls
    , max(is_wicket)                                      as partnership_ended_in_wicket
    , max(case when is_wicket then wicket_player_out end) as dismissed_batter
    , max(case when is_wicket then wicket_kind end)       as wicket_kind
  from partnership_boundaries
  group by 1, 2, 3, 4
)

, partnership_with_dismissal_order as (
  select
    *
    , case
      when wicket_kind is not null and wicket_kind not ilike '%retired%'
        then sum(case when wicket_kind is not null and wicket_kind not ilike '%retired%' then 1 else 0 end)
          over (
            partition by match_id, innings_number, batting_team
            order by partnership_number
            rows between unbounded preceding and current row
          )
    end as dismissal_order
  from partnership_base
)

, partnership_batters as (
  select
    match_id
    , innings_number
    , batting_team
    , partnership_number
    , mode(batter)      as batter_1
    , mode(non_striker) as batter_2
  from partnership_boundaries
  group by 1, 2, 3, 4
)

, partnership_batter_runs as (
  select
    match_id
    , innings_number
    , batting_team
    , partnership_number
    , batter           as player_name
    , sum(runs_batter) as batter_runs
  from partnership_boundaries
  group by 1, 2, 3, 4, 5
)

select
  base.match_id
  , base.innings_number
  , base.batting_team
  , base.partnership_number
  , batters.batter_1
  , batters.batter_2
  , base.partnership_runs
  , base.partnership_extras
  , base.partnership_balls
  , base.partnership_ended_in_wicket
  , base.dismissed_batter
  , base.wicket_kind
  , base.dismissal_order
  , base.partnership_start_delivery
  , base.partnership_end_delivery
  , coalesce(b1.batter_runs, 0)                                                 as batter_1_runs
  , coalesce(b2.batter_runs, 0)                                                 as batter_2_runs
  , round(base.partnership_runs * 100.0 / nullif(base.partnership_balls, 0), 2) as partnership_run_rate
from partnership_with_dismissal_order as base
left join partnership_batters as batters
  on
    base.match_id = batters.match_id
    and base.innings_number = batters.innings_number
    and base.batting_team = batters.batting_team
    and base.partnership_number = batters.partnership_number
left join partnership_batter_runs as b1
  on
    base.match_id = b1.match_id
    and base.innings_number = b1.innings_number
    and base.batting_team = b1.batting_team
    and base.partnership_number = b1.partnership_number
    and batters.batter_1 = b1.player_name
left join partnership_batter_runs as b2
  on
    base.match_id = b2.match_id
    and base.innings_number = b2.innings_number
    and base.batting_team = b2.batting_team
    and base.partnership_number = b2.partnership_number
    and batters.batter_2 = b2.player_name
