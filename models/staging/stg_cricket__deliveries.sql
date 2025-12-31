{{ config(materialized='view', tags=['stage']) }}

select
  d.match_id
  , d.innings_number
  , d.over_number
  , d.batting_team
  , d.batter
  , d.non_striker
  , d.bowler
  , d.wicket_player_out
  , d.wicket_kind
  , d.wicket_fielder_1
  , d.wicket_fielder_2
  , d.ingested_at
  , d.runs_batter
  , d.runs_extras
  , d.runs_total
  , d.extras_byes
  , d.extras_legbyes
  , d.extras_noballs
  , d.extras_penalty
  , d.extras_wides
  , d.is_wicket
  , try_cast(d.delivery_idx as integer) as ball_in_over
from {{ ref('int_cricket__deliveries_flattened') }} as d
