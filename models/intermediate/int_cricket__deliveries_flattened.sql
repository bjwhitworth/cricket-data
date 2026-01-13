{{ config(materialized='view', tags=['intermediate']) }}

with overs as (
  select
    match_id
    , ingested_at
    , innings_number
    , batting_team
    , over_idx
    , over_struct
  from {{ ref('int_cricket__overs_flattened') }}
)

, deliveries_with_wickets as (
  select
    overs.match_id
    , overs.ingested_at
    , overs.innings_number
    , overs.batting_team
    , d.delivery_idx
    , overs.over_idx
    , trim(both '"' from delivery_struct.batter::varchar)       as batter
    , trim(both '"' from delivery_struct.non_striker::varchar)  as non_striker
    , trim(both '"' from delivery_struct.bowler::varchar)       as bowler
    , try_cast(coalesce(overs.over_struct.over, overs.over_idx - 1) as integer)
      as over_number
    , try_cast(coalesce(delivery_struct.runs.batter, 0) as integer)
      as runs_batter
    , try_cast(coalesce(delivery_struct.runs.extras, 0) as integer)
      as runs_extras
    , try_cast(coalesce(delivery_struct.runs.total, 0) as integer)
      as runs_total
    , try_cast(coalesce(delivery_struct.extras.byes, 0) as integer)
      as extras_byes
    , try_cast(coalesce(delivery_struct.extras.legbyes, 0) as integer)
      as extras_legbyes
    , try_cast(coalesce(delivery_struct.extras.noballs, 0) as integer)
      as extras_noballs
    , try_cast(coalesce(delivery_struct.extras.penalty, 0) as integer)
      as extras_penalty
    , try_cast(coalesce(delivery_struct.extras.wides, 0) as integer)
      as extras_wides
    , delivery_struct.wickets[
      0
    ]
      as wicket_struct_0
    , coalesce(
      delivery_struct.wickets is not null and len(delivery_struct.wickets) > 0, false
    )
      as is_wicket
    , try_cast(delivery_struct.wickets[0] as struct(player_out varchar, kind varchar, fielders struct(name varchar) []))
      as wicket_struct
  from overs
  cross join unnest(overs.over_struct.deliveries) with ordinality as d (delivery_struct, delivery_idx)
)

select
  deliveries_with_wickets.*
  , wicket_struct.player_out::varchar       as wicket_player_out
  , wicket_struct.kind::varchar             as wicket_kind
  , wicket_struct.fielders[1].name::varchar as wicket_fielder_1
  , wicket_struct.fielders[2].name::varchar as wicket_fielder_2
from deliveries_with_wickets
