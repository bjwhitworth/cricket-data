{{ config(materialized='table', tags=['marts', 'dimension']) }}

with match_registry as (
  select
    match_id
    , player_registry
  from {{ ref('int_cricket__matches_flattened') }}
)

, player_list as (
  select distinct
    mr.match_id
    , e.entry.key   as player_key
    , e.entry.value as player_name
  from match_registry as mr
  cross join unnest(map_entries(mr.player_registry)) as e (entry)
  where typeof(mr.player_registry) = 'MAP'
)

, batting_appearances as (
  select distinct
    batter         as player_name
    , batting_team as team
  from {{ ref('int_cricket__deliveries_flattened') }}
  where batter is not null
)

, bowling_appearances as (
  select distinct bowler as player_name
  from {{ ref('int_cricket__deliveries_flattened') }}
  where bowler is not null
)

, all_players as (
  select distinct player_name
  from batting_appearances
  union
  select distinct player_name
  from bowling_appearances
  union
  select distinct player_name
  from player_list
)

, player_teams as (
  select
    player_name
    , array_agg(distinct team) as teams_played_for
  from batting_appearances
  group by player_name
)

select
  ap.player_name
  , pt.teams_played_for
  , coalesce(exists (
    select 1 from batting_appearances
    where player_name = ap.player_name
  ), false) as has_batted
  , coalesce(exists (
    select 1 from bowling_appearances
    where player_name = ap.player_name
  ), false) as has_bowled
from all_players as ap
left join player_teams as pt
  on ap.player_name = pt.player_name
