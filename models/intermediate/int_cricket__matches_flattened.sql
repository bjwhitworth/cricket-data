{{ config(materialized='view', tags=['intermediate']) }}

with base as (
  select
    match_id
    , source_file_path
    , meta_json
    , info_json
    , meta
    , info
    , ingested_at
    , try_cast(info.event as struct(name varchar, match_number integer, stage varchar, "group" varchar))
      as event_struct
    , try_cast(info.officials as struct(umpires varchar [], match_referees varchar [], tv_umpires varchar []))
      as officials_struct
    , try_cast(info.outcome as struct(
      winner varchar
      , result varchar
      , eliminator varchar
      , method varchar
      , bowl_out varchar
      , "by" struct(runs integer, wickets integer, innings integer)
    ))
      as outcome_struct
  from {{ ref('stg_cricket__raw_json') }}
)

, match_venues as (
  select
    match_id
    , venue_id
    , venue
    , venue_from_source
    , city_from_source
    , city_mapped_or_source
    , match_country
  from {{ ref('int_cricket__match_venues') }}
)

, super_over_rounds as (
  select
    match_id
    , count(*)                                  as super_over_innings
    , try_cast(ceil(count(*) / 2.0) as integer) as super_over_rounds
  from {{ ref('int_cricket__innings_flattened') }}
  where is_super_over = true
  group by match_id
)

select
  base.match_id
  , base.source_file_path
  , base.meta_json
  , base.info_json
  , meta.data_version
  , info.match_type
  , info.team_type
  , info.gender
  , event_struct.name                                                                                  as event_name
  , event_struct.match_number
    as event_match_number
  , event_struct.stage                                                                                 as event_stage
  , event_struct."group"                                                                               as event_group
  , mv.city_from_source
  , mv.city_mapped_or_source
  , mv.venue_from_source
  , mv.venue_id
  , mv.venue
  , mv.match_country
  , info.toss.winner                                                                                   as toss_winner
  , info.toss.decision                                                                                 as toss_decision
  , outcome_struct.winner
    as winner_after_regulation_time
  , outcome_struct.eliminator
    as winner_after_eliminator
  , outcome_struct.result
    as result_text_if_no_winner
  , outcome_struct.by.runs                                                                             as win_by_runs
  , outcome_struct.by.wickets                                                                          as win_by_wickets
  , outcome_struct.by.innings                                                                          as win_by_innings
  , outcome_struct.method                                                                              as outcome_method
  , outcome_struct.bowl_out
  , info.player_of_match
    as players_of_match
  , info.teams
    as participating_teams
  , officials_struct.umpires
  , officials_struct.match_referees
  , officials_struct.tv_umpires
  , info.players
    as players_by_team
  , info.registry.people
    as player_registry
  , base.ingested_at
  , replace(info.season::varchar, '"', '')                                                             as season
  , coalesce(sor.super_over_rounds, 0)
    as super_over_rounds
  , if(outcome_struct.eliminator is not null, outcome_struct.eliminator, outcome_struct.winner)        as winner
  , case
    when outcome_struct.eliminator is not null
      then 'tie_then_super_over(s)'
    when outcome_struct.by.innings is not null
      then 'by_innings'
    when outcome_struct.by.wickets is not null
      then 'by_wickets'
    when outcome_struct.by.runs is not null
      then 'by_runs'
    when result_text_if_no_winner is not null
      then result_text_if_no_winner
    else 'N/A'
  end                                                                                                  as result_type
  , case
    when outcome_struct.eliminator is not null
      then 'Super over' || case
        when coalesce(sor.super_over_rounds, 0) > 0
          then
            ' (' || sor.super_over_rounds || ' round' || case when sor.super_over_rounds = 1 then '' else 's' end || ')'
        else ''
      end
    when outcome_struct.by.innings is not null
      then 'innings' || ' and ' || outcome_struct.by.runs || ' runs'
    when outcome_struct.by.wickets is not null
      then outcome_struct.by.wickets || ' wickets'
    when outcome_struct.by.runs is not null
      then outcome_struct.by.runs || ' runs'
    when result_text_if_no_winner is not null
      then result_text_if_no_winner
    else 'N/A'
  end
    as result_description
  , try_cast(info.match_type_number as integer)
    as match_type_number
  , try_cast(list_extract(info.dates, 1) as date)
    as match_start_date
  , try_cast(coalesce(list_extract(info.dates, len(info.dates)), list_extract(info.dates, 1)) as date) as match_end_date
  , info.teams[1]                                                                                      as team_1
  , info.teams[2]                                                                                      as team_2
  , {{ get_nation_type('info.teams[1]') }}                                                             as nation_type_team_1
  , {{ get_nation_type('info.teams[2]') }}                                                             as nation_type_team_2
  , try_cast(meta.revision as integer)                                                                 as data_revision
  , try_cast(meta.created as date)
    as data_created_date
  , try_cast(info.overs as integer)
    as scheduled_overs
  , try_cast(info.balls_per_over as integer)                                                           as balls_per_over
from base
left join super_over_rounds as sor
  on base.match_id = sor.match_id
left join match_venues as mv
  on base.match_id = mv.match_id

-- TODO: Add match length in days
