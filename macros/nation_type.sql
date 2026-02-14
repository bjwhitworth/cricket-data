{%- macro get_nation_type(team_name) -%}
{#- 
  Returns the nation classification for a cricket team.
  Returns 'Full' for full ICC member nations, 'Associate' for associate members, 
  or 'Unknown' if classification cannot be determined.
-#}
  case lower(trim(coalesce({{ team_name }}, '')))
    when 'afghanistan' then 'Full'
    when 'australia' then 'Full'
    when 'bangladesh' then 'Full'
    when 'england' then 'Full'
    when 'india' then 'Full'
    when 'ireland' then 'Full'
    when 'new zealand' then 'Full'
    when 'pakistan' then 'Full'
    when 'south africa' then 'Full'
    when 'sri lanka' then 'Full'
    when 'west indies' then 'Full'
    when 'zimbabwe' then 'Full'
    -- Associate nations (expanded list)
    when 'oman' then 'Associate'
    when 'united arab emirates' then 'Associate'
    when 'uae' then 'Associate'
    when 'netherlands' then 'Associate'
    when 'namibia' then 'Associate'
    when 'papua new guinea' then 'Associate'
    when 'png' then 'Associate'
    when 'scotland' then 'Associate'
    when 'kenya' then 'Associate'
    when 'uganda' then 'Associate'
    when 'nepal' then 'Associate'
    when 'hong kong' then 'Associate'
    when 'romania' then 'Associate'
    when 'canada' then 'Associate'
    when 'suriname' then 'Associate'
    when 'bermuda' then 'Associate'
    when 'cayman islands' then 'Associate'
    when 'singapore' then 'Associate'
    when 'thailand' then 'Associate'
    when 'japan' then 'Associate'
    when 'malaysia' then 'Associate'
    when 'indonesia' then 'Associate'
    when 'italy' then 'Associate'
    when 'france' then 'Associate'
    when 'mexico' then 'Associate'
    when 'argentina' then 'Associate'
    when 'chile' then 'Associate'
    else 'Unknown'
  end
{%- endmacro -%}
