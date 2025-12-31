{% macro cricket_raw_json_glob() %}
    {# Prefer user override via var; use a repo-relative default glob #}
    {% set default_glob = 'data/raw/all_json/**/*.json' %}
    {{ return(var('raw_json_glob', default_glob)) }}
{% endmacro %}
