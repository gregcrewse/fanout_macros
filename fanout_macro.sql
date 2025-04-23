-- macros/simple_fanout_detector.sql

{% macro simple_fanout_detector(model_name, id_column) %}
/*
  The absolute simplest fanout detector possible. Just pass a model name and an ID column
  that should be unique, and it will tell you if there's fanout.
  
  This uses no loops, no complex logic, and is unlikely to cause recursion issues.
  
  Example usage:
    -- In an analysis file
    {{ simple_fanout_detector('my_model', 'customer_id') }}
*/

-- Get counts using ref() to resolve the proper model reference
{% set query %}
  select 
    count(*) as total_rows,
    count(distinct {{ id_column }}) as distinct_ids
  from {{ ref(model_name) }}
{% endset %}

-- Execute the query and get results
{% set results = run_query(query) %}
{% set total_rows = results.columns[0].values()[0] %}
{% set distinct_ids = results.columns[1].values()[0] %}

-- Calculate the fanout ratio (safely)
{% set fanout_ratio = 0 %}
{% if distinct_ids > 0 %}
  {% set fanout_ratio = total_rows / distinct_ids %}
{% endif %}

-- Log the results
{{ log("=== Fanout Analysis for " ~ model_name ~ " ===", info=true) }}
{{ log("Total rows: " ~ total_rows, info=true) }}
{{ log("Distinct " ~ id_column ~ " values: " ~ distinct_ids, info=true) }}
{{ log("Fanout ratio: " ~ fanout_ratio, info=true) }}

{% if fanout_ratio > 1 %}
  {{ log("⚠️  FANOUT DETECTED - Each " ~ id_column ~ " appears on average " ~ fanout_ratio ~ " times", info=true) }}
  
  -- Show some examples of duplicated IDs
  {% set dupes_query %}
    select 
      {{ id_column }},
      count(*) as occurrences
    from {{ ref(model_name) }}
    group by {{ id_column }}
    having count(*) > 1
    order by count(*) desc
    limit 5
  {% endset %}
  
  {{ log("Top duplicated values:", info=true) }}
  {% set dupes = run_query(dupes_query) %}
  {% do dupes.print_table() %}
  
{% else %}
  {{ log("✅ NO FANOUT - Each " ~ id_column ~ " appears exactly once", info=true) }}
{% endif %}

{% endmacro %}
