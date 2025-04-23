-- macros/analyze_fanout.sql

{% macro analyze_fanout(model_name, group_by_columns) %}
/*
  Simple macro that analyzes and logs potential fanout in a model.
  This is a diagnostic tool that can be run manually or integrated into your dbt runs.
  
  Arguments:
      model_name: The name of the model to analyze (will be passed to ref())
      group_by_columns: List of column names that should form a unique key
  
  Example usage:
      
      -- In a one-off analysis file:
      {{ analyze_fanout('my_model', ['account_id', 'case_id']) }}
      
      -- Or in a post-hook:
      {{ config(
          post_hook="{{ analyze_fanout(this.name, ['account_id']) }}"
      ) }}
*/

{% set model_ref = ref(model_name) %}
{% set columns_csv = group_by_columns | join(', ') %}

{% set query %}
  with total_count as (
    select count(*) as total_rows from {{ model_ref }}
  ),
  distinct_count as (
    select count(*) as distinct_groups
    from (
      select 
        {{ columns_csv }}
      from {{ model_ref }}
      group by {{ columns_csv }}
    ) as unique_groups
  ),
  duplicate_groups as (
    select 
      {{ columns_csv }},
      count(*) as row_count
    from {{ model_ref }}
    group by {{ columns_csv }}
    having count(*) > 1
    order by count(*) desc
    limit 5
  )
  
  select
    t.total_rows,
    d.distinct_groups,
    case 
      when d.distinct_groups = 0 then 0
      else cast(t.total_rows as float) / cast(d.distinct_groups as float)
    end as fanout_ratio
  from total_count t
  cross join distinct_count d
{% endset %}

{% set results = run_query(query) %}
{% set total_rows = results.columns[0].values()[0] %}
{% set distinct_groups = results.columns[1].values()[0] %}
{% set fanout_ratio = results.columns[2].values()[0] %}

{% if fanout_ratio > 1.001 %}
  {% set fanout_pct = ((fanout_ratio - 1) * 100) | round(2) %}
  
  {{ log("⚠️  FANOUT DETECTED in " ~ model_name ~ ":", info=true) }}
  {{ log("   - Total rows: " ~ total_rows, info=true) }}
  {{ log("   - Distinct " ~ group_by_columns | join('/') ~ " combinations: " ~ distinct_groups, info=true) }}
  {{ log("   - Fanout ratio: " ~ fanout_ratio ~ " (" ~ fanout_pct ~ "% overhead)", info=true) }}
  
  -- If fanout detected, find examples of duplicated groups
  {% set dupes_query %}
    select 
      {{ columns_csv }},
      count(*) as row_count
    from {{ model_ref }}
    group by {{ columns_csv }}
    having count(*) > 1
    order by count(*) desc
    limit 5
  {% endset %}
  
  {{ log("   - Sample duplicated groups:", info=true) }}
  {% set dupes_results = run_query(dupes_query) %}
  {% do dupes_results.print_table() %}

{% else %}
  {{ log("✅ No fanout detected in " ~ model_name ~ " based on " ~ group_by_columns | join('/'), info=true) }}
{% endif %}

{% endmacro %}
