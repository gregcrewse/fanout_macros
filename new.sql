-- tests/generic/test_detect_fanout.sql
{% test detect_fanout(model, group_by_columns, max_fanout_ratio=1.05) %}
/*
  Simple test that checks if a model has fanout based on specified group by columns.
  
  Arguments:
      model: The model to test
      group_by_columns: List of columns that should uniquely identify rows (the primary key)
      max_fanout_ratio: Maximum allowed ratio of (total rows / distinct grouped rows)
                         Default is 1.05 (allowing 5% overhead)
  
  Example usage in schema.yml:
      models:
        - name: my_model
          tests:
            - detect_fanout:
                group_by_columns: ['customer_id']  # Or ['account_id', 'case_id'] etc.
                max_fanout_ratio: 1.0  # Strict: no duplicates allowed
*/

{# Create comma-separated list of group_by_columns #}
{% set columns_csv = group_by_columns | join(', ') %}

with 
-- Count total rows in the model
total_row_count as (
    select 
        count(*) as total_rows
    from {{ model }}
),

-- Count distinct combinations of the group_by_columns
distinct_group_count as (
    select
        count(*) as distinct_groups
    from (
        select
            {% for column in group_by_columns %}
                {{ column }}{% if not loop.last %},{% endif %}
            {% endfor %}
        from {{ model }}
        group by 
            {% for column in group_by_columns %}
                {{ column }}{% if not loop.last %},{% endif %}
            {% endfor %}
    ) as distinct_groups
),

-- Calculate the fanout ratio
fanout_analysis as (
    select
        t.total_rows,
        d.distinct_groups,
        case 
            when d.distinct_groups = 0 then 0
            else cast(t.total_rows as float) / cast(d.distinct_groups as float)
        end as fanout_ratio,
        {{ max_fanout_ratio }} as max_allowed_ratio
    from total_row_count t
    cross join distinct_group_count d
)

-- Return failing rows if fanout detected
select
    'Fanout detected' as failure_reason,
    total_rows as row_count,
    distinct_groups as unique_combinations,
    fanout_ratio,
    max_allowed_ratio,
    (fanout_ratio - max_allowed_ratio) as excess_ratio
from fanout_analysis
where fanout_ratio > max_allowed_ratio

{% endtest %}
