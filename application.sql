
### re_data

Use re_data's filtering macros:

```sql
-- models/no_duplicates.sql
SELECT 
  customer_id, 
  name,
  email 
FROM {{ re_data.filter_remove_duplicates(
  ref('users'), 
  ['customer_id'],
  'created_at DESC'
) }}

-- tests/generic/test_no_fanout.sql
{% test no_fanout(model, compare_model, join_columns) %}

{% set join_cols_csv = join_columns|join(', ') %}

with base_count as (
    select count(*) as row_count
    from {{ compare_model }}
),
model_count as (
    select count(*) as row_count
    from {{ model }}
)

select
    'Potential fanout detected' as error_message,
    m.row_count as model_rows,
    b.row_count as compare_model_rows,
    (m.row_count - b.row_count) as difference
from model_count m
cross join base_count b
where m.row_count > b.row_count

{% endtest %}


-- macros/log_row_count.sql
{% macro log_row_count(model_name) %}
  {% set query %}
    SELECT COUNT(*) FROM {{ ref(model_name) }}
  {% endset %}
  
  {% set count = run_query(query).columns[0][0] %}
  {% do log("Row count for " ~ model_name ~ ": " ~ count, info=true) %}
{% endmacro %}

-- In your model:
{{ log_row_count('upstream_model') }}
-- Your model SQL here...
-- Check after processing:
{{ log_row_count('this') }}
