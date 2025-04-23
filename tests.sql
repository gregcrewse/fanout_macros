-- tests/test_salesforce_case_fanout.sql
with account_count as (
    select count(*) as account_count
    from {{ ref('sf_accounts') }}
),
case_count as (
    select count(*) as case_count
    from {{ ref('sf_cases') }}
),
account_cases_count as (
    select count(*) as model_count
    from {{ ref('account_cases') }}
),
account_case_comments_count as (
    select count(*) as model_count
    from {{ ref('account_case_comments') }}
)

-- Test if account_cases has more rows than expected
select 
    'account_cases has unexpected fanout' as issue,
    a.account_count,
    c.case_count,
    ac.model_count
from account_count a
cross join case_count c
cross join account_cases_count ac
where ac.model_count > c.case_count

union all

-- Test if account_case_comments has more rows than expected (we expect fanout here)
select 
    'account_case_comments has expected fanout' as issue,
    a.account_count,
    c.case_count,
    acc.model_count
from account_count a
cross join case_count c
cross join account_case_comments_count acc
where acc.model_count > c.case_count

-- macros/test_sf_fanout.sql
{% macro test_sf_fanout(model, base_model, join_key) %}

{% set model_query %}
    select count(*) as row_count from {{ model }}
{% endset %}

{% set base_model_query %}
    select count(*) as row_count from {{ base_model }}
{% endset %}

{% set model_count = run_query(model_query).columns[0].values()[0] %}
{% set base_model_count = run_query(base_model_query).columns[0].values()[0] %}

{% if model_count > base_model_count %}
    {{ log("FANOUT DETECTED: " ~ model ~ " has " ~ model_count ~ " rows while " ~ base_model ~ " has " ~ base_model_count ~ " rows", info=true) }}
    
    -- Return records that indicate the test failed
    select 
        '{{ join_key }}' as join_key,
        {{ model_count }} as model_count,
        {{ base_model_count }} as base_model_count,
        'Fanout detected' as issue
    
{% else %}
    -- Return empty result to indicate test passed
    select null as join_key where 1=0
    
{% endif %}

{% endmacro %}

-- tests/generic/test_no_case_fanout.sql
{% test no_case_fanout(model, base_model, join_key) %}

with base_count as (
    select count(*) as base_count
    from {{ base_model }}
),
model_count as (
    select count(*) as model_count
    from {{ model }}
)

select
    'Fanout detected' as issue,
    b.base_count,
    m.model_count,
    (m.model_count - b.base_count) as row_difference
from base_count b
cross join model_count m
where m.model_count > b.base_count

{% endtest %}

-- Example usage in a schema.yml file:
version: 2

models:
  - name: account_cases
    tests:
      - no_case_fanout:
          base_model: ref('sf_cases')
          join_key: case_id
      
  - name: account_case_comments
    tests:
      - no_case_fanout:
          base_model: ref('sf_cases')
          join_key: case_id
