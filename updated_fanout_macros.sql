-- macros/salesforce_fanout_analysis.sql
{% macro analyze_case_fanout(model, group_by_columns, count_threshold=1) %}

{% set group_by_cols_csv = group_by_columns|join(', ') %}

{% set query %}
with base_count as (
    select count(*) as total_row_count
    from {{ model }}
),
group_counts as (
    select 
        {{ group_by_cols_csv }},
        count(*) as group_row_count
    from {{ model }}
    group by {{ group_by_cols_csv }}
),
fanout_groups as (
    select
        {{ group_by_cols_csv }},
        group_row_count,
        round((group_row_count * 100.0) / (select total_row_count from base_count), 2) as percent_of_total
    from group_counts
    where group_row_count > {{ count_threshold }}
    order by group_row_count desc
)

select * from fanout_groups
{% endset %}

{% do log("Analyzing potential fanout in " ~ model ~ " grouped by [" ~ group_by_cols_csv ~ "]", info=true) %}
{% set results = run_query(query) %}
{% do results.print_table() %}

{% endmacro %}

-- macros/salesforce_fanout_detection_suite.sql
{% macro run_salesforce_fanout_detection_suite(models_dict) %}
    {% for model_name, config in models_dict.items() %}
        {% do log("=== Running Salesforce fanout detection for " ~ model_name ~ " ===", info=true) %}
        
        {% if config.base_model %}
            {% set base_model = config.base_model %}
            {% set model = ref(model_name) %}
            
            {% set base_count_query %}
                select count(*) from {{ base_model }}
            {% endset %}
            
            {% set model_count_query %}
                select count(*) from {{ model }}
            {% endset %}
            
            {% set base_count = run_query(base_count_query).columns[0][0] %}
            {% set model_count = run_query(model_count_query).columns[0][0] %}
            
            {% do log("Base model: " ~ base_model ~ " - " ~ base_count ~ " rows", info=true) %}
            {% do log("Target model: " ~ model ~ " - " ~ model_count ~ " rows", info=true) %}
            
            {% if model_count > base_count %}
                {% do log("⚠️ FANOUT DETECTED: Row count increased by " ~ (model_count - base_count) ~ " rows", info=true) %}
                
                {% if config.group_by_columns %}
                    {% do analyze_case_fanout(model, config.group_by_columns) %}
                {% endif %}
            {% else %}
                {% do log("✓ NO FANOUT DETECTED: Row counts look good", info=true) %}
            {% endif %}
        {% endif %}
        
        {% do log("", info=true) %}
    {% endfor %}
{% endmacro %}

-- Example invocation (in an analysis file or hook):
-- 
-- {{ 
--   run_salesforce_fanout_detection_suite({
--     'account_cases': {
--       'base_model': ref('sf_accounts'),
--       'group_by_columns': ['account_id']
--     },
--     'account_case_comments': {
--       'base_model': ref('sf_cases'),
--       'group_by_columns': ['case_id']
--     }
--   }) 
-- }}

-- macro/salesforce_case_metrics.sql
{% macro analyze_case_comment_distribution(case_model, comment_model) %}
    {% set query %}
        with case_comment_counts as (
            select 
                c.case_id,
                c.subject,
                c.status,
                count(cc.comment_id) as comment_count
            from {{ case_model }} c
            left join {{ comment_model }} cc on c.case_id = cc.case_id
            group by c.case_id, c.subject, c.status
        ),
        comment_distribution as (
            select
                comment_count,
                count(*) as case_count,
                round((count(*) * 100.0) / (select count(*) from case_comment_counts), 2) as percent_of_total
            from case_comment_counts
            group by comment_count
            order by comment_count
        )
        
        select * from comment_distribution
    {% endset %}
    
    {% do log("Analyzing comment distribution across cases", info=true) %}
    {% set results = run_query(query) %}
    {% do results.print_table() %}
{% endmacro %}
