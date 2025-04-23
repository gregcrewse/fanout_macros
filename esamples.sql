-- Example schema.yml showing how to use the detect_fanout test
version: 2

models:
  - name: account_cases
    description: "Accounts joined to cases (should not have fanout)"
    tests:
      - detect_fanout:
          group_by_columns: ['case_id']
          max_fanout_ratio: 1.0  # Strict - no duplication allowed
    columns:
      - name: case_id
        description: "Unique case identifier"
        tests:
          - not_null
      - name: account_id
        description: "The account this case belongs to"
        tests:
          - not_null

  - name: account_case_comments
    description: "Accounts joined to cases and comments (known to have fanout)"
    tests:
      - detect_fanout:
          group_by_columns: ['case_id']
          # This test will fail and show the fanout ratio

  - name: account_cases_with_fix
    description: "Fixed model that prevents fanout through aggregation"
    tests:
      - detect_fanout:
          group_by_columns: ['case_id']
          max_fanout_ratio: 1.0

-- Example analysis SQL file that uses the analyze_fanout macro:
-- analysis/check_all_models_for_fanout.sql

{{ analyze_fanout('account_cases', ['case_id']) }}
{{ analyze_fanout('account_case_comments', ['case_id']) }}
{{ analyze_fanout('account_cases_with_fix', ['case_id']) }}

-- Example model post-hook that automatically checks for fanout
-- models/my_model.sql
{{
  config(
    materialized='table',
    post_hook="{{ analyze_fanout(this.name, ['account_id']) }}"
  )
}}

select * from ...
