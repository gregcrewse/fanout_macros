-- models/base/sf_accounts.sql
with sf_accounts as (
    select 
        'A001' as account_id, 'Acme Corp' as account_name, 'Technology' as industry, 'Enterprise' as segment
    union all
    select 
        'A002' as account_id, 'Globex Inc' as account_name, 'Manufacturing' as industry, 'Mid-Market' as segment
    union all
    select 
        'A003' as account_id, 'Stark Industries' as account_name, 'Energy' as industry, 'Enterprise' as segment
)

select * from sf_accounts

-- models/base/sf_contacts.sql
with sf_contacts as (
    select 
        'C001' as contact_id, 'A001' as account_id, 'John Smith' as contact_name, 'john@acme.com' as email
    union all
    select
        'C002' as contact_id, 'A001' as account_id, 'Sarah Jones' as contact_name, 'sarah@acme.com' as email
    union all
    select
        'C003' as contact_id, 'A002' as account_id, 'Alex Brown' as contact_name, 'alex@globex.com' as email
    union all
    select
        'C004' as contact_id, 'A003' as account_id, 'Tony Stark' as contact_name, 'tony@stark.com' as email
)

select * from sf_contacts

-- models/base/sf_cases.sql
with sf_cases as (
    select 
        'CS001' as case_id, 'A001' as account_id, 'C001' as contact_id, 'Product Issue' as subject, 
        'High' as priority, 'Open' as status, '2023-01-15' as created_date
    union all
    select
        'CS002' as case_id, 'A001' as account_id, 'C002' as contact_id, 'Billing Question' as subject, 
        'Medium' as priority, 'Closed' as status, '2023-01-20' as created_date
    union all
    select
        'CS003' as case_id, 'A002' as account_id, 'C003' as contact_id, 'Feature Request' as subject, 
        'Low' as priority, 'Open' as status, '2023-02-05' as created_date
    union all
    select
        'CS004' as case_id, 'A001' as account_id, 'C001' as contact_id, 'API Integration Help' as subject, 
        'High' as priority, 'In Progress' as status, '2023-02-10' as created_date
)

select * from sf_cases

-- models/base/sf_case_comments.sql
with sf_case_comments as (
    select 
        'CM001' as comment_id, 'CS001' as case_id, 'Initial troubleshooting done' as comment_body, '2023-01-16' as created_date
    union all
    select
        'CM002' as comment_id, 'CS001' as case_id, 'Waiting for customer response' as comment_body, '2023-01-17' as created_date
    union all
    select
        'CM003' as comment_id, 'CS002' as case_id, 'Billing issue resolved' as comment_body, '2023-01-21' as created_date
    union all
    select
        'CM004' as comment_id, 'CS003' as case_id, 'Added to feature backlog' as comment_body, '2023-02-06' as created_date
    union all
    select
        'CM005' as comment_id, 'CS004' as case_id, 'Sent documentation links' as comment_body, '2023-02-11' as created_date
    union all
    select
        'CM006' as comment_id, 'CS004' as case_id, 'Scheduled technical call' as comment_body, '2023-02-12' as created_date
)

select * from sf_case_comments

-- models/marts/account_cases.sql (No fanout - proper join)
select
    a.account_id,
    a.account_name,
    a.industry,
    a.segment,
    c.case_id,
    c.subject,
    c.priority,
    c.status,
    c.created_date
from {{ ref('sf_accounts') }} a
left join {{ ref('sf_cases') }} c on a.account_id = c.account_id

-- models/marts/account_case_comments.sql (With fanout - multiple comments per case)
select
    a.account_id,
    a.account_name,
    a.industry,
    a.segment,
    c.case_id,
    c.subject,
    c.priority,
    c.status,
    c.created_date as case_created_date,
    cc.comment_id,
    cc.comment_body,
    cc.created_date as comment_created_date
from {{ ref('sf_accounts') }} a
left join {{ ref('sf_cases') }} c on a.account_id = c.account_id
left join {{ ref('sf_case_comments') }} cc on c.case_id = cc.case_id

-- models/marts/account_cases_with_fix.sql (Fixed fanout with distinct/group by)
select
    a.account_id,
    a.account_name,
    a.industry,
    a.segment,
    c.case_id,
    c.subject,
    c.priority,
    c.status,
    c.created_date as case_created_date,
    count(cc.comment_id) as comment_count,
    max(cc.created_date) as latest_comment_date
from {{ ref('sf_accounts') }} a
left join {{ ref('sf_cases') }} c on a.account_id = c.account_id
left join {{ ref('sf_case_comments') }} cc on c.case_id = cc.case_id
group by 
    a.account_id,
    a.account_name,
    a.industry,
    a.segment,
    c.case_id,
    c.subject,
    c.priority,
    c.status,
    c.created_date
