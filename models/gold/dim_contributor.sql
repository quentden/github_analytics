{{ config(materialized='table') }}

with commit_activities as (
    select
        author_login as login,
        repo_id,
        cast(author_date as timestamp) as activity_at,
        'commit' as activity_type
    from {{ ref('stg_commits') }}
),

pr_activities as (
    select
        user_login as login,
        repo_id,
        cast(created_at as timestamp) as activity_at,
        'pr' as activity_type
    from {{ ref('stg_pull_requests') }}
),

all_activities as (
    select * from commit_activities
    union all
    select * from pr_activities
),

filtered as (
    select *
    from all_activities
    where lower(login) not in ('unknown')
      and login is not null
)

select
    -- identifiant (PK) : ici on prend le login comme contributor_id
    login as contributor_id,
    login,

    min(activity_at) as first_contribution_at,
    count(distinct repo_id) as repos_contributed_to,
    count(*) as total_activities

from filtered
group by login
