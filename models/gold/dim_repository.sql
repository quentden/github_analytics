{{ config(materialized='table') }}

with repo as (
    select *
    from {{ ref('stg_repositories') }}
)

select

    repo_id,
    repo_name,
    owner_login,
    description,
    language,
    license_name,
    created_at,
    stars_count,
    forks_count,
    watchers_count,
    open_issues_count,
    has_wiki,
    has_pages,
    repo_age_days,
    default_branch



from repo


