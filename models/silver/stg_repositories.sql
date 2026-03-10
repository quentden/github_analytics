-- models/silver/stg_repositories.sql

{{ config(
    materialized='incremental',
    schema='silver',
    unique_key='repo_id',
    incremental_strategy='merge'
) }}
-- car les repositories peuvent changer d'état facilement (forks, etc.)

with source as (

select *,
row_number() over (
partition by full_name
order by updated_at desc
) as rn

from {{ source('bronze','raw_repositories') }}

),

cleaned as (

select
    full_name as repo_id,
    name as repo_name,
    owner_login,
    license_name,

    coalesce(description,'No description') as description,
    coalesce(language,'Unknown') as language,

    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(pushed_at as timestamp) as pushed_at,

    cast(stargazers_count as integer) as stars_count,
    cast(forks_count as integer) as forks_count,
    cast(watchers_count as integer) as watchers_count,
    cast(open_issues_count as integer) as open_issues_count,

    default_branch,
    cast(has_wiki as boolean) as has_wiki,
    cast(has_pages as boolean) as has_pages,

    datediff('day', cast(created_at as date), current_date) as repo_age_days

from source
where archived = false
and rn = 1
)

select * from cleaned

-- aucune condition incrémentale car nous n'avons pas beaucoup de repositories et ces derniers sont très changeants sur beaucoup de variables