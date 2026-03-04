{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('bronze', 'raw_repositories') }}
),

cleaned as (

    select
        -- Business key
        full_name as repo_id,

        -- Null handling
        coalesce(description, 'No description') as description,
        coalesce(language, 'Unknown') as language,

        -- Dates -> TIMESTAMP
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(pushed_at  as timestamp) as pushed_at,

        -- Metrics -> INTEGER
        cast(stargazers_count as integer) as stargazers_count,
        cast(forks_count as integer) as forks_count,
        cast(watchers_count as integer) as watchers_count,
        cast(open_issues_count as integer) as open_issues_count,

        -- Derived column: age in days (today - created_at)
        datediff('day', cast(created_at as date), current_date) as repo_age_days

    from source
    where archived =  false
    
    
)

select * from cleaned