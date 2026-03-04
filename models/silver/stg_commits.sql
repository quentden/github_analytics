-- models/silver/stg_commits.sql
{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('bronze', 'raw_commits') }}
),
cleaned as (
    select

    sha as commit_sha,
    repo_full_name as repo_id,

    coalesce(author_login, 'No author login') as author_login,


    cast(author_date as timestamp) as author_date,
    cast(committer_date as timestamp) as committer_date,

    date_part('dow', cast(author_date as timestamp)) as day_of_week,
    date_part('hour', cast(author_date as timestamp)) as hour_of_day,

    left(coalesce(message, ''), 200) as message
        
    from source
    where sha is not null
)

select * from cleaned