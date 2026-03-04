-- models/silver/stg_pull_requests.sql
{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('bronze', 'raw_pull_requests') }}
),

cleaned as (
    select

        repo_full_name as repo_id,
        cast(pr_number as integer) as pr_number,

        -- Dates -> TIMESTAMP
        cast(created_at as timestamp) as created_at,
        cast(closed_at  as timestamp) as closed_at,
        cast(merged_at  as timestamp) as merged_at,

        -- Booleans
        (merged_at is not null) as is_merged,
        coalesce(cast(draft as boolean), false) as is_draft,

        case
            when merged_at is not null then
                datediff('hour', cast(created_at as timestamp), cast(merged_at as timestamp))
            when closed_at is not null then
                datediff('hour', cast(created_at as timestamp), cast(closed_at as timestamp))
            else
                null
        end as time_to_close_hours

    from source
)

select *
from cleaned