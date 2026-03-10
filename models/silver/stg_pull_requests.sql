-- models/silver/stg_pull_requests.sql

{{ config(
    materialized='incremental',
    schema='silver',
    unique_key=['repo_id', 'pr_number'],
    incremental_strategy='merge'
) }}
-- car les PR peuvent changer d'état : état open → closed

with source as (
    select *
    from {{ source('bronze', 'raw_pull_requests') }}
),

cleaned as (
    select

        repo_full_name as repo_id,
        cast(pr_number as integer) as pr_number,
        user_login as user_login,
        state as pr_state,

        -- Dates -> TIMESTAMP
        cast(created_at as timestamp) as created_at,
        cast(closed_at  as timestamp) as closed_at,
        cast(merged_at  as timestamp) as merged_at,

        -- Booleans
        (merged_at is not null) as is_merged,
        cast(draft as boolean) = true as is_draft,
        

        case
            when merged_at is not null then
                datediff('hour', cast(created_at as timestamp), cast(merged_at as timestamp))
            when closed_at is not null then
                datediff('hour', cast(created_at as timestamp), cast(closed_at as timestamp))
            else
                null
        end as time_to_close_hours

    from source
    where pr_number is not null
)

select *
from cleaned

{% if is_incremental() %}
where 
    created_at > (select coalesce(max(created_at), '1900-01-01') from {{ this }})
    or (closed_at is not null and closed_at > (select coalesce(max(closed_at), '1900-01-01') from {{ this }}))
    or (merged_at is not null and merged_at > (select coalesce(max(merged_at), '1900-01-01') from {{ this }}))
{% endif %}