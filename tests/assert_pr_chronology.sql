select
    repo_id,
    pr_number,
    created_at,
    closed_at
from {{ ref('stg_pull_requests') }}
where closed_at is not null
  and closed_at < created_at