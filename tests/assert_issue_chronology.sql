select
    repo_id,
    issue_number,
    created_at,
    closed_at
from {{ ref('stg_issues') }}
where closed_at is not null
  and closed_at < created_at