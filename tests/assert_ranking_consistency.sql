-- tests/assert_ranking_consistency.sql
-- Fails if the #1 ranked repo does not have the best score

select
    repo_id,
    score_global,
    ranking
from {{ ref('scoring_repositories') }}
where ranking = 1
and score_global < (
    select max(score_global)
    from {{ ref('scoring_repositories') }}
)
