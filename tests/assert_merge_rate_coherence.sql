-- tests/assert_merge_rate_coherence.sql
-- Vérifie que le nombre de PRs mergées n'excède pas le nombre total de PRs
-- Échoue si merged_prs > total_prs pour un repo donné

with community_history as (
    select
        repo_id,
        sum(prs_opened) as total_prs,
        sum(prs_merged) as merged_prs
    from {{ ref('fact_repo_activity') }}
    group by repo_id
)

select *
from community_history
where merged_prs > total_prs