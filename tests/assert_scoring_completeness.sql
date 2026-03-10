-- tests/assert_scoring_completeness.sql
-- Vérifie que tous les repos de dim_repository apparaissent dans scoring_repositories
-- Échoue si un repo a été perdu lors du scoring

select
    d.repo_id
from {{ ref('dim_repository') }} d
left join {{ ref('scoring_repositories') }} s
    on d.repo_id = s.repo_id
where s.repo_id is null