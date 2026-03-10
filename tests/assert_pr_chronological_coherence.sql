-- tests/assert_pr_chronology.sql
-- Vérifie que toutes les PRs ont une date de fermeture cohérente
-- Échoue si une PR est fermée avant d'avoir été créée

select
    repo_id,
    pr_number,
    created_at,
    closed_at
from {{ ref('stg_pull_requests') }}
where closed_at is not null
  and closed_at < created_at