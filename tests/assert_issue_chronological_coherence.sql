-- tests/assert_issue_chronology.sql
-- Vérifie que la date de fermeture d'une issue n'est pas antérieure à sa date de création
-- Échoue si closed_at < created_at pour une issue donnée

select
    repo_id,
    issue_number,
    created_at,
    closed_at
from {{ ref('stg_issues') }}
where closed_at is not null
  and closed_at < created_at