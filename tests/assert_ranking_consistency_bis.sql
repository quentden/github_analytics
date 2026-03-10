-- tests/assert_ranking_consistency_bis.sql
-- Vérifie qu'il y a exactement un repo classé 1
-- Échoue si plusieurs repos ou aucun repo ont le ranking = 1

select *
from (
    select count(*) as nb_rank_1
    from {{ ref('scoring_repositories') }}
    where ranking = 1
) t
where nb_rank_1 <> 1
