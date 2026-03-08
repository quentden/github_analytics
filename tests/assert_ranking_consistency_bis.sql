-- Vérifie qu'il y a bien un seul meilleur repo 

select *
from (
    select count(*) as nb_rank_1
    from {{ ref('scoring_repositories') }}
    where ranking = 1
) t
where nb_rank_1 <> 1
