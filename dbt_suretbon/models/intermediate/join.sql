

select
  ff.name,
  ac.app_libelle_etablissement,
  ff.type
from {{ ref('stg_alimconfiance') }} ac
join {{ ref('stg_france_food') }} ff
  on lower(ac.app_libelle_etablissement) = lower(ff.name)
