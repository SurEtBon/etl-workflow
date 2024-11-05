select
  ac.*,
  {{ clean_text('ac.app_libelle_etablissement') }} AS clean_name
from {{ ref('stg_alimconfiance') }} ac
where ac.filtre = 'Restaurants'
