select
  * except(geography_column_wkt),
  ST_GEOGFROMTEXT(geography_column_wkt) as geores,
  {{ clean_text('app_libelle_etablissement') }} AS clean_name,
  concat(adresse_2_ua, ' - ', code_postal, ' ', libelle_commune) as full_address
from (
  select distinct * except(geores),
  ST_ASTEXT(geores) as geography_column_wkt
  from
    {{ ref('stg_alimconfiance') }} ac
  where
    ac.filtre = 'Restaurants'
)
