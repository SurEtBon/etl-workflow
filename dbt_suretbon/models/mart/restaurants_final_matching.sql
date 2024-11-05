select
  *
from
  {{ ref('int_join_restaurants') }}
where
  (osm_clean_name = alimconfiance_clean_name)
  or (osm_siret = alimconfiance_siret)
  or (alimconfiance_clean_name is null)
  or (distance_name_label <= 3)
