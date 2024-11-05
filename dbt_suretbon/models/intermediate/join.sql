select
  osm.name,
  ac.app_libelle_etablissement,
  osm.type
from algebraic-link-440513-f9.raw.alimconfiance ac
join `algebraic-link-440513-f9.raw.osm-france-food-service` osm
  on lower(ac.app_libelle_etablissement) = lower(osm.name)
