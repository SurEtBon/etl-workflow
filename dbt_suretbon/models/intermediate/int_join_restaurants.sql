select
  ff.name as osm_name,
  ff.clean_name as osm_clean_name,
  ff.siret as osm_siret,
  ff.meta_geo_point as geo_osm,
  ST_ASBINARY(ff.meta_geo_point) as geopandas_osm,
  ac.app_libelle_etablissement as alimconfiance_name,
  ac.clean_name as alimconfiance_clean_name,
  ac.siret as alimconfiance_siret,
  ac.geores as geo_alimconfiance,
  ST_ASBINARY(ac.geores) as geopandas_alimconfiance,
  EDIT_DISTANCE(ff.clean_name, ac.clean_name) as distance_name_label,
  ff.type,
  ff.stars,
  ac.synthese_eval_sanit,
  ac.app_code_synthese_eval_sanit,
  ff.meta_osm_id,
  ac.adresse_2_ua,
  ac.code_postal,
  ac.libelle_commune,
  ac.date_inspection,
  ac.reg_name,
  ac.dep_name,
  ac.com_name,
  ac.full_address
from
  {{ ref('int_alimconfiance') }} ac
join
  {{ ref('int_france_food') }} ff
on
  ST_DWithin(ac.geores, ff.meta_geo_point, 30)
