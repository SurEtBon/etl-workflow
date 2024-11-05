select
  trim(app_libelle_etablissement) as app_libelle_etablissement,
  trim(siret) as siret,
  trim(adresse_2_ua) as adresse_2_ua,
  trim(code_postal) as code_postal,
  trim(libelle_commune) as libelle_commune,
  cast(date_inspection as date) as date_inspection,
  trim(app_libelle_activite_etablissement) as app_libelle_activite_etablissement,
  trim(synthese_eval_sanit) as synthese_eval_sanit,
  cast(app_code_synthese_eval_sanit as int64) as app_code_synthese_eval_sanit,
  trim(agrement) as agrement,
  geores,
  trim(filtre) as filtre,
  trim(ods_type_activite) as ods_type_activite,
  trim(reg_name) as reg_name,
  trim(reg_code) as reg_code,
  trim(dep_name) as dep_name,
  trim(dep_code) as dep_code,
  trim(com_name) as com_name,
  trim(com_code) as com_code
from
  `algebraic-link-440513-f9.raw.export_alimconfiance` ac
