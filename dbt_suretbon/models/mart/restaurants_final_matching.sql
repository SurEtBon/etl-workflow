with inspections as (
  select
    alimconfiance_name,
    geopandas_osm,
    osm_name,
    count(alimconfiance_name) as nb_inspections,
    array_agg(to_json_string(struct(date_inspection, app_code_synthese_eval_sanit))) as inspections_dict
from {{ ref('int_join_restaurants') }}
group by 1,2,3
),

matching_restaurants as (
  select
    rest.*,
    inspections.nb_inspections,
    inspections.inspections_dict,
    row_number() over (partition by rest.alimconfiance_name, rest.geopandas_osm, rest.osm_name order by date_inspection desc) as row_num
  from
    {{ ref('int_join_restaurants') }} rest
  join inspections
    on inspections.alimconfiance_name = rest.alimconfiance_name
    and inspections.geopandas_osm = rest.geopandas_osm
    and inspections.osm_name = rest.osm_name
  where
    (rest.osm_clean_name = rest.alimconfiance_clean_name)
    or (rest.osm_siret = rest.alimconfiance_siret)
    or (rest.alimconfiance_clean_name is null)
    or (rest.distance_name_label <= 3)
)

select
  * except(row_num)
from
  matching_restaurants
where
  row_num = 1
