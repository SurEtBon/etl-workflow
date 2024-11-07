select
  meta_osm_id,
  created_at,
  cast(regexp_extract(to_json_string(response), r'\"rating\\":\s*\\"([\d.]+)') as float64) as tripadvisor_rating,
from
  `algebraic-link-440513-f9.raw.tripadvisor-location-details`