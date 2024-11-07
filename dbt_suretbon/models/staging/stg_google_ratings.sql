select
  meta_osm_id,
  created_at,
  cast(regexp_extract(to_json_string(response), r'\"rating\\":\s*([\d.]+)') as float64) as google_rating,
  regexp_extract(to_json_string(response), r'displayName\\":\s*\{\\"text\\": \\"(.*?)\\",') as google_display_name
from
  `algebraic-link-440513-f9.raw.googleplacesapi`
