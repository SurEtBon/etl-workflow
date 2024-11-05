select
  ff.*,
  {{ clean_text('ff.name') }} AS clean_name
from
  {{ ref('stg_france_food') }} ff
where
  ff.type in ('restaurant', 'fast_food')
