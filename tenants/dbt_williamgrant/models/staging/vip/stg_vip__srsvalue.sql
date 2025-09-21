{{
  config(
    materialized = 'view'
  )
}}

with staged as (
select
  record_type,
  field_name,
  field_desc,
  value,
  value_desc,
  row_number() over (partition by field_name, value order by value_desc) as deduper
from {{ source('vip', 'srsvalue') }}
qualify deduper = 1
)
select
  *
from staged