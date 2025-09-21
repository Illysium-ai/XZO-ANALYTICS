{{
  config(
    materialized = 'view'
  )
}}

with deduped as (
select
    *,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY customer_id) AS row_num
from {{ source('master_data', 'hyperion_customer_master') }}
qualify row_num = 1
)
select * from deduped