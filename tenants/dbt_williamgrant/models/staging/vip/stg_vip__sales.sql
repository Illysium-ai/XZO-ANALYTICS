{{
  config(
    materialized = 'view'
  )
}}

{% set depletion_period_parsed = "TRY_TO_DATE(s.depletion_period, 'YYYYMM')" %}
{% set invoice_date_parsed = "TRY_TO_DATE(s.invoice_date, 'YYYYMMDD')" %}

with staged as (
  select
    
    extract(year from {{ depletion_period_parsed }})::INTEGER as year,
    extract(month from {{ depletion_period_parsed }})::INTEGER as month,
    to_char({{ depletion_period_parsed }}, 'MMMM') as month_name,
    {{ depletion_period_parsed }}::DATE as month_date,
    s.dist_id::VARCHAR as distributor_id,
    s.acct_nbr::VARCHAR as outlet_id,
    s.supp_item::VARCHAR as sku_id,
    s.dist_item::VARCHAR as distributor_item_id,
    {{ invoice_date_parsed }}::DATE as invoice_date,
    s.invoice_nbr::VARCHAR as invoice_number,
    s.invoice_line::BIGINT as invoice_line,
    s.uom,
    coalesce(s.qty::FLOAT, 0.0)::FLOAT as quantity,
    coalesce(s.net_price::FLOAT, 0.0)::FLOAT as net_amount,
    coalesce(s.front::FLOAT, 0.0)::FLOAT as frontline_amount
  from 
    {{ source('vip', 'slsda') }} s
  where 
    s.invoice_date is not null
    and s.supp_item != 'XXXXXX'
    and s.dist_item is not null
  )

select * from staged