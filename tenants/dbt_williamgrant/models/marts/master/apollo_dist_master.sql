{{
  config(
    materialized = 'incremental',
    unique_key = 'distributor_id',
    cluster_by = ['distributor_id'],
    on_schema_change = 'sync_all_columns'
  )
}}

with staged as (
  select
    coalesce(left(src.distributor_id,5)::VARCHAR, hcm.customer_id::VARCHAR) as customer_id,
    TRIM(SPLIT_PART(hcm.customer_actual_data, '-', 2)) AS customer_name,
    coalesce(src.distributor_id, hcm.customer_id) as distributor_id,
    src.distributor_name,
    src.street as street_address,
    src.city,
    src.state,
    src.zip as zip_code,
    src.phone,
    src.contact_1_name as primary_contact,
    src.contact_1_email as primary_email,
    src.parent_id as parent_distributor_id,
    src.distributor_rep as rep_name,
    src.distributor_rank,
    src.certification_status,
    src.phase,
    src.last_audit_month_eom as last_audit_date,
    'Distributor' as industry_tier, -- Tier 2 in the three-tier system
    src.division_code as division_code,
    src.division_description as division_name,
    src.area_code as area_code,
    src.area_description as area_name,
    src.market_code as market_code,
    src.market_description as market_name,
    case
      when src.certification_status = 'Certified' then
        -- Oregon and Utah data comes from Green Book
        -- All other markets come from US Commercial Regions
        case when src.market_code in ('USAOR1', 'USAUT1') and src.division_description = 'Green Book' then 1
             when src.market_code not in ('USAOR1', 'USAUT1') and src.division_description = 'US Commercial Regions' then 1
             else 0
             end
      else 0
      end as is_depletions_eligible,
    hcm.market as hp_market,
    hcm.customer_actual_data as hp_customer_actual_data,
    coalesce(hcm.planning_member, hcpgm.planning_group) as hp_planning_member,
    hcm.customer_stat_level as hp_customer_stat_level,
    hcm.market_coding as hp_market_coding,
    hcm.customer_coding as hp_customer_coding,
    hcm.planning_member_coding as hp_planning_member_coding
  from {{ source('vip', 'distda') }} src
  full outer join {{ ref('stg_hyperion__customer_master') }} hcm
    on left(src.distributor_id,5)::VARCHAR = hcm.customer_id::VARCHAR
  left join {{ source('master_data', 'hyperion_customer_planning_group_mapping') }} hcpgm
    on left(src.distributor_id,5)::VARCHAR = hcpgm.customer_id::VARCHAR
)

select * from staged 