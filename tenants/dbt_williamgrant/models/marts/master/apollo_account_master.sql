{{
  config(
    materialized = 'incremental',
    unique_key = ['outlet_id', 'distributor_id'],
    on_schema_change = 'sync_all_columns'
  )
}}

with vpcot as (
  select distinct
    ctcot,
    ctcotdesc,
    wkprem,
    wkpremdesc
  from {{ source('vip', 'vocot') }}
),
staged as (
select
  --Retail Outlet Data
  coalesce(vip.rscust::VARCHAR, out.account::VARCHAR) as outlet_id,
  coalesce(vip.distid::VARCHAR, out.dist_id::VARCHAR) as distributor_id,
  first_value(coalesce(vip.vpdba::VARCHAR, out.dba::VARCHAR)) over (partition by outlet_id order by outlet_id desc) as outlet_name,
  coalesce(vip.vpaddr::VARCHAR, out.addr1::VARCHAR) as address_line_1,
  coalesce(vip.vpaddr2::VARCHAR, out.addr2::VARCHAR) as address_line_2,
  coalesce(vip.vpcity::VARCHAR, out.city::VARCHAR) as city,
  coalesce(vip.vpstat::VARCHAR, out.state::VARCHAR) as state,
  coalesce(vip.vpzip9::VARCHAR, out.zip9::VARCHAR) as zip_code,
  coalesce(vip.vpctry::VARCHAR, out.country::VARCHAR) as country,
  coalesce(vip.vpstatus::VARCHAR, out.status::VARCHAR) as out_status,
  coalesce(vip.vpchn::VARCHAR, out.chain::VARCHAR) as out_chain,
  out.chain2::VARCHAR as out_chain_2,
  coalesce(vip.vpcsts::VARCHAR, out.chain_status::VARCHAR) as out_chain_status,
  coalesce(vip.vpcot::VARCHAR, out.class_of_trade::VARCHAR) as out_cot,
  srs.value_desc as out_cot_desc,

  --VIP Outlet Master Data
  vip.vpid as vip_id,
  vip.vpchn::VARCHAR as vip_chain_code,
  coalesce(chain.vpchnm, outchain.vpchnm) as vip_chain_name,
  coalesce(chain.vpparchn, outchain.vpparchn) as vip_parent_chain_code,
  coalesce(chain.vpparnm, outchain.vpparnm) as vip_parent_chain_name,
  vip.vpstore as chain_store_num,
  vip.vpfranind as is_franchise,
  vip.vpcsts as chain_status,
  coalesce(vip.vpiocode::VARCHAR, '') as ownership_hier_lvl1,
  coalesce(ownhier.vpochnhcd2::VARCHAR, '') as ownership_hier_lvl2,
  coalesce(ownhier.vpochnhcd3::VARCHAR, '') as ownership_hier_lvl3,
  coalesce(ownhier.vpochnhcd4::VARCHAR, '') as ownership_hier_lvl4,
  ownhier.vpochnstat as ownership_status,
  ownhier.vpochnhnm1 as ownership_hier_lvl1_name,
  ownhier.vpochnhnm2 as ownership_hier_lvl2_name,
  ownhier.vpochnhnm3 as ownership_hier_lvl3_name,
  ownhier.vpochnhnm4 as ownership_hier_lvl4_name,
  vip.vpcot::VARCHAR as vip_cot_code, --ref vocot file
  vip.vpsubchnl::VARCHAR as vip_subcot_code,
  vpcot.ctcotdesc as vip_cot_desc,
  vpsubcot.ctscotdesc as vip_subcot_desc,
  vpcot.wkprem as vip_cot_premise_type_code,
  vpcot.wkpremdesc as vip_cot_premise_type_desc,
  vpsubcot.wklictyp as vip_subcot_license_type_code,
  vpsubcot.wklictypd as vip_subcot_license_type_desc,
  vip.vpcustyp::VARCHAR as vip_cuisine_type,
  vpcus.desc as vip_cuisine_desc,
  vip.vpprem as vip_premise_type,
  vip.vpstatus as store_status,
  vip.vpbuygrp as corp_buying_office,
  vip.vptransid as transferred_to_vip_id,
  vip.vptransdt as tranferred_date,
  vip.vpoldid as previous_vip_id,
  vip.vpaltdist as alternative_distributor_id,
  vip.tdlinkid
from {{ ref('stg_vip__vipout') }} vip
full outer join {{ source('vip', 'outda') }} out
  on out.account = vip.rscust and out.dist_id = vip.distid
  and coalesce(vip.rscust, '') != '' and coalesce(vip.distid, '') != ''
left join vpcot vpcot
  on vip.vpcot = vpcot.ctcot
left join {{ source('vip', 'vocot') }} vpsubcot
  on vip.vpsubchnl = vpsubcot.ctscot
left join {{ source('vip', 'vipvalue') }} vpcus
  on vip.vpcustyp = vpcus.code
  and vpcus.field = 'VPCUSTYP'
left join {{ source('vip', 'vochain') }} chain
  on vip.vpchn = chain.vpchn
left join {{ source('vip', 'vochain') }} outchain
  on out.chain = outchain.vpchn
left join {{ source('vip', 'voowndesc') }} owndesc
  on vip.vpiocode = owndesc.vpchnhcd
left join {{ source('vip', 'voownhier') }} ownhier
  on vip.vpiocode = ownhier.vpochnhcd1
left join {{ ref('stg_vip__srsvalue') }} srs
  on out.class_of_trade = srs.value
  and srs.field_name = 'ROCOT'
)
select * from staged
where outlet_id is not null and distributor_id is not null
order by outlet_id, distributor_id