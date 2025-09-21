{{
  config(
    materialized = 'view'
  )
}}

with staged as (
  select
    recordtype,
    distid,
    rscust,
    vpid,
    vpdba,
    vplnam,
    vpaddr,
    vpaddr2,
    vpcity,
    vpstat,
    vpzip9,
    vpcoun,
    vpctry,
    vpphon,
    vpchn,
    vpstore,
    vpfranind,
    vpcsts,
    vpiocode,
    vpcot,
    vpsubchnl,
    vpcustyp,
    vpprem,
    vpstatus,
    vpmalt,
    vpwine,
    vpspirits,
    vpbuygrp,
    vplat,
    vplong,
    vpfips,
    vpmsacd,
    vpfutur1,
    vpfutur2,
    vpopen,
    vpclosed,
    vptransid,
    vptransdt,
    vpoldid,
    vpaltdist,
    vpparent,
    vpltlncd,
    vpfips15,
    vpdraft,
    last_updated_at,
    tdlinkid,
    row_number() over (partition by distid, rscust order by last_updated_at desc) as deduper
  from {{ source('vip', 'vipout') }}
  qualify deduper = 1
)
select * from staged