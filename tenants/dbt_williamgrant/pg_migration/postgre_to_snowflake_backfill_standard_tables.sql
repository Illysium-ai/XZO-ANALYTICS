INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."SLSDA"
SELECT
  rcd_type,
  dist_id,
  reserved,
  invoice_date,
  invoice_nbr,
  acct_nbr,
  supp_item,
  qty,
  uom,
  front,
  fl_uom,
  net_price,
  net_uom,
  NULL AS from_date,
  null as to_date,
  vip_id,
  dist_item,
  net_price4,
  deposit,
  crv,
  local_tax,
  adtl_chrg,
  sls_rep_id,
  repack,
  whse_id,
  parent_id,
  cost,
  trans_type,
  promo_id,
  promo_group,
  supplier_promo_id,
  ext_dist_disc,
  supp_dep_allow,
  split_case_chrg,
  combo,
  free_good,
  duty_free,
  order_date,
  order_qty,
  order_uom,
  invoice_line,
  reference_invoice,
  extend_front_line_price,
  depletion_period,
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/PRE2025_SLSDA.WGS.gz' as file_name,
  '2024-12-31'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  estuary_db.estuary_schema.wg_vip_glue_slsdahist a
WHERE NOT EXISTS (
  SELECT 1 
  FROM estuary_db.estuary_schema.wg_vip_glue_slsda b
  WHERE a.dist_id = b.dist_id
    AND a.invoice_date = b.invoice_date
)
and "_meta/op" != 'd';

INSERT INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."SLSDA"
SELECT
  rcd_type,
  dist_id,
  reserved,
  invoice_date,
  invoice_nbr,
  acct_nbr,
  supp_item,
  qty,
  uom,
  front,
  fl_uom,
  net_price,
  net_uom,
  NULL AS from_date,
  null as to_date,
  vip_id,
  dist_item,
  net_price4,
  deposit,
  crv,
  local_tax,
  adtl_chrg,
  sls_rep_id,
  repack,
  whse_id,
  parent_id,
  cost,
  trans_type,
  promo_id,
  promo_group,
  supplier_promo_id,
  ext_dist_disc,
  supp_dep_allow,
  split_case_chrg,
  combo,
  free_good,
  duty_free,
  order_date,
  order_qty,
  order_uom,
  invoice_line,
  reference_invoice,
  extend_front_line_price,
  depletion_period,
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/2025YTD_SLSDA.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  estuary_db.estuary_schema.wg_vip_glue_slsda a
where "_meta/op" != 'd';

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."DEPLDA"
SELECT
  "RECORD_TYPE",
  "DIST_ID",
  "DEPL_YEAR_MON",
  "SUPPLIER_ITEM",
  "DIST_ITEM",
  "BEGIN_ON_HAND",
  "RECEIPTS",
  "TRANS_IN",
  "TRANS_OUT",
  "RETURNS",
  "BREAKAGE",
  "SAMPLES",
  "ADJUSTMENTS",
  "TOTAL_INV_CHG",
  "TOTAL_SALES",
  "END_ON_HAND",
  "ON_ORDER",
  "IN_BOND",
  "MILITARY_OFF",
  "NON_RETAIL",
  "SUB_DIST_SALES",
  "OFF_PREMISE",
  "TRANSPORT",
  "MILITARY_ON",
  "OTHER_DIST_SALES",
  "UN_CLASSIFIED",
  "ON_PREMISE",
  "ALT_DIST_ID",
  "AUDIT_DATE",
  "AUDIT_USER",
  "OUT_AUDIT_DATE",
  "OUT_AUDIT_USER",
  "INV_AUDIT_DATE",
  "INV_AUDIT_USER",
  "ON_PREMISES",
  "OFF_PREMISES",
  NULL AS "FROM_DATE",
  NULL AS "TO_DATE",
  "LAST_SLS_DATE",
  "AVG_SALES",
  "PARENT_ID",
  "DEPLETION_PERIOD",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/2025YTD_DEPLDA.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  estuary_db.estuary_schema.wg_vip_glue_deplda
where "_meta/op" != 'd';

INSERT INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."DEPLDA"
SELECT
  "RECORD_TYPE",
  "DIST_ID",
  "DEPL_YEAR_MON",
  "SUPPLIER_ITEM",
  "DIST_ITEM",
  "BEGIN_ON_HAND",
  "RECEIPTS",
  "TRANS_IN",
  "TRANS_OUT",
  "RETURNS",
  "BREAKAGE",
  "SAMPLES",
  "ADJUSTMENTS",
  "TOTAL_INV_CHG",
  "TOTAL_SALES",
  "END_ON_HAND",
  "ON_ORDER",
  "IN_BOND",
  "MILITARY_OFF",
  "NON_RETAIL",
  "SUB_DIST_SALES",
  "OFF_PREMISE",
  "TRANSPORT",
  "MILITARY_ON",
  "OTHER_DIST_SALES",
  "UN_CLASSIFIED",
  "ON_PREMISE",
  "ALT_DIST_ID",
  "AUDIT_DATE",
  "AUDIT_USER",
  "OUT_AUDIT_DATE",
  "OUT_AUDIT_USER",
  "INV_AUDIT_DATE",
  "INV_AUDIT_USER",
  "ON_PREMISES",
  "OFF_PREMISES",
  NULL AS "FROM_DATE",
  NULL AS "TO_DATE",
  "LAST_SLS_DATE",
  "AVG_SALES",
  "PARENT_ID",
  "DEPLETION_PERIOD",
  CAST('2024-12-31' AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/PRE2025_DEPLDA.WGS.gz' as file_name,
  '2024-12-31'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  estuary_db.estuary_schema.wg_vip_backfill_deplda h
WHERE NOT EXISTS (
  SELECT 1 
  FROM estuary_db.estuary_schema.wg_vip_glue_deplda c
  WHERE h.dist_id::text = c.dist_id::text
        and h.alt_dist_id::text = c.alt_dist_id::text
        and h.depletion_period::text = c.depletion_period::text
        and h.supplier_item::text = c.supplier_item::text
        and h.dist_item::text = c.dist_item::text
)
and "_meta/op" != 'd';

-- CREATE TABLE APOLLO_WILLIAMGRANT.MASTER_DATA.HYPERION_CUSTOMER_PLANNING_GROUP_MAPPING
-- CLONE ESTUARY_DB.ESTUARY_SCHEMA.WG_MASTER_DATA_HYPERION_CUSTOMER_PLANNING_GROUP_MAPPING;

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."CTLDA"
SELECT
  "RECORD_TYPE",
  "DIST_ID",
  "RESERVED",
  "DIST_NAME",
  "SUPPLIER_ITEM",
  "QUANTITY",
  "UNIT_OF_MEASURE",
  "CONTROL_DATE",
  "PARENT_ID",
  "DIST_ITEM",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/061925_CTLDA.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  ESTUARY_DB.ESTUARY_SCHEMA.WG_VIP_GLUE_CTLDA
where "_meta/op" != 'd';

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."CTLSDA"
SELECT
  "RECORD_TYPE",
  "DIST_ID",
  "RESERVED",
  "DIST_NAME",
  "SUPPLIER_ITEM",
  "QUANTITY",
  "UNIT_OF_MEASURE",
  "CONTROL_DATE",
  "PARENT_ID",
  "DIST_ITEM",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/061925_CTLSDA.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  ESTUARY_DB.ESTUARY_SCHEMA.WG_VIP_GLUE_CTLSDA
where "_meta/op" != 'd';

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."DISTDA"
SELECT
  "RECORD_ID",
  "SUPPLIER_ID",
  "DISTRIBUTOR_ID",
  "DISTRIBUTOR_NAME",
  "STREET",
  "CITY",
  "STATE",
  "ZIP",
  "PHONE",
  "CONTACT_1_NAME",
  "CONTACT_1_EMAIL",
  "PARENT_ID",
  "DISTRIBUTOR_REP",
  "ISV_NAME",
  "DISTRIBUTOR_RANK",
  "CERTIFICATION_STATUS",
  "PHASE",
  "LAST_AUDIT_MONTH_EOM",
  "LAST_AUDIT_USER",
  "DIVISION_CODE",
  "DIVISION_DESCRIPTION",
  "AREA_CODE",
  "AREA_DESCRIPTION",
  "MARKET_CODE",
  "MARKET_DESCRIPTION",
  "REP_CODE",
  "REP_DESCRIPTION",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/062425_DISTDA.WGS.gz' as file_name,
  '2025-06-24'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  ESTUARY_DB.ESTUARY_SCHEMA.WG_VIP_GLUE_DISTDA
where "_meta/op" != 'd';

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."ITMDA"
SELECT
  "RECORD_TYPE",
  "SUPPLIER",
  "DISTRIBUTOR",
  "GLN",
  "SUPPLIER_ITEM",
  "DIST_ITEM",
  "DESCRIPTION",
  "GTIN",
  "STATUS",
  "SELL",
  "UNIT",
  "CREATION_DATE",
  "DIST_ITEM_SIZE",
  "PROOF",
  "VINTAGE",
  "DIST_ITEM_GTIN",
  "REPACK",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/061925_ITMDA.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  ESTUARY_DB.ESTUARY_SCHEMA.WG_VIP_GLUE_ITMDA
where "_meta/op" != 'd';

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."NONDA"
SELECT
  "RECORD_TYPE",
  "DIST_ID",
  "NAME",
  "DATE",
  "AVERAGE_SALES",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/061925_NONDA.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  ESTUARY_DB.ESTUARY_SCHEMA.WG_VIP_GLUE_NONDA
where "_meta/op" != 'd';

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."OUTDA"
SELECT
  "RECORD_TYPE",
  "DIST_ID",
  "RESERVED",
  "account",
  "DBA",
  "LIC_NAME",
  "ADDR1",
  "ADDR2",
  "CITY",
  "STATE",
  "ZIP9",
  "COUNTRY",
  "PHONE",
  "CHAIN",
  "CHAIN2",
  "CLASS_OF_TRADE",
  "FINE_WINE",
  "CHAIN_STATUS",
  "DISPLAYS",
  "PATRON_ETHNICITY",
  "INDUSTRY_VOLUME",
  "PATRON_LIFE_STYLE",
  "OCCUPATION",
  "PATRON_AGE",
  "PACKAGE_TYPE",
  "WINE",
  "SPIRIT",
  "MALT",
  "SELL",
  "SALESMAN1",
  "SALESMAN2",
  "STORE",
  "STATUS",
  "T01",
  "T02",
  "T03",
  "T04",
  "T05",
  "T06",
  "T07",
  "T08",
  "T09",
  "T10",
  "T11",
  "T12",
  "T13",
  "T14",
  "T15",
  "T16",
  "T17",
  "T18",
  "T19",
  "T20",
  "T21",
  "T22",
  "T23",
  "T24",
  "T25",
  "T26",
  "T27",
  "T28",
  "T29",
  "T30",
  "TD1",
  "TD2",
  "VIP_ID",
  "VP_MALT",
  "BUYER",
  "LICENSE_TYPE",
  "WHSE_DIST",
  "LICENSE",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/061925_OUTDA.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  ESTUARY_DB.ESTUARY_SCHEMA.WG_VIP_GLUE_OUTDA
where "_meta/op" != 'd';

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."SLMDA"
SELECT
  "RECORD_TYPE",
  "SUPPLIER",
  "DIST_ID",
  "SALES_PERSON_ID",
  "SALE_PERSON_NAME",
  "LEVEL1_CODE",
  "LEVEL1_NAME",
  "LEVEL2_CODE",
  "LEVEL2_NAME",
  "LEVEL3_CODE",
  "LEVEL3_NAME",
  "DIVISION_CODE",
  "DIVISION_NAME",
  "PARENT_ID",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/061925_SLMDA.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  ESTUARY_DB.ESTUARY_SCHEMA.WG_VIP_GLUE_SLMDA
where "_meta/op" != 'd';

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."SRSCAL"
SELECT
  "RECORD_TYPE",
  "DIST_ID",
  "SC_TYPE",
  "CALENDAR_DATA",
  "PARENT_ID",
  "START_DATE",
  "END_DATE",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/061925_SRSCAL.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  ESTUARY_DB.ESTUARY_SCHEMA.WG_VIP_GLUE_SRSCAL
where "_meta/op" != 'd';

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."SRSCHAIN"
SELECT
  "RECORD_TYPE",
  "CHAIN",
  "DESC",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/061925_SRSCHAIN.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  ESTUARY_DB.ESTUARY_SCHEMA.WG_VIP_GLUE_SRSCHAIN
where "_meta/op" != 'd';

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."SRSVALUE"
SELECT
  "RECORD_TYPE",
  "FIELD_NAME",
  "FIELD_DESC",
  "VALUE",
  "VALUE_DESC",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/061925_SRSVALUE.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  ESTUARY_DB.ESTUARY_SCHEMA.WG_VIP_GLUE_SRSVALUE
where "_meta/op" != 'd';

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."VIPVALUE"
SELECT
  "RECORDTYPE",
  "FIELD",
  "FIELDNAME",
  "CODE",
  "DESC",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/061925_VIPVALUE.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  ESTUARY_DB.ESTUARY_SCHEMA.WG_VIP_GLUE_VIPVALUE
where "_meta/op" != 'd';

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."VOCHAIN"
SELECT
  "RECORDTYPE",
  "VPCHN",
  "VPCHNM",
  "VPCHNTYP",
  "VPPARCHN",
  "VPPARNM",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/061925_VOCHAIN.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  ESTUARY_DB.ESTUARY_SCHEMA.WG_VIP_GLUE_VOCHAIN
where "_meta/op" != 'd';

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."VOCOT"
SELECT
  "RECORDTYPE",
  "CTCOT",
  "CTCOTDESC",
  "CTSCOT",
  "CTSCOTDESC",
  "WKPREM",
  "WKPREMDESC",
  "WKLICTYP",
  "WKLICTYPD",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/061925_VOCOT.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  ESTUARY_DB.ESTUARY_SCHEMA.WG_VIP_GLUE_VOCOT
where "_meta/op" != 'd';

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."VOOWNDESC"
SELECT
  "RECORDTYPE",
  "VPCHNHCD",
  "VPCHNHNM",
  "VPCHNHADR",
  "VPCHNHCITY",
  "VPCHNHST",
  "VPCHNHZIP",
  "VPCHNHPHN",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/061925_VOOWNDESC.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  ESTUARY_DB.ESTUARY_SCHEMA.WG_VIP_GLUE_VOOWNDESC
where "_meta/op" != 'd';

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."VOOWNHIER"
SELECT
  "RECORDTYPE",
  "VPOCHNHCD1",
  "VPOCHNHNM1",
  "VPOCHNHCD2",
  "VPOCHNHNM2",
  "VPOCHNHCD3",
  "VPOCHNHNM3",
  "VPOCHNHCD4",
  "VPOCHNHNM4",
  "VPOCHNSTAT",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/061925_VOOWNHIER.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  ESTUARY_DB.ESTUARY_SCHEMA.WG_VIP_GLUE_VOOWNHIER
where "_meta/op" != 'd';

INSERT OVERWRITE INTO "APOLLO_WILLIAMGRANT"."SOURCE_DATA"."VOXREF"
SELECT
  "RECORDTYPE",
  "DISTID",
  "RSCUST",
  "VPID",
  "VPST",
  "VPPARENT",
  "TDLINXCD",
  CAST(last_updated_glue AS TIMESTAMP_NTZ) AS last_updated_at,
  'upload/061925_VOXREF.WGS.gz' as file_name,
  '2025-06-19'::date as file_date,
  NULL AS FILE_ROW_NUMBER
FROM
  ESTUARY_DB.ESTUARY_SCHEMA.WG_VIP_GLUE_VOXREF
where "_meta/op" != 'd';

