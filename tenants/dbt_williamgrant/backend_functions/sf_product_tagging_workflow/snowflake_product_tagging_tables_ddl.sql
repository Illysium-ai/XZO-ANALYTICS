-- DDLs for Product Tagging Workflow Tables in Snowflake
-- Target Schema: APOLLO_WILLIAMGRANT.MASTER_DATA


-- Drop existing tables if they exist (order might matter if FKs were present)
DROP TABLE IF EXISTS MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG;
DROP TABLE IF EXISTS MASTER_DATA.APOLLO_PRODUCT_TAGS;


-- 1. Create a table to store unique tags (Hybrid Table)
CREATE OR REPLACE HYBRID TABLE MASTER_DATA.APOLLO_PRODUCT_TAGS (
  TAG_ID INTEGER IDENTITY(1,1) PRIMARY KEY,
  TAG_NAME VARCHAR UNIQUE NOT NULL
);
COMMENT ON TABLE MASTER_DATA.APOLLO_PRODUCT_TAGS IS 'Stores unique product tags. Implemented as a Hybrid Table.';


-- 2. Create the apollo_variant_size_pack_tag table (Hybrid Table)
-- This table maps variant_size_pack_id to arrays of tag_ids and tag_names.
CREATE OR REPLACE HYBRID TABLE MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG AS
SELECT
  sku_master.VARIANT_SIZE_PACK_ID,          -- This will be the PK
  sku_master.VARIANT_SIZE_PACK_DESC,
  CAST(NULL AS ARRAY) AS TAG_IDS,       -- Initially NULL, will be updated to empty array. Stores array of INTEGER tag IDs.
  CAST(NULL AS ARRAY) AS TAG_NAMES      -- Initially NULL, will be updated to empty array. Stores array of VARCHAR tag names.
FROM MASTER_DATA.APOLLO_SKU_MASTER sku_master -- Assuming APOLLO_SKU_MASTER is the final table name for the dbt model
GROUP BY 1,2;
-- ORDER BY 1,2; -- ORDER BY in CTAS is not typically needed for table structure, but kept if original logic relied on it for some reason (unlikely for PK setup)

-- Add primary key constraint after table creation from CTAS
ALTER TABLE MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
ADD PRIMARY KEY (VARIANT_SIZE_PACK_ID);

-- Initialize the array columns to empty arrays
UPDATE MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
SET TAG_IDS = ARRAY_CONSTRUCT(),
    TAG_NAMES = ARRAY_CONSTRUCT();

-- Add planning/exclusion/custom columns with defaults
ALTER TABLE MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
  ADD COLUMN IF NOT EXISTS IS_PLANNED VARCHAR; -- allowed: 'true','false','default'
ALTER TABLE MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
  ADD COLUMN IF NOT EXISTS MARKET_CODE_EXCLUSIONS ARRAY DEFAULT ARRAY_CONSTRUCT();
ALTER TABLE MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
  ADD COLUMN IF NOT EXISTS CUSTOMER_ID_EXCLUSIONS ARRAY DEFAULT ARRAY_CONSTRUCT();
ALTER TABLE MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
  ADD COLUMN IF NOT EXISTS IS_CUSTOM_PRODUCT BOOLEAN DEFAULT FALSE;

-- Enforce NOT NULL where appropriate
-- IS_PLANNED remains nullable by design for test gating
-- ALTER TABLE MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
--   ALTER COLUMN IS_PLANNED SET NOT NULL;
ALTER TABLE MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
  ALTER COLUMN MARKET_CODE_EXCLUSIONS SET NOT NULL;
ALTER TABLE MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
  ALTER COLUMN CUSTOMER_ID_EXCLUSIONS SET NOT NULL;
ALTER TABLE MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
  ALTER COLUMN IS_CUSTOM_PRODUCT SET NOT NULL;

COMMENT ON TABLE MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG IS 'Maps variant_size_pack_id to an array of product tag IDs and names, plus planning flags and exclusions. Implemented as a Hybrid Table.';

-- Note: The original PostgreSQL DDL had ALTER COLUMN statements to change types to arrays.
-- In this Snowflake version, TAG_IDS and TAG_NAMES are defined as ARRAY type during table creation (via CAST(NULL AS ARRAY)),
-- making subsequent type alterations unnecessary. They are then initialized to empty arrays. 