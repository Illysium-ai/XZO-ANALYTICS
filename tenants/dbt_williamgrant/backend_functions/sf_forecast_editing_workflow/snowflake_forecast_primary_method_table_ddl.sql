-- This script is intended for one-time manual execution to set up the hybrid table
-- that will be managed by the application and augmented by a dbt model.

-- Target Schema: APOLLO_WILLIAMGRANT.FORECAST

-- Step 1: Create the Hybrid Table with a Primary Key
CREATE OR REPLACE HYBRID TABLE FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD (
    MARKET_NAME VARCHAR,
    MARKET_CODE VARCHAR,
    DISTRIBUTOR_ID VARCHAR,
    VARIANT_SIZE_PACK_DESC VARCHAR,
    VARIANT_SIZE_PACK_ID VARCHAR,
    FORECAST_METHOD VARCHAR,
    IS_PRIMARY_FORECAST_METHOD NUMBER(1,0),
    FORECAST_GENERATION_MONTH_DATE DATE,
    -- Define a primary key to enable transactional, single-row updates from the application
    PRIMARY KEY (MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID, FORECAST_GENERATION_MONTH_DATE)
);

COMMENT ON TABLE FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD IS 
'Stores the primary forecast method for a given product combination. This is a Hybrid Table to allow for transactional updates from the application, while being populated with defaults by a dbt model.';


-- Step 2: Perform the initial population of the table using the dbt model logic.
-- This should be run *after* the table is created and *before* the dbt model runs for the first time.
INSERT INTO FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD (
    MARKET_NAME,
    MARKET_CODE,
    DISTRIBUTOR_ID,
    VARIANT_SIZE_PACK_DESC,
    VARIANT_SIZE_PACK_ID,
    FORECAST_METHOD,
    IS_PRIMARY_FORECAST_METHOD,
    FORECAST_GENERATION_MONTH_DATE
)
WITH forecast_data_with_potential_method as (
  select distinct
    fci.market_name,
    fci.market_code,
    fci.distributor_id,
    fci.variant_size_pack_desc,
    fci.variant_size_pack_id,
    case when fci.data_source = 'previous_consensus' then 'run_rate' else coalesce(fcm.primary_forecast_method, 'six_month') end as potential_forecast_method,
    fci.forecast_generation_month_date
  from FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT fci -- Use the table directly
  left join MASTER_DATA.HYPERION_SKU_FORECAST_METHOD fcm -- Use the table directly
    on fci.variant_size_pack_id = fcm.variant_size_pack_id
  where fci.is_current_forecast_generation = 1
), primary_forecast_stage as (
  select
    fd.market_name,
    fd.market_code,
    fd.distributor_id,
    fd.variant_size_pack_desc,
    fd.variant_size_pack_id,
    fd.potential_forecast_method as forecast_method,
    1 as is_primary_forecast_method,
    fd.forecast_generation_month_date
  from forecast_data_with_potential_method fd
)
SELECT * FROM primary_forecast_stage; 