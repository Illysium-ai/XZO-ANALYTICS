
CREATE OR REPLACE HYBRID TABLE FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD_CHAINS (
    MARKET_NAME VARCHAR,
    MARKET_CODE VARCHAR,
    DISTRIBUTOR_ID VARCHAR,
    PARENT_CHAIN_CODE VARCHAR,
    VARIANT_SIZE_PACK_DESC VARCHAR,
    VARIANT_SIZE_PACK_ID VARCHAR,
    FORECAST_METHOD VARCHAR,
    IS_PRIMARY_FORECAST_METHOD NUMBER(1,0),
    PRIMARY KEY (MARKET_CODE, DISTRIBUTOR_ID, PARENT_CHAIN_CODE, VARIANT_SIZE_PACK_ID),
    INDEX idx_depletions_forecast_primary_forecast_method_chains_composite (MARKET_CODE, FORECAST_METHOD, DISTRIBUTOR_ID, PARENT_CHAIN_CODE, VARIANT_SIZE_PACK_ID)
) AS
-- INSERT INTO FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD_CHAINS
select
    fci.market_name,
    fci.market_code,
    fci.distributor_id,
    fci.parent_chain_code,
    fci.variant_size_pack_desc,
    fci.variant_size_pack_id,
    pfm.forecast_method as forecast_method,
    1 as is_primary_forecast_method
from FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT_CHAINS fci
JOIN FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD pfm
    on fci.market_code = pfm.market_code
    and fci.distributor_id = pfm.distributor_id
    and fci.variant_size_pack_id = pfm.variant_size_pack_id
    and fci.forecast_method = pfm.forecast_method
    and fci.forecast_generation_month_date = pfm.forecast_generation_month_date
where fci.forecast_generation_month_date = '2025-07-01'
group by all;