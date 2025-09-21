-- DDLs for Forecast Editing Workflow Tables in Snowflake
-- Target Schema: APOLLO_WILLIAMGRANT.FORECAST


-- Drop existing tables if they exist (order matters due to potential FK)
DROP TABLE IF EXISTS FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS;
DROP TABLE IF EXISTS FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST;

-- 2. Create the main table (current state) - As a Hybrid Table as per playbook recommendation
CREATE OR REPLACE HYBRID TABLE FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST (
    ID INTEGER IDENTITY(1,1) PRIMARY KEY, -- Snowflake uses IDENTITY for auto-increment
    MARKET_NAME VARCHAR(100),
    MARKET_CODE VARCHAR(50),
    DISTRIBUTOR_NAME VARCHAR(100),
    DISTRIBUTOR_ID VARCHAR(50),
    BRAND VARCHAR(100),
    BRAND_ID VARCHAR(50),
    VARIANT VARCHAR(100),
    VARIANT_ID VARCHAR(50),
    VARIANT_SIZE_PACK_DESC VARCHAR(100),
    VARIANT_SIZE_PACK_ID VARCHAR(50),
    FORECAST_YEAR INTEGER,
    MONTH INTEGER,
    FORECAST_METHOD VARCHAR(50),
    FORECAST_GENERATION_MONTH_DATE DATE,
    MANUAL_CASE_EQUIVALENT_VOLUME NUMBER, -- Changed NUMERIC to NUMBER
    UPDATED_BY_USER_ID VARCHAR(50),
    FORECAST_STATUS VARCHAR(50),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), -- Changed TIMESTAMP to TIMESTAMP_NTZ
    CURRENT_VERSION INTEGER DEFAULT 1,
    COMMENT TEXT,
    -- Define secondary indexes if needed for query performance on Hybrid Tables
    INDEX idx_manual_forecast_keys (MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE)
);

-- Add Unique Constraint for business key + FGMD (as per original DDL)
-- Note: For Hybrid tables, PK implies uniqueness. This adds another named unique constraint.
ALTER TABLE FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST
ADD CONSTRAINT UQ_MANUAL_FORECAST_KEY_FGMD
UNIQUE (MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE);

-- 3. Create the version history table (append-only) - As a standard Snowflake table
CREATE OR REPLACE TABLE FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS (
    VERSION_ID INTEGER IDENTITY(1,1) PRIMARY KEY, -- Snowflake uses IDENTITY
    FORECAST_ID INTEGER NOT NULL, -- REFERENCES FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST(ID), -- FK can be added if desired
    VERSION_NUMBER INTEGER NOT NULL,
    MARKET_NAME VARCHAR(100),
    MARKET_CODE VARCHAR(50),
    DISTRIBUTOR_NAME VARCHAR(100),
    DISTRIBUTOR_ID VARCHAR(50),
    BRAND VARCHAR(100),
    BRAND_ID VARCHAR(50),
    VARIANT VARCHAR(100),
    VARIANT_ID VARCHAR(50),
    VARIANT_SIZE_PACK_DESC VARCHAR(100),
    VARIANT_SIZE_PACK_ID VARCHAR(50),
    FORECAST_YEAR INTEGER,
    MONTH INTEGER,
    FORECAST_METHOD VARCHAR(50),
    FORECAST_GENERATION_MONTH_DATE DATE,
    MANUAL_CASE_EQUIVALENT_VOLUME NUMBER, -- Changed NUMERIC to NUMBER
    UPDATED_BY_USER_ID VARCHAR(50),
    FORECAST_STATUS VARCHAR(50),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), -- Changed TIMESTAMP to TIMESTAMP_NTZ
    COMMENT TEXT
);

-- Add Foreign Key constraint if desired (ensure parent table is created first)
-- ALTER TABLE FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS
-- ADD CONSTRAINT FK_VERSIONS_TO_FORECAST FOREIGN KEY (FORECAST_ID) 
-- REFERENCES FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST(ID);


-- 4. Create index for efficient version retrieval (Original: ON FORECAST.manual_input_depletions_forecast_versions(forecast_id, version_number);)
-- Note: For standard Snowflake tables, consider clustering keys for very large tables based on query patterns.
-- For example: ALTER TABLE FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS CLUSTER BY (FORECAST_ID, VERSION_NUMBER);
-- Or rely on Snowflake's natural micro-partitioning. Indexing is less common for this type of table.

-- 5. Create composite indexes for the main table (Original: ON FORECAST.manual_input_depletions_forecast(...);)
-- Note: The Hybrid Table above has a secondary index defined (idx_manual_forecast_keys). 
-- Additional optimization can be done via query tuning or Snowflake's Search Optimization Service if needed.

COMMENT ON TABLE FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST IS 'Stores the current state of manual forecast inputs. Implemented as a Hybrid Table for fast edits.';
COMMENT ON TABLE FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS IS 'Append-only table storing version history of manual forecast inputs.';

-- Note: Derived marts like depletions_forecast_init_draft and its _chains variant should be regenerated via dbt after any remap operation to reflect canonical IDs. Published tables and primary method tables will be updated by the remap SP or regenerated where applicable. 