-- =================================================================================================
-- Hybri...
--
-- Pattern: Preserve IDs and Manually Control the Sequence
--
-- Why this pattern is used:
-- When backfilling data from an external source (like a PostgreSQL backup) that already has
-- established primary key IDs, using Snowflake's `IDENTITY` or `AUTOINCREMENT` feature directly
-- in the `CREATE TABLE` statement can cause "duplicate key" errors. This is because Snowflake's
-- internal sequence counter for the column doesn't automatically update to the maximum value of
-- the data being loaded.
--
-- This is especially critical for tables where the original ID is used as a foreign key in other
-- tables. Allowing Snowflake to generate *new* IDs during the backfill would break these
-- relationships, orphaning historical data (e.g., forecast versions).
--
-- The solution is a robust, three-step, one-time setup process for each affected table:
--
-- 1.  **Define a Simple Primary Key:** In the `CREATE TABLE` DDL, define the ID column as a
--     simple `INTEGER PRIMARY KEY` without `AUTOINCREMENT` or `IDENTITY`. This allows the
--     `CREATE TABLE ... AS SELECT` statement to load the original, correct IDs from the backup,
--     preserving all foreign key relationships.
--
-- 2.  **Create and Set a Dedicated Sequence:** After the data is loaded, create a separate
--     `SEQUENCE` object. Then, find the maximum value of the ID column in the newly loaded table
--     and set the sequence's next value to start right after it (`MAX(ID) + 1`). This ensures
--     there will be no future collisions.
--
-- 3.  **Link the Sequence to the Table:** Alter the table's ID column to use the newly created
--     and correctly positioned sequence as its `DEFAULT` value for all future `INSERT` operations.
--
-- This pattern ensures data integrity during the backfill and guarantees safe, conflict-free
-- primary key generation for all subsequent application usage.
-- =================================================================================================
USE ROLE BACKEND_ENG_ROLE;
-- =================================================================================================
-- Table: apollo_williamgrant.public.util_data
-- =================================================================================================
-- Step 1: Create a temporary staging table to hold the backfill data.
CREATE OR REPLACE TEMPORARY TABLE temp_util_data_staging AS
SELECT
    id,
    table_name,
    data,
    created_at,
    updated_at
FROM estuary_db.estuary_schema.util_data
WHERE "_meta/op" != 'd';

-- Step 2: Create a sequence starting after the max existing ID from the staging table.
BEGIN
    LET create_sequence_sql VARCHAR := 'CREATE OR REPLACE SEQUENCE apollo_williamgrant.public.seq_util_data_id START = ' || (SELECT COALESCE(MAX(id), 0) + 1 FROM temp_util_data_staging) || 'INCREMENT = 1 ORDER';
    EXECUTE IMMEDIATE create_sequence_sql;
    RETURN create_sequence_sql;
END;

-- Step 3: Create the final Hybrid Table with the sequence default in its definition.
CREATE OR REPLACE HYBRID TABLE apollo_williamgrant.public.util_data (
    id INTEGER PRIMARY KEY DEFAULT apollo_williamgrant.public.seq_util_data_id.nextval,
    table_name VARCHAR(255) NOT NULL,
    data VARIANT NOT NULL,
    created_at TIMESTAMP_NTZ NOT NULL,
    updated_at TIMESTAMP_NTZ NOT NULL
);

-- Step 4: Insert the backfill data from the staging table into the final table.
INSERT INTO apollo_williamgrant.public.util_data (id, table_name, data, created_at, updated_at)
SELECT * FROM temp_util_data_staging;

-- =================================================================================================
-- Table: apollo_williamgrant.public.user_users
-- =================================================================================================
-- Step 1: Stage data
CREATE OR REPLACE TEMPORARY TABLE temp_user_users_staging AS
SELECT
    id,
    email,
    first_name,
    last_name,
    address,
    city,
    state_code,
    zip,
    password_hash,
    role,
    user_access,
    user_settings,
    created_at,
    updated_at,
    dashboard,
    reset_token,
    reset_token_expires,
    phone_number,
    phone_verified,
    two_fa_enabled,
    two_fa_backup_codes,
    two_fa_setup_completed
FROM estuary_db.estuary_schema.user_users
WHERE "_meta/op" != 'd';

-- Step 2: Create sequence
BEGIN
    LET create_sequence_sql VARCHAR := 'CREATE OR REPLACE SEQUENCE apollo_williamgrant.public.seq_user_users_id START = ' || (SELECT COALESCE(MAX(id), 0) + 1 FROM temp_user_users_staging) || 'INCREMENT = 1 ORDER';
    EXECUTE IMMEDIATE create_sequence_sql;
    RETURN create_sequence_sql;
END;

-- Step 3: Create final table
CREATE OR REPLACE HYBRID TABLE apollo_williamgrant.public.user_users (
    id INTEGER PRIMARY KEY DEFAULT apollo_williamgrant.public.seq_user_users_id.nextval,
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    address VARCHAR(255),
    city VARCHAR(255),
    state_code VARCHAR(255),
    zip VARCHAR(255),
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL,
    user_access VARIANT,
    user_settings VARIANT,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    dashboard VARIANT,
    reset_token VARCHAR(255),
    reset_token_expires TIMESTAMP_TZ,
    phone_number VARCHAR(20),
    phone_verified BOOLEAN,
    two_fa_enabled BOOLEAN,
    two_fa_backup_codes ARRAY,
    two_fa_setup_completed BOOLEAN,
    INDEX idx_user_users_phone_number (PHONE_NUMBER),
    INDEX idx_user_users_reset_token (reset_token),
    INDEX idx_user_users_two_fa_enabled (TWO_FA_ENABLED)
);

-- Step 4: Insert from staging
INSERT INTO apollo_williamgrant.public.user_users
SELECT * FROM temp_user_users_staging;

-- =================================================================================================
-- Table: apollo_williamgrant.public.user_audit_logs
-- =================================================================================================
-- Step 1: Stage data
CREATE OR REPLACE TEMPORARY TABLE temp_user_audit_logs_staging AS
SELECT
    id,
    user_id,
    action_type,
    action_timestamp,
    ip_address,
    action,
    status,
    details,
    created_at
FROM estuary_db.estuary_schema.user_audit_logs
WHERE "_meta/op" != 'd';

-- Step 2: Create sequence
BEGIN
    LET create_sequence_sql VARCHAR := 'CREATE OR REPLACE SEQUENCE apollo_williamgrant.public.seq_user_audit_logs_id START = ' || (SELECT COALESCE(MAX(id), 0) + 1 FROM temp_user_audit_logs_staging) || 'INCREMENT = 1 ORDER';
    EXECUTE IMMEDIATE create_sequence_sql;
    RETURN create_sequence_sql;
END;

-- Step 3: Create final table
CREATE OR REPLACE HYBRID TABLE apollo_williamgrant.public.user_audit_logs (
    id INTEGER PRIMARY KEY DEFAULT apollo_williamgrant.public.seq_user_audit_logs_id.nextval,
    user_id INTEGER NOT NULL,
    action_type VARCHAR(20),
    action_timestamp TIMESTAMP_TZ,
    ip_address VARCHAR(50),
    action VARCHAR(255),
    status VARCHAR(50),
    details VARIANT,
    created_at TIMESTAMP_TZ
);

-- Step 4: Insert from staging
INSERT INTO apollo_williamgrant.public.user_audit_logs
SELECT * FROM temp_user_audit_logs_staging;


-- =================================================================================================
-- Table: apollo_williamgrant.master_data.market_settings
-- =================================================================================================
-- Step 1: Stage data
CREATE OR REPLACE TEMPORARY TABLE temp_market_settings_staging AS
SELECT
    id,
    market_code,
    market_name,
    PARSE_JSON(settings) as settings
FROM estuary_db.estuary_schema.wg_master_data_market_settings
WHERE "_meta/op" != 'd';

-- Step 2: Create sequence
BEGIN
    LET create_sequence_sql VARCHAR := 'CREATE OR REPLACE SEQUENCE apollo_williamgrant.master_data.seq_market_settings_id START = ' || (SELECT COALESCE(MAX(id), 0) + 1 FROM temp_market_settings_staging) || 'INCREMENT = 1 ORDER';
    EXECUTE IMMEDIATE create_sequence_sql;
    RETURN create_sequence_sql;
END;

-- Step 3: Create final table
CREATE OR REPLACE HYBRID TABLE apollo_williamgrant.master_data.market_settings (
    id INTEGER PRIMARY KEY DEFAULT apollo_williamgrant.master_data.seq_market_settings_id.nextval,
    market_code VARCHAR(10) NOT NULL,
    market_name VARCHAR(100) NOT NULL,
    settings VARIANT NOT NULL
);

-- Step 4: Insert from staging
INSERT INTO apollo_williamgrant.master_data.market_settings
SELECT * FROM temp_market_settings_staging;


-- =================================================================================================
-- Table: APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD
-- Note: This table has a natural composite primary key, so the sequence pattern is not needed.
-- =================================================================================================
CREATE OR REPLACE HYBRID TABLE APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD (
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
) AS
SELECT
    MARKET_NAME,
    MARKET_CODE,
    DISTRIBUTOR_ID,
    VARIANT_SIZE_PACK_DESC,
    VARIANT_SIZE_PACK_ID,
    FORECAST_METHOD,
    IS_PRIMARY_FORECAST_METHOD,
    FORECAST_GENERATION_MONTH_DATE
FROM estuary_db.estuary_schema.WG_FORECAST_DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD
WHERE "_meta/op" != 'd';

COMMENT ON TABLE APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD IS
'Stores the primary forecast method for a given product combination. This is a Hybrid Table to allow for transactional updates from the application, while being populated with defaults by a dbt model.';


-- =================================================================================================
-- Table: APOLLO_WILLIAMGRANT.FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST
-- =================================================================================================
-- Step 1: Stage data
CREATE OR REPLACE TEMPORARY TABLE temp_manual_forecast_staging AS
SELECT
    ID,
    MARKET_NAME,
    MARKET_CODE,
    DISTRIBUTOR_NAME,
    DISTRIBUTOR_ID,
    BRAND,
    BRAND_ID,
    VARIANT,
    VARIANT_ID,
    VARIANT_SIZE_PACK_DESC,
    VARIANT_SIZE_PACK_ID,
    FORECAST_YEAR,
    MONTH,
    FORECAST_METHOD,
    FORECAST_GENERATION_MONTH_DATE,
    MANUAL_CASE_EQUIVALENT_VOLUME,
    UPDATED_BY_USER_ID,
    FORECAST_STATUS,
    UPDATED_AT,
    CURRENT_VERSION,
    COMMENT
FROM estuary_db.estuary_schema.WG_FORECAST_MANUAL_INPUT_DEPLETIONS_FORECAST
WHERE "_meta/op" != 'd';

-- Step 2: Create sequence
BEGIN
    LET create_sequence_sql VARCHAR := 'CREATE OR REPLACE SEQUENCE APOLLO_WILLIAMGRANT.FORECAST.SEQ_MANUAL_FORECAST_ID START = ' || (SELECT COALESCE(MAX(ID), 0) + 1 FROM temp_manual_forecast_staging) || 'INCREMENT = 1 ORDER';
    EXECUTE IMMEDIATE create_sequence_sql;
    RETURN create_sequence_sql;
END;

-- Step 3: Create final table
-- CREATE OR REPLACE HYBRID TABLE APOLLO_WILLIAMGRANT.FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST (
--     ID INTEGER PRIMARY KEY DEFAULT APOLLO_WILLIAMGRANT.FORECAST.SEQ_MANUAL_FORECAST_ID.NEXTVAL,
--     MARKET_NAME VARCHAR(100),
--     MARKET_CODE VARCHAR(50),
--     DISTRIBUTOR_NAME VARCHAR(100),
--     DISTRIBUTOR_ID VARCHAR(50),
--     BRAND VARCHAR(100),
--     BRAND_ID VARCHAR(50),
--     VARIANT VARCHAR(100),
--     VARIANT_ID VARCHAR(50),
--     VARIANT_SIZE_PACK_DESC VARCHAR(100),
--     VARIANT_SIZE_PACK_ID VARCHAR(50),
--     FORECAST_YEAR INTEGER,
--     MONTH INTEGER,
--     FORECAST_METHOD VARCHAR(50),
--     FORECAST_GENERATION_MONTH_DATE DATE,
--     MANUAL_CASE_EQUIVALENT_VOLUME FLOAT,
--     UPDATED_BY_USER_ID VARCHAR(50),
--     FORECAST_STATUS VARCHAR(50),
--     UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
--     CURRENT_VERSION INTEGER DEFAULT 1,
--     COMMENT TEXT,
--     INDEX idx_manual_forecast_keys (MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE),
--     INDEX idx_manual_forecast_composite (MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID, FORECAST_GENERATION_MONTH_DATE),
--     CONSTRAINT UQ_MANUAL_FORECAST_KEY_FGMD UNIQUE (MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE)
-- );

-- Step 3: Create final table
CREATE OR REPLACE TABLE APOLLO_WILLIAMGRANT.FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST (
    ID INTEGER PRIMARY KEY DEFAULT APOLLO_WILLIAMGRANT.FORECAST.SEQ_MANUAL_FORECAST_ID.NEXTVAL,
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
    MANUAL_CASE_EQUIVALENT_VOLUME FLOAT,
    UPDATED_BY_USER_ID VARCHAR(50),
    FORECAST_STATUS VARCHAR(50),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CURRENT_VERSION INTEGER DEFAULT 1,
    COMMENT TEXT,
    CONSTRAINT UQ_MANUAL_FORECAST_KEY_FGMD UNIQUE (MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE)
)
CLUSTER BY (FORECAST_GENERATION_MONTH_DATE, MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID);

-- Step 4: Insert from staging
INSERT INTO APOLLO_WILLIAMGRANT.FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST
SELECT * FROM temp_manual_forecast_staging;

-- =================================================================================================
-- Table: APOLLO_WILLIAMGRANT.FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS
-- =================================================================================================
-- Step 1: Stage data
CREATE OR REPLACE TEMPORARY TABLE temp_manual_forecast_versions_staging AS
SELECT
    VERSION_ID,
    FORECAST_ID,
    VERSION_NUMBER,
    MARKET_NAME,
    MARKET_CODE,
    DISTRIBUTOR_NAME,
    DISTRIBUTOR_ID,
    BRAND,
    BRAND_ID,
    VARIANT,
    VARIANT_ID,
    VARIANT_SIZE_PACK_DESC,
    VARIANT_SIZE_PACK_ID,
    FORECAST_YEAR,
    MONTH,
    FORECAST_METHOD,
    FORECAST_GENERATION_MONTH_DATE,
    MANUAL_CASE_EQUIVALENT_VOLUME,
    UPDATED_BY_USER_ID,
    FORECAST_STATUS,
    CREATED_AT,
    COMMENT
FROM estuary_db.estuary_schema.WG_FORECAST_MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS
WHERE "_meta/op" != 'd';

-- Step 2: Create sequence
BEGIN
    LET create_sequence_sql VARCHAR := 'CREATE OR REPLACE SEQUENCE APOLLO_WILLIAMGRANT.FORECAST.SEQ_MANUAL_FORECAST_VERSIONS_ID START = ' || (SELECT COALESCE(MAX(VERSION_ID), 0) + 1 FROM temp_manual_forecast_versions_staging) || 'INCREMENT = 1 ORDER';
    EXECUTE IMMEDIATE create_sequence_sql;
    RETURN create_sequence_sql;
END;

-- Step 3: Create final table
-- CREATE OR REPLACE HYBRID TABLE APOLLO_WILLIAMGRANT.FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS (
--     VERSION_ID INTEGER PRIMARY KEY DEFAULT APOLLO_WILLIAMGRANT.FORECAST.SEQ_MANUAL_FORECAST_VERSIONS_ID.NEXTVAL,
--     FORECAST_ID INTEGER NOT NULL,
--     VERSION_NUMBER INTEGER NOT NULL,
--     MARKET_NAME VARCHAR(100),
--     MARKET_CODE VARCHAR(50),
--     DISTRIBUTOR_NAME VARCHAR(100),
--     DISTRIBUTOR_ID VARCHAR(50),
--     BRAND VARCHAR(100),
--     BRAND_ID VARCHAR(50),
--     VARIANT VARCHAR(100),
--     VARIANT_ID VARCHAR(50),
--     VARIANT_SIZE_PACK_DESC VARCHAR(100),
--     VARIANT_SIZE_PACK_ID VARCHAR(50),
--     FORECAST_YEAR INTEGER,
--     MONTH INTEGER,
--     FORECAST_METHOD VARCHAR(50),
--     FORECAST_GENERATION_MONTH_DATE DATE,
--     MANUAL_CASE_EQUIVALENT_VOLUME FLOAT,
--     UPDATED_BY_USER_ID VARCHAR(50),
--     FORECAST_STATUS VARCHAR(50),
--     CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
--     COMMENT TEXT,
--     FOREIGN KEY (FORECAST_ID) REFERENCES APOLLO_WILLIAMGRANT.FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST(ID),
--     INDEX idx_manual_forecast_versions_id_version (FORECAST_ID, VERSION_NUMBER)
-- );

-- Step 3: Create final table
CREATE OR REPLACE TABLE APOLLO_WILLIAMGRANT.FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS (
    VERSION_ID INTEGER PRIMARY KEY DEFAULT APOLLO_WILLIAMGRANT.FORECAST.SEQ_MANUAL_FORECAST_VERSIONS_ID.NEXTVAL,
    FORECAST_ID INTEGER NOT NULL,
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
    MANUAL_CASE_EQUIVALENT_VOLUME FLOAT,
    UPDATED_BY_USER_ID VARCHAR(50),
    FORECAST_STATUS VARCHAR(50),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    COMMENT TEXT
);

-- Step 4: Insert from staging
INSERT INTO APOLLO_WILLIAMGRANT.FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS
SELECT * FROM temp_manual_forecast_versions_staging;

COMMENT ON TABLE APOLLO_WILLIAMGRANT.FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST IS 'Stores the current state of manual forecast inputs. Implemented as a Hybrid Table for fast edits.';
COMMENT ON TABLE APOLLO_WILLIAMGRANT.FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS IS 'Append-only table storing version history of manual forecast inputs.';


-- =================================================================================================
-- Table: APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS
-- =================================================================================================
-- Step 1: Stage data
CREATE OR REPLACE TEMPORARY TABLE temp_publication_groups_staging AS
SELECT
    GROUP_ID,
    DIVISION,
    PUBLICATION_DATE,
    INITIATED_BY_USER_ID,
    FORECAST_GENERATION_MONTH_DATE,
    PUBLICATION_NOTE,
    GROUP_STATUS
FROM estuary_db.estuary_schema.WG_FORECAST_DEPLETIONS_FORECAST_PUBLICATION_GROUPS
WHERE "_meta/op" != 'd';

-- Step 2: Create sequence
BEGIN
    LET create_sequence_sql VARCHAR := 'CREATE OR REPLACE SEQUENCE APOLLO_WILLIAMGRANT.FORECAST.SEQ_PUBLICATION_GROUPS_ID START = ' || (SELECT COALESCE(MAX(GROUP_ID), 0) + 1 FROM temp_publication_groups_staging) || 'INCREMENT = 1 ORDER';
    EXECUTE IMMEDIATE create_sequence_sql;
    RETURN create_sequence_sql;
END;

-- Step 3: Create final table
CREATE OR REPLACE HYBRID TABLE APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS (
    GROUP_ID INTEGER PRIMARY KEY DEFAULT APOLLO_WILLIAMGRANT.FORECAST.SEQ_PUBLICATION_GROUPS_ID.NEXTVAL,
    DIVISION VARCHAR(100) NOT NULL,
    PUBLICATION_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    INITIATED_BY_USER_ID VARCHAR(50) NOT NULL,
    FORECAST_GENERATION_MONTH_DATE DATE NOT NULL,
    PUBLICATION_NOTE TEXT,
    GROUP_STATUS VARCHAR(50) DEFAULT 'active'
);

-- Step 4: Insert from staging
INSERT INTO APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS
SELECT * FROM temp_publication_groups_staging;

COMMENT ON TABLE APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS IS 'Stores division-level publication groupings for forecasts. Implemented as a Hybrid Table.';


-- =================================================================================================
-- Table: APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS
-- =================================================================================================
-- Step 1: Stage data
CREATE OR REPLACE TEMPORARY TABLE temp_publications_staging AS
SELECT
    PUBLICATION_ID,
    GROUP_ID,
    MARKET_CODE,
    PUBLISHED_BY_USER_ID,
    FORECAST_GENERATION_MONTH_DATE,
    PUBLICATION_NOTE,
    PUBLICATION_STATUS,
    APPROVAL_STATUS_DATE
FROM estuary_db.estuary_schema.WG_FORECAST_DEPLETIONS_FORECAST_PUBLICATIONS
WHERE "_meta/op" != 'd';

-- Step 2: Create sequence
BEGIN
    LET create_sequence_sql VARCHAR := 'CREATE OR REPLACE SEQUENCE APOLLO_WILLIAMGRANT.FORECAST.SEQ_PUBLICATIONS_ID START = ' || (SELECT COALESCE(MAX(PUBLICATION_ID), 0) + 1 FROM temp_publications_staging) || 'INCREMENT = 1 ORDER';
    EXECUTE IMMEDIATE create_sequence_sql;
    RETURN create_sequence_sql;
END;

-- Step 3: Create final table
CREATE OR REPLACE HYBRID TABLE APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS (
    PUBLICATION_ID INTEGER PRIMARY KEY DEFAULT APOLLO_WILLIAMGRANT.FORECAST.SEQ_PUBLICATIONS_ID.NEXTVAL,
    GROUP_ID INTEGER,
    MARKET_CODE VARCHAR(50) NOT NULL,
    PUBLISHED_BY_USER_ID VARCHAR(50) NOT NULL,
    FORECAST_GENERATION_MONTH_DATE DATE NOT NULL,
    PUBLICATION_NOTE TEXT,
    PUBLICATION_STATUS VARCHAR(50) DEFAULT 'review',
    APPROVAL_STATUS_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (GROUP_ID) REFERENCES APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS(GROUP_ID),
    INDEX idx_pub_market_fgmd (MARKET_CODE, FORECAST_GENERATION_MONTH_DATE, PUBLICATION_STATUS)
);

-- Step 4: Insert from staging
INSERT INTO APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS
SELECT * FROM temp_publications_staging;

COMMENT ON TABLE APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS IS 'Stores market-level forecast publication details and status. Implemented as a Hybrid Table.';


-- =================================================================================================
-- Table: APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS
-- =================================================================================================
-- Step 1: Stage data
CREATE OR REPLACE TEMPORARY TABLE temp_published_forecasts_staging AS
SELECT
    ID,
    GROUP_ID,
    PUBLICATION_ID,
    SOURCE_TABLE,
    SOURCE_ID,
    MARKET_NAME,
    MARKET_CODE,
    DISTRIBUTOR_NAME,
    DISTRIBUTOR_ID,
    BRAND,
    BRAND_ID,
    VARIANT,
    VARIANT_ID,
    VARIANT_SIZE_PACK_DESC,
    VARIANT_SIZE_PACK_ID,
    FORECAST_YEAR,
    MONTH,
    FORECAST_METHOD,
    FORECAST_GENERATION_MONTH_DATE,
    CASE_EQUIVALENT_VOLUME,
    VERSION_NUMBER,
    PUBLISHED_AT
FROM estuary_db.estuary_schema.WG_FORECAST_DEPLETIONS_FORECAST_PUBLISHED_FORECASTS
WHERE "_meta/op" != 'd';

-- Step 2: Create sequence
BEGIN
    LET create_sequence_sql VARCHAR := 'CREATE OR REPLACE SEQUENCE APOLLO_WILLIAMGRANT.FORECAST.SEQ_PUBLISHED_FORECASTS_ID START = ' || (SELECT COALESCE(MAX(ID), 0) + 1 FROM temp_published_forecasts_staging) || 'INCREMENT = 1 ORDER';
    EXECUTE IMMEDIATE create_sequence_sql;
    RETURN create_sequence_sql;
END;

-- Step 3: Create final table
-- CREATE OR REPLACE HYBRID TABLE APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS (
--     ID BIGINT PRIMARY KEY DEFAULT APOLLO_WILLIAMGRANT.FORECAST.SEQ_PUBLISHED_FORECASTS_ID.NEXTVAL,
--     GROUP_ID INTEGER,
--     PUBLICATION_ID INTEGER NOT NULL,
--     SOURCE_TABLE VARCHAR(50) NOT NULL,
--     SOURCE_ID INTEGER,
--     MARKET_NAME VARCHAR(100),
--     MARKET_CODE VARCHAR(50) NOT NULL,
--     DISTRIBUTOR_NAME VARCHAR(100),
--     DISTRIBUTOR_ID VARCHAR(50) NOT NULL,
--     BRAND VARCHAR(100),
--     BRAND_ID VARCHAR(50),
--     VARIANT VARCHAR(100),
--     VARIANT_ID VARCHAR(50),
--     VARIANT_SIZE_PACK_DESC VARCHAR(100),
--     VARIANT_SIZE_PACK_ID VARCHAR(50) NOT NULL,
--     FORECAST_YEAR INTEGER NOT NULL,
--     MONTH INTEGER NOT NULL,
--     FORECAST_METHOD VARCHAR(50) NOT NULL,
--     FORECAST_GENERATION_MONTH_DATE DATE NOT NULL,
--     CASE_EQUIVALENT_VOLUME FLOAT NOT NULL,
--     VERSION_NUMBER INTEGER,
--     PUBLISHED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
--     FOREIGN KEY (GROUP_ID) REFERENCES APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS(GROUP_ID),
--     FOREIGN KEY (PUBLICATION_ID) REFERENCES APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS(PUBLICATION_ID),
--     INDEX idx_depletions_forecast_published_forecasts_publication (PUBLICATION_ID),
--     INDEX idx_published_forecasts_market (MARKET_CODE, FORECAST_GENERATION_MONTH_DATE),
--     INDEX idx_published_forecasts_composite (MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE),
--     INDEX idx_depletions_forecast_published_forecasts_covering (market_code, distributor_id, brand, variant_size_pack_id, forecast_year, month, forecast_method, forecast_generation_month_date) INCLUDE (case_equivalent_volume),
--     INDEX idx_depletions_forecast_published_forecasts_market_code (market_code, forecast_method, forecast_generation_month_date),
--     INDEX idx_depletions_forecast_published_forecasts_market_distributor_id (market_code, distributor_id, forecast_method, forecast_generation_month_date),
--     INDEX idx_depletions_forecast_published_forecasts_market_brand (market_code, brand, forecast_method, forecast_generation_month_date),
--     INDEX idx_depletions_forecast_published_forecasts_market_size_pack (market_code, variant_size_pack_id, forecast_method, forecast_generation_month_date)
-- );

-- Step 3: Create final table
CREATE OR REPLACE TABLE APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS (
    ID BIGINT PRIMARY KEY DEFAULT APOLLO_WILLIAMGRANT.FORECAST.SEQ_PUBLISHED_FORECASTS_ID.NEXTVAL,
    GROUP_ID INTEGER,
    PUBLICATION_ID INTEGER NOT NULL,
    SOURCE_TABLE VARCHAR(50) NOT NULL,
    SOURCE_ID INTEGER,
    MARKET_NAME VARCHAR(100),
    MARKET_CODE VARCHAR(50) NOT NULL,
    DISTRIBUTOR_NAME VARCHAR(100),
    DISTRIBUTOR_ID VARCHAR(50) NOT NULL,
    BRAND VARCHAR(100),
    BRAND_ID VARCHAR(50),
    VARIANT VARCHAR(100),
    VARIANT_ID VARCHAR(50),
    VARIANT_SIZE_PACK_DESC VARCHAR(100),
    VARIANT_SIZE_PACK_ID VARCHAR(50) NOT NULL,
    FORECAST_YEAR INTEGER NOT NULL,
    MONTH INTEGER NOT NULL,
    FORECAST_METHOD VARCHAR(50) NOT NULL,
    FORECAST_GENERATION_MONTH_DATE DATE NOT NULL,
    CASE_EQUIVALENT_VOLUME FLOAT NOT NULL,
    VERSION_NUMBER INTEGER,
    PUBLISHED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (FORECAST_GENERATION_MONTH_DATE, MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID);

-- Step 4: Insert from staging
INSERT INTO APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS
SELECT * FROM temp_published_forecasts_staging;

COMMENT ON TABLE APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS IS 'Stores the actual forecast data that has been published. Implemented as a Hybrid Table for instant read-after-write/delete consistency for the application.';


-- =================================================================================================
-- Table: APOLLO_WILLIAMGRANT.MASTER_DATA.APOLLO_PRODUCT_TAGS
-- =================================================================================================
-- Step 1: Stage data
CREATE OR REPLACE TEMPORARY TABLE temp_apollo_product_tags_staging AS
SELECT
    TAG_ID,
    TAG_NAME
FROM estuary_db.estuary_schema.WG_MASTER_DATA_APOLLO_PRODUCT_TAGS
WHERE "_meta/op" != 'd';

-- Step 2: Create sequence
BEGIN
    LET create_sequence_sql VARCHAR := 'CREATE OR REPLACE SEQUENCE APOLLO_WILLIAMGRANT.MASTER_DATA.SEQ_APOLLO_PRODUCT_TAGS_ID START = ' || (SELECT COALESCE(MAX(TAG_ID), 0) + 1 FROM temp_apollo_product_tags_staging) || 'INCREMENT = 1 ORDER';
    EXECUTE IMMEDIATE create_sequence_sql;
    RETURN create_sequence_sql;
END;

-- Step 3: Create final table
CREATE OR REPLACE HYBRID TABLE APOLLO_WILLIAMGRANT.MASTER_DATA.APOLLO_PRODUCT_TAGS (
  TAG_ID INTEGER PRIMARY KEY DEFAULT APOLLO_WILLIAMGRANT.MASTER_DATA.SEQ_APOLLO_PRODUCT_TAGS_ID.NEXTVAL,
  TAG_NAME VARCHAR UNIQUE NOT NULL
);

-- Step 4: Insert from staging
INSERT INTO APOLLO_WILLIAMGRANT.MASTER_DATA.APOLLO_PRODUCT_TAGS
SELECT * FROM temp_apollo_product_tags_staging;

COMMENT ON TABLE APOLLO_WILLIAMGRANT.MASTER_DATA.APOLLO_PRODUCT_TAGS IS 'Stores unique product tags. Implemented as a Hybrid Table.';


-- =================================================================================================
-- Table: APOLLO_WILLIAMGRANT.MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
-- =================================================================================================
-- Note: This table uses a natural primary key, so the sequence pattern is not needed.
-- =================================================================================================
CREATE OR REPLACE HYBRID TABLE APOLLO_WILLIAMGRANT.MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG (
  VARIANT_SIZE_PACK_ID VARCHAR PRIMARY KEY,
  VARIANT_SIZE_PACK_DESC VARCHAR,
  TAG_IDS ARRAY,
  TAG_NAMES ARRAY
) AS
SELECT
    VARIANT_SIZE_PACK_ID,
    VARIANT_SIZE_PACK_DESC,
    TAG_ID AS TAG_IDS,
    TAG_NAME AS TAG_NAMES
FROM estuary_db.estuary_schema.WG_MASTER_DATA_APOLLO_VARIANT_SIZE_PACK_TAG
WHERE "_meta/op" != 'd';

COMMENT ON TABLE APOLLO_WILLIAMGRANT.MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG IS 'Maps variant_size_pack_id to an array of product tag IDs and names. Implemented as a Hybrid Table.';


------------------------------------------------------------------------------
--  PUBLIC schema
------------------------------------------------------------------------------
-- -- util_data
-- GRANT OWNERSHIP ON SEQUENCE APOLLO_WILLIAMGRANT.PUBLIC.SEQ_UTIL_DATA_ID     TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;
-- GRANT OWNERSHIP ON TABLE    APOLLO_WILLIAMGRANT.PUBLIC.UTIL_DATA            TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;

-- -- user_users
-- GRANT OWNERSHIP ON SEQUENCE APOLLO_WILLIAMGRANT.PUBLIC.SEQ_USER_USERS_ID     TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;
-- GRANT OWNERSHIP ON TABLE    APOLLO_WILLIAMGRANT.PUBLIC.USER_USERS           TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;

-- -- user_audit_logs
-- GRANT OWNERSHIP ON SEQUENCE APOLLO_WILLIAMGRANT.PUBLIC.SEQ_USER_AUDIT_LOGS_ID TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;
-- GRANT OWNERSHIP ON TABLE    APOLLO_WILLIAMGRANT.PUBLIC.USER_AUDIT_LOGS        TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;

-- ------------------------------------------------------------------------------
-- --  MASTER_DATA schema
-- ------------------------------------------------------------------------------
-- -- market_settings
-- GRANT OWNERSHIP ON SEQUENCE APOLLO_WILLIAMGRANT.MASTER_DATA.SEQ_MARKET_SETTINGS_ID TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;
-- GRANT OWNERSHIP ON TABLE    APOLLO_WILLIAMGRANT.MASTER_DATA.MARKET_SETTINGS        TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;

-- -- apollo_product_tags
-- GRANT OWNERSHIP ON SEQUENCE APOLLO_WILLIAMGRANT.MASTER_DATA.SEQ_APOLLO_PRODUCT_TAGS_ID TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;
-- GRANT OWNERSHIP ON TABLE    APOLLO_WILLIAMGRANT.MASTER_DATA.APOLLO_PRODUCT_TAGS        TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;

-- -- apollo_variant_size_pack_tag
-- GRANT OWNERSHIP ON TABLE    APOLLO_WILLIAMGRANT.MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;

-- ------------------------------------------------------------------------------
-- --  FORECAST schema
-- ------------------------------------------------------------------------------
-- -- primary_forecast_method  (natural PK – no sequence)
-- GRANT OWNERSHIP ON TABLE APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD        TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;

-- -- manual_input_depletions_forecast
-- GRANT OWNERSHIP ON SEQUENCE APOLLO_WILLIAMGRANT.FORECAST.SEQ_MANUAL_FORECAST_ID                            TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;
-- GRANT OWNERSHIP ON TABLE    APOLLO_WILLIAMGRANT.FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST                 TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;

-- -- manual_input_depletions_forecast_versions
-- GRANT OWNERSHIP ON SEQUENCE APOLLO_WILLIAMGRANT.FORECAST.SEQ_MANUAL_FORECAST_VERSIONS_ID                  TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;
-- GRANT OWNERSHIP ON TABLE    APOLLO_WILLIAMGRANT.FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS        TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;

-- -- publication_groups
-- GRANT OWNERSHIP ON SEQUENCE APOLLO_WILLIAMGRANT.FORECAST.SEQ_PUBLICATION_GROUPS_ID                        TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;
-- GRANT OWNERSHIP ON TABLE    APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS           TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;

-- -- publications
-- GRANT OWNERSHIP ON SEQUENCE APOLLO_WILLIAMGRANT.FORECAST.SEQ_PUBLICATIONS_ID                              TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;
-- GRANT OWNERSHIP ON TABLE    APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS                 TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;

-- -- published_forecasts
-- GRANT OWNERSHIP ON SEQUENCE APOLLO_WILLIAMGRANT.FORECAST.SEQ_PUBLISHED_FORECASTS_ID                       TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;
-- GRANT OWNERSHIP ON TABLE    APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS          TO ROLE BACKEND_ENG_ROLE COPY CURRENT GRANTS;