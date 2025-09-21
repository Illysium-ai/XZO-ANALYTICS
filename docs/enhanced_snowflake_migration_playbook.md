# Enhanced PostgreSQL to Snowflake Migration Playbook for Apollo Analytics

**Version:** 1.4
**Date:** 2025-06-12

## 0. Introduction & Goals

This document provides an enhanced, step-by-step playbook for migrating the Apollo Analytics platform's backend database for the William Grant & Sons (WGS) tenant from PostgreSQL to Snowflake. It utilizes a single Snowflake database `APOLLO_WILLIAMGRANT` with a schema-based organization. Other tenants (e.g., Hotaling) will reside in separate databases.

**Primary Goals of this Migration (for WGS):**

*   **Scalability:** Leverage Snowflake's architecture.
*   **Performance:** Improve dbt transformations and analytical queries.
*   **Maintainability:** Consolidate data processing within Snowflake.
*   **Cost-Effectiveness & Automation:** Implement an efficient, automated, and scalable ingestion pipeline using event-driven S3, an SNS topic for fanning out events, SQS for message queuing (with filtering), a central Lambda function for dynamic table/schema management, and a generic Snowpipe triggered via API. This minimizes manual DDL and reduces reliance on Glue DPUs for raw ingestion.

**Key Architectural Components to Migrate for WGS:**

1.  **Data Ingestion:** Transition from the current AWS Glue job (direct to Postgres) to an event-driven S3 (CSV upload to `s3://apollo-wg/`) -> SNS Topic -> SQS (filtered) -> central Lambda (orchestrating dynamic table/schema management in `APOLLO_WILLIAMGRANT.S3` and triggering Snowpipe) -> Generic Snowpipe API -> Snowflake pipeline for raw data ingestion.
2.  **Data Transformation (Raw-to-Gold):** Utilize Snowflake Streams and Tasks to process data from `APOLLO_WILLIAMGRANT.S3` tables into curated "Gold" tables within schemas like `APOLLO_WILLIAMGRANT.VIP`, `APOLLO_WILLIAMGRANT.MASTER_DATA`, etc., using the `APOLLO_WILLIAMGRANT.UTILITY` schema for the root task controller.
3.  **Data Transformation (dbt):** Adapt the legacy `apollo_dbt` project (as `dbt_williamgrant`) for Snowflake, sourcing from the Gold tables within `APOLLO_WILLIAMGRANT`.
4.  **PostgreSQL Functions:** Migrate critical PL/pgSQL functions to Snowflake Stored Procedures and UDFs within the `APOLLO_WILLIAMGRANT` database (primarily in `FORECAST` and `MASTER_DATA` schemas).
5.  **Operational Data & User Inputs:** Support real-time user overrides using Snowflake capabilities (e.g., Hybrid Tables in `APOLLO_WILLIAMGRANT.FORECAST`).

---

## 1. Pre-Migration Analysis & Preparation

### 1.1. Detailed Codebase Review Summary (Context)

*   **Existing `apollo-glue` (`glue_s3_to_postgres_dynamic.py`):** This job's logic for transformations (cleaning, standardization, aggregation, deduplication) and dynamic table/PK handling provides essential context for the new Snowflake Streams and Tasks that will perform the Raw-to-Gold transformations within Snowflake.
*   **Existing `apollo_dbt`:** Legacy source for the `dbt_williamgrant` project. Models and macros need Snowflake dialect adaptation. PostgreSQL functions in `macros/` require translation.

### 1.2. Confirming Snowflake Suitability (for WGS)

*   **Single Database (`APOLLO_WILLIAMGRANT`):** Confirmed as suitable for WGS.
*   **Hybrid Tables:** Recommended for `APOLLO_WILLIAMGRANT.FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST` and potentially for `APOLLO_WILLIAMGRANT.MASTER_DATA.APOLLO_PRODUCT_TAGS` and `APOLLO_VARIANT_SIZE_PACK_TAG` for fast user edits.

### 1.3. Snowflake Environment Setup Checklist (for WGS)

*   **[ ] Account & Access:** Finalize Snowflake edition, MFA for users.
*   **[X - User Updated Script] Warehouses:** `DATAOPS_WH`, `COMPUTE_WH` (created by `snowflake_raw_env_setup.sql`).
*   **[X - User Updated Script] Database & Schemas (Single DB: `APOLLO_WILLIAMGRANT`):
    *   **`APOLLO_WILLIAMGRANT` (Database):**
        *   `S3` (Schema): For raw CSV data landed via Snowpipe into dynamically created tables, and for ingestion control tables.
        *   `VIP` (Schema): For WGS VIP-related production tables/views (primary target for Streams/Tasks from S3).
        *   `MASTER_DATA` (Schema): For WGS master data production tables/views.
        *   `FORECAST` (Schema): For WGS forecast-related production tables, UDFs, SPs.
        *   `UTILITY` (Schema): For shared UDFs, Stored Procedures, and root task controllers relevant to WGS.
    *   **Consider a `DBT_DEV_<username>` schema within `APOLLO_WILLIAMGRANT` for dbt development.**
*   **[X - User Updated Script] User Roles & Permissions (RBAC):** (`BACKEND_ENG_ROLE`, `ANALYST_ROLE` grants updated for `APOLLO_WILLIAMGRANT` and its schemas. A new `BACKEND_ENG_ROLE` will be needed for the ingestion Lambda).
*   **[ ] Network Policies:** Implement as needed.
*   **[ ] Enable Unistore:** If Hybrid Tables are utilized.
*   **[ ] Resource Monitors:** Set up for cost control.

### 1.4. Tooling & dbt Project Configuration (for `dbt_williamgrant`)

*   **[ ] SnowSQL CLI:** Install and configure.
*   **[ ] dbt Snowflake Adapter:** Install.
*   **[X - User Task] `profiles.yml`:** Updated `apollo_snowflake` profile targeting `APOLLO_WILLIAMGRANT`.
*   **[X - Done] `dbt_project.yml` (in `dbt_williamgrant`):** Name, profile, schema configs updated.
*   **[ ] Version Control:** Git for all scripts.
*   **[ ] Secrets Management:** For Snowflake credentials and Snowpipe private key.

---

## 2. Data Ingestion Pipeline: Automated & Scalable S3 to Snowflake via SNS, SQS, Lambda & Generic Snowpipe (for WGS)

This phase implements an automated, event-driven pipeline: S3 CSV (`s3://apollo-wg/`) -> SNS Topic -> SQS Queue (with filter policy) -> Central Lambda (dynamic table/schema management & Snowpipe trigger) -> Generic Snowpipe (API triggered) -> Snowflake `APOLLO_WILLIAMGRANT.S3` tables. Subsequent transformations to "Gold" tables are handled by Snowflake Streams and Tasks.

### Step 2.1: AWS IAM Configuration for Snowflake S3 Access & Central Lambda
*   **Goal**: Create IAM roles for the central Lambda function and ensure Snowflake's Storage Integration has S3 permissions.
*   **Script**: `apollo-glue/setup_snowflake_lambda_pipeline_iam.sh` (to be created/updated).
*   **Key Actions**:
    1.  **Snowflake Storage Integration Role**: Ensure the IAM role used by your Snowflake Storage Integration (e.g., `S3_INT_APOLLO_WGS`) has `s3:GetObject` and `s3:ListBucket` permissions on `s3://apollo-wg/`.
    2.  **Central Lambda Execution Role (`LambdaS3SnowflakeOrchestratorRole-WGS`)**: Create a new IAM role. It requires permissions for:
        *   SQS: `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes` for the SQS queue.
        *   S3: `s3:GetObject` (for schema inference from `s3://apollo-wg/`), `s3:GetObjectMetadata`. If moving files: `s3:PutObject`, `s3:DeleteObject` for `s3://apollo-wg/` and a potential processed/archive path.
        *   Secrets Manager: `secretsmanager:GetSecretValue` to retrieve Snowflake connection details (e.g., private key or user credentials).
        *   CloudWatch Logs: `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`.
        *   (If not using Key-Pair for Snowflake connection from Lambda, ensure network access to Snowflake).
*   **Status**: `[X - Script Updated]`

### Step 2.2: Snowflake Service User & Authentication for Lambda
*   **Goal**: Create a dedicated Snowflake user and role for the Lambda function, granting necessary permissions for dynamic DDL, DML, and pipe execution.
*   **Snowflake Setup (part of `sql_utils/snowflake_env_setup.sql` or a new script for ingestion-specific objects)**:
    1.  Create a user (e.g., `LAMBDA_SF_USER`).
    2.  Create a role (e.g., `BACKEND_ENG_ROLE`).
    3.  Grant `BACKEND_ENG_ROLE` to `LAMBDA_SF_USER`.
    4.  Grant necessary permissions to `BACKEND_ENG_ROLE`:
        *   `USAGE` on Warehouse (e.g., `DATAOPS_WH`).
        *   `USAGE` on Database `APOLLO_WILLIAMGRANT`.
        *   `USAGE`, `CREATE TABLE` on Schema `APOLLO_WILLIAMGRANT.S3`.
        *   `SELECT` on `APOLLO_WILLIAMGRANT.INFORMATION_SCHEMA.COLUMNS`.
        *   Ability to `ALTER TABLE` for tables within `APOLLO_WILLIAMGRANT.S3` (Ownership or explicit ALTER).
        *   `INSERT`, `SELECT` on `APOLLO_WILLIAMGRANT.S3.INGESTED_FILES`.
        *   `USAGE` on the generic pipe `APOLLO_WILLIAMGRANT.S3.DATA_PIPE`.
        *   Permission to call `SYSTEM$PIPE_INSERT(\'APOLLO_WILLIAMGRANT.S3.DATA_PIPE\', ARRAY(...))` (usually covered by pipe USAGE and role context).
        *   Permission to `SET` session variables.
    5.  Authentication Method: Key-Pair authentication is recommended. Assign the public key to `LAMBDA_SF_USER`. Store the private key securely in AWS Secrets Manager.
*   **Status**: `[ ] TO DO (User Task & Script Verification)`

### Step 2.3: AWS SNS Topic, SQS Queue & S3 Event Notification Setup
*   **Goal**: Decouple S3 events using an SNS topic for fan-out, and ensure the Snowflake Lambda's SQS queue receives only relevant (filtered) messages.
*   **Script**: `apollo-glue/setup_snowflake_lambda_pipeline_aws_resources.sh` (updated).
*   **Key Actions**:
    1.  Create/Verify an SNS Topic (e.g., `s3_object_created_notifications_apollo`).
    2.  Create/Verify the SQS Standard Queue for the Snowflake Lambda (e.g., `s3_to_snowflake_orchestrator_queue_wgs`).
    3.  Subscribe the SQS queue to the SNS topic, enabling `RawMessageDelivery` and applying a **filter policy** to only receive notifications for S3 objects matching a specific prefix (e.g., `upload_sf/`).
    4.  Grant the SNS topic permission to send messages to this SQS queue (update SQS queue policy).
    5.  Configure S3 event notifications on `s3://apollo-wg/` to publish `s3:ObjectCreated:*` events (for a broad prefix like `uploads/` or specific prefixes if S3 notification filtering is still desired here, and suffix like `*.csv.gz`) to the SNS topic.
*   **Status**: `[X - Script Updated]`

### Step 2.4: Central Lambda Function Development & Deployment
*   **Goal**: Create Python Lambda to parse SQS messages (containing S3 events from SNS), manage Snowflake table DDL dynamically, and trigger the generic Snowpipe.
*   **Scripts**:
    *   Python code: `apollo-glue/lambda/lambda_s3_to_snowflake_orchestrator.py` (updated for BRONZE_SCHEMA_NAME, S3 schema, DATA_PIPE).
    *   Deployment handled by `apollo-glue/setup_snowflake_lambda_pipeline_aws_resources.sh`.
*   **Key Logic** (summary, details in script):
    1.  Triggered by SQS message.
    2.  Parse S3 object key from message body.
    3.  Connect to Snowflake.
    4.  Idempotency check against `APOLLO_WILLIAMGRANT.S3.INGESTED_FILES`.
    5.  **Dynamic Table Creation/Evolution**: Read CSV header from S3, create table in `APOLLO_WILLIAMGRANT.S3` if not exists (all columns `VARCHAR`), or alter existing table to add new `VARCHAR` columns.
    6.  Set session variable `TARGET_TABLE_FOR_PIPE`.
    7.  Trigger Snowpipe `APOLLO_WILLIAMGRANT.S3.DATA_PIPE` via `SYSTEM$PIPE_INSERT`.
    8.  Log status in `APOLLO_WILLIAMGRANT.S3.INGESTED_FILES`.
*   **Environment Variables**: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_SECRET_ARN`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `BRONZE_SCHEMA_NAME` (value: `S3`), `SNOWFLAKE_ROLE`, `LANDING_BUCKET_NAME`.
*   **Status**: `[X - Script Updated]`

### Step 2.5: Snowflake Generic Pipe, S3 Schema Objects, Streams, and Task Configuration (within `APOLLO_WILLIAMGRANT`)
*   **Goal**: Define Snowflake objects for the generic ingestion framework and subsequent S3-to-Gold transformations.
*   **Scripts (in `apollo-analytics/sql_utils/`)**:
    1.  **S3 Schema & Common Objects (Script: `snowflake_raw_env_setup.sql`)** (updated):
        *   Creates `APOLLO_WILLIAMGRANT.S3` schema, `INGESTED_FILES` table.
        *   Defines `S3_LANDING_ZONE_INT` storage integration, `LANDING_STAGE` external stage, `GENERIC_CSV_FORMAT` file format.
        *   Creates generic pipe `APOLLO_WILLIAMGRANT.S3.DATA_PIPE` (`AUTO_INGEST = FALSE`).
    2.  **Streams & Tasks (S3-to-Gold Logic) (Script: `snowflake_streams_and_tasks_raw_to_gold.sql`)** (updated):
        *   Creates `APOLLO_WILLIAMGRANT.UTILITY.ROOT_TASK_CONTROLLER`.
        *   Defines Streams on *expected* tables in `APOLLO_WILLIAMGRANT.S3` (e.g., `S3.STREAM_CTLDA`).
        *   Defines Tasks (e.g., `S3.TASK_PROCESS_CTLDA`) to `MERGE` into Gold tables (e.g., in `APOLLO_WILLIAMGRANT.VIP`), using direct column access (placeholders for actual header names).
        *   **Note**: `MERGE` logic in this script now selects directly from discrete columns of the S3 tables (not from a variant `raw_data` column). Column names in `MERGE` are placeholders and MUST be customized.
*   **Status**: `[X - Scripts Updated]`

---

## 3. dbt Project Migration to Snowflake (`dbt_williamgrant`)

(This section remains largely the same as the previous update, with emphasis on ensuring `source()` calls align with the new Gold tables created by Snowflake Tasks in `APOLLO_WILLIAMGRANT` schemas like `VIP`, `MASTER_DATA`, etc.)

*   **Step 3.1. Ensure `profiles.yml` is Configured (User Task)**
    *   **[X - User Task]** (`apollo_snowflake` profile targeting `APOLLO_WILLIAMGRANT`).
*   **Step 3.2. Review `dbt_project.yml` (User Task / Verify)**
    *   **[X - Done]** (Updated for `APOLLO_WILLIAMGRANT` structure).
*   **Step 3.3. Adapt Models (`models/**/*.sql`)**
    *   **[X - Models Adapted, Macros Adapted, Tests Adapted]** (Previous syntax adaptations stand).
    *   **[ ] Review `source()` definitions in dbt staging models**: Ensure they point to the Gold tables in `APOLLO_WILLIAMGRANT.<GoldSchema>` (e.g., `APOLLO_WILLIAMGRANT.VIP.SLSDA`) populated by the Snowflake Tasks from Step 2.5. Staging models might also source directly from `APOLLO_WILLIAMGRANT.S3.<Table>` if some initial dbt transformation is preferred over full Stream/Task logic for certain cases.
*   **Step 3.4. Adapt Macros (`macros/**/*.sql`)**
    *   **[X - Standalone SQL Macros Adapted]**
*   **Step 3.5. Seed Data Loading (`seeds/*.csv`)**
    *   **[X - Reviewed, User Task to Run]** (Targets `APOLLO_WILLIAMGRANT.MASTER_DATA`).
*   **Step 3.6. Testing dbt in Snowflake Development Environment**
    *   **[X - Instructions Provided]** Run `dbt deps`.
    *   **[X - Instructions Provided, User Task to Run]** Execute `dbt run --full-refresh`.
    *   **[X - Tests Adapted, Instructions Provided for User to Run]** Execute `dbt test`.
    *   **[ ]** Compare row counts and sample data against Postgres for key models.

---

## 4. PostgreSQL Function Migration to Snowflake Stored Procedures/UDFs (within `APOLLO_WILLIAMGRANT`)

(Function migration scripts previously generated will be reviewed to ensure they correctly target schemas within `APOLLO_WILLIAMGRANT`, e.g., `APOLLO_WILLIAMGRANT.FORECAST` and `APOLLO_WILLIAMGRANT.MASTER_DATA`.)

### 4.1. General Approach & Best Practices (As per original playbook)

### 4.2. Detailed Migration Plan per Workflow (Targeting `APOLLO_WILLIAMGRANT`)

#### 4.2.1. DDL Scripts for Application Tables (Targeting `APOLLO_WILLIAMGRANT.<schema>`)
*   **[X - Scripts Provided for all 3 workflows]**
    *   `sql_utils/snowflake_forecast_editing_tables_ddl.sql` (for `APOLLO_WILLIAMGRANT.FORECAST`)
    *   `sql_utils/snowflake_forecast_publishing_tables_ddl.sql` (for `APOLLO_WILLIAMGRANT.FORECAST`)
    *   `sql_utils/snowflake_product_tagging_tables_ddl.sql` (for `APOLLO_WILLIAMGRANT.MASTER_DATA`)

#### 4.2.2. Forecast Editing Workflow Functions (Targeting `APOLLO_WILLIAMGRANT.FORECAST`)
*   **[X - Script Provided] `pgfunction__get_forecast_history.sql` (translated to `sql_utils/udf_get_forecast_history.sql`)**
*   **[X - Script Provided] `pgfunction__get_valid_forecast_generation_month_date.sql` (translated to `sql_utils/udf_get_valid_forecast_generation_month_date.sql`)**
*   **[X - Script Provided] `pgfunction__batch_save_forecasts_json.sql` (translated to `sql_utils/sp_batch_save_forecasts_json.sql`)**
*   **[X - Script Provided] `pgfunction__smart_save_forecast.sql` (translated to `sql_utils/sp_smart_save_forecast.sql`)**
*   **[X - Script Provided] `pgfunction__save_forecast_version.sql` (translated to `sql_utils/sp_save_forecast_version.sql`)**
*   **[X - Script Provided] `pgfunction__revert_forecast_to_version.sql` (translated to `sql_utils/sp_revert_forecast_to_version.sql`)**
*   **[X - Script Provided] `pgfunction__get_depletions_forecast.sql` (translated to `sql_utils/udtf_get_depletions_forecast.sql`)**

#### 4.2.3. Forecast Publishing Workflow Functions (Targeting `APOLLO_WILLIAMGRANT.FORECAST`)
*   **[X - Script Provided] `pgfunction__is_depletions_forecast_published.sql` (translated to `sql_utils/udf_is_depletions_forecast_published.sql`)**
*   **[X - Script Provided] `pgfunction__get_division_forecast_publication_history.sql` (translated to `sql_utils/udtf_get_division_forecast_publication_history.sql`)**
*   **[X - Script Provided] `pgfunction__get_market_published_forecasts.sql` (translated to `sql_utils/udtf_get_market_published_forecasts.sql`)**
*   **[X - Script Provided, Needs Review/Refactor] `pgfunction__publish_division_forecasts.sql` (translated to `sql_utils/sp_publish_division_forecasts.sql`)**
*   **[X - Script Provided] `pgfunction__unpublish_group.sql` (translated to `sql_utils/sp_unpublish_group.sql`)**
*   **[X - Script Provided] `pgfunction__unpublish_publication.sql` (translated to `sql_utils/sp_unpublish_publication.sql`)**
*   **[X - Script Provided] `pgfunction__unpublish_division_forecasts.sql` (translated to `sql_utils/sp_unpublish_division_forecasts.sql`)**
*   **[X - Script Provided] `pgfunction__unpublish_market_forecast.sql` (translated to `sql_utils/sp_unpublish_market_forecast.sql`)**

#### 4.2.4. Product Tagging Workflow Functions (Targeting `APOLLO_WILLIAMGRANT.MASTER_DATA`)
*   **[X - Logic Embedded in sp_add_product_tags_to_vsp.sql] `pgfunction__set_product_tag_ids_from_tag_names.sql` (Trigger Function)**
*   **[X - Script Provided] `pgfunction__add_product_tags_to_variant_size_pack.sql` (translated to `sql_utils/sp_add_product_tags_to_vsp.sql`)**
*   **[X - Script Provided] `pgfunction__batch_update_apollo_variant_size_pack_tags.sql` (translated to `sql_utils/sp_batch_update_apollo_variant_size_pack_tags.sql`)**

---

## 5. Data Validation and Testing Strategy

(Content from original playbook section 5 mostly remains relevant, ensure tests target Snowflake objects in `APOLLO_WILLIAMGRANT`)
*   **[ ] Unit Tests for Stored Procedures & UDFs:**
*   **[ ] Data Reconciliation (Postgres vs. Snowflake):**
*   **[ ] Business Logic Validation:**

---

## 6. Workflow Orchestration in Snowflake (for WGS)

(Content from original playbook section 6, adapting to new ingestion and `APOLLO_WILLIAMGRANT`)
*   **[ ] Ingestion Orchestration (New S3->SNS->SQS->Lambda->Generic Snowpipe):**
    *   S3 Event Notification (`s3://apollo-wg/`) -> SNS Topic (`s3_object_created_notifications_apollo`) -> SQS (`s3_to_snowflake_orchestrator_queue_wgs` with filter for `upload_sf/` prefix) -> Central Lambda (`lambda_s3_to_snowflake_orchestrator`) for dynamic DDL (CREATE/ALTER TABLE in `APOLLO_WILLIAMGRANT.S3`) and triggering generic pipe.
    *   Generic Snowpipe (`APOLLO_WILLIAMGRANT.S3.DATA_PIPE`) loads into dynamically determined `APOLLO_WILLIAMGRANT.S3.\"<TableTarget>\"`.
    *   Snowflake Streams on `APOLLO_WILLIAMGRANT.S3.\"<ExpectedTableType>\"` tables -> Snowflake Tasks (dependent on `APOLLO_WILLIAMGRANT.UTILITY.ROOT_TASK_CONTROLLER`) for MERGE into Gold tables (e.g., in `APOLLO_WILLIAMGRANT.VIP`, `APOLLO_WILLIAMGRANT.MASTER_DATA`).
*   **[ ] dbt Run Orchestration:** (dbt Cloud or other orchestrator targeting `dbt_williamgrant` project).
*   **[ ] Function Execution (Application Workflow):** Application connects to `APOLLO_WILLIAMGRANT` and calls UDFs/SPs in relevant schemas.
*   **[ ] `on-run-end` Hooks for dbt:** (Review if still needed, update to call Snowflake SPs if necessary).

---

## 7. Deployment and Cutover Strategy (for WGS)

(Content from original playbook section 7 remains relevant)

---

## 8. Post-Migration Monitoring & Optimization (for WGS)

(Content from original playbook section 8 remains relevant)

---

## 9. Consolidated Step-by-Step Checklist (for WGS Snowflake Pipeline)

**Phase 1: Setup & Initial Configuration (Snowflake for WGS)**
*   [ ] **Team Training:** Snowflake basics, SQL differences, dbt for Snowflake.
*   [X - User Updated Script] **Snowflake Environment:** `APOLLO_WILLIAMGRANT` DB, `S3` schema, `VIP`, `MASTER_DATA`, `FORECAST`, `UTILITY` schemas, warehouses, roles as per `sql_utils/snowflake_env_setup.sql`.
*   [ ] **Snowflake Account:** Confirm Unistore enabled (if Hybrid Tables are used).
*   [ ] **S3 Buckets:** Confirm `s3://apollo-wg/` for incoming CSVs. Define common event prefix (e.g., `uploads/`) and distinct sub-prefixes (`upload/` for Postgres, `upload_sf/` for Snowflake).
*   [ ] **Tooling:** Install SnowSQL, configure dbt for Snowflake (`profiles.yml` for `APOLLO_WILLIAMGRANT`).

**Phase 2: Event-Driven S3 to Snowflake Ingestion (for WGS - New Automated Flow with SNS)**
*   [X - Script Updated] **IAM Setup (Script: `apollo-glue/setup_snowflake_lambda_pipeline_iam.sh`)**: Create/verify IAM roles for Central Lambda and Snowflake S3 Storage Integration.
*   [X - Needs Verification] **Snowflake Service User & Auth (Snowflake RBAC setup)**: Create `LAMBDA_SF_USER` & `BACKEND_ENG_ROLE` with permissions for dynamic DDL/DML, pipe ops; Key-Pair auth; private key in Secrets Manager.
*   [X - Script Updated] **AWS SNS Topic, SQS Queue & S3 Event Notification (Script: `apollo-glue/setup_snowflake_lambda_pipeline_aws_resources.sh`)**:
    *   Create SNS Topic (e.g., `s3_object_created_notifications_apollo`).
    *   Create SQS queue for Snowflake Lambda (`s3_to_snowflake_orchestrator_queue_wgs`).
    *   Subscribe Snowflake SQS queue to SNS Topic with filter policy (for `upload_sf/` prefix, RawMessageDelivery enabled).
    *   Grant SNS permission to send to Snowflake SQS queue.
    *   Configure S3 event on `s3://apollo-wg/` (for common prefix like `uploads/` and suffix `*.csv.gz`) to the SNS Topic.
*   [X - Script Updated] **Central Lambda (Script: `apollo-glue/lambda/lambda_s3_to_snowflake_orchestrator.py` & deployed by `setup_snowflake_lambda_pipeline_aws_resources.sh`)**: Develop and deploy Lambda (dynamic table creation with discrete VARCHAR columns from header, schema evolution, Snowpipe trigger via `SYSTEM$PIPE_INSERT`).
*   [X - Script Updated] **Snowflake S3 Schema Objects (Script: `sql_utils/snowflake_raw_env_setup.sql`)**: Create `APOLLO_WILLIAMGRANT.S3` schema, `INGESTED_FILES` table, storage integration, external stage, file format, and `APOLLO_WILLIAMGRANT.S3.DATA_PIPE`.
*   [X - Script Updated] **Snowflake Streams & Tasks (Script: `sql_utils/snowflake_streams_and_tasks_raw_to_gold.sql`)**:
    *   Create `APOLLO_WILLIAMGRANT.UTILITY.ROOT_TASK_CONTROLLER`.
    *   Define Streams on tables in `APOLLO_WILLIAMGRANT.S3` (e.g., `S3.STREAM_CTLDA`).
    *   Define Tasks in `APOLLO_WILLIAMGRANT.S3` (e.g., `S3.TASK_PROCESS_CTLDA`) to `MERGE` into Gold tables (e.g., in `APOLLO_WILLIAMGRANT.VIP`), using direct column access (placeholders for actual header names).

**Phase 3: dbt Project Migration (`dbt_williamgrant`)**
*   [X - User Task Done] Copied legacy `apollo_dbt` to `dbt_williamgrant`.
*   [X - User Task] `profiles.yml` configured for `APOLLO_WILLIAMGRANT`.
*   [X - Done] `dbt_project.yml` updated for `dbt_williamgrant` and `APOLLO_WILLIAMGRANT` structure.
*   [ ] **Review `sources.yml`**: Ensure sources point to Snowflake Gold tables in `APOLLO_WILLIAMGRANT` (e.g., `APOLLO_WILLIAMGRANT.VIP.YOUR_GOLD_TABLE`) or relevant `APOLLO_WILLIAMGRANT.S3` tables if applicable.
*   [X - Models Adapted, Macros Adapted, Tests Adapted] SQL syntax for models, macros, tests updated for Snowflake.
*   [X - Reviewed, User Task to Run] Run `dbt seed --target your_snowflake_dev_target`.
*   [X - Instructions Provided, User Task to Run] Run `dbt run --full-refresh --target your_snowflake_dev_target`.
*   [X - Tests Adapted, Instructions Provided for User to Run] Run `dbt test --target your_snowflake_dev_target`.

**Phase 4: PostgreSQL Function Migration (to `APOLLO_WILLIAMGRANT` Schemas)**
*   [X - Scripts Provided for all 3 workflows] DDLs for `FORECAST` and `MASTER_DATA` tables translated (`sql_utils/*_tables_ddl.sql`).
*   [X - All Scripts Provided] Migrate `forecast_editing_workflow` functions (`sql_utils/udf_*.sql`, `sql_utils/sp_*.sql`).
*   [X - All Scripts Provided] Migrate `forecast_publishing_workflow` functions (noting `sp_publish_division_forecasts.sql` needs review/refactor) (`sql_utils/udf_*.sql`, `sql_utils/sp_*.sql`).
*   [X - All Scripts Provided] Migrate `product_tagging_workflow` functions (`sql_utils/sp_*.sql`, trigger logic embedded).

**Phase 5: Data Validation and Testing Strategy** (Details in Section 5)

**Phase 6: Workflow Orchestration in Snowflake** (Details in Section 6)

**Phase 7: Deployment and Cutover Strategy** (Details in Section 7)

**Phase 8: Post-Migration Monitoring & Optimization** (Details in Section 8)

---
## Appendix A: Key PostgreSQL Functions and Migration Strategy Ideas

(Content remains largely the same but ensure all schema/table references are updated to `APOLLO_WILLIAMGRANT.S3.<object>` or `APOLLO_WILLIAMGRANT.<GoldSchema>.<object>` if mentioned here)
