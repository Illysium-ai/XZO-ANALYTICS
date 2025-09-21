# Architecture Decision Record: dbt Source Configuration

## Context
The new Snowflake architecture uses an event-driven pipeline (S3 -> SNS -> SQS -> Lambda -> Snowpipe -> Streams/Tasks) to ingest raw data and transform it into "Gold" tables. The `dbt_williamgrant` project needs a clear, stable, and maintainable strategy for sourcing this data for its models.

- **System Requirements:**
  - dbt models need a reliable interface to consume data from the ingestion pipeline.
  - The dbt source configuration should be stable and not break when new columns appear in raw source files.
  - The architecture should be easy to understand and maintain.
- **Technical Constraints:**
  - The solution must work with the established data pipeline.
  - The dbt project sources from the `APOLLO_WILLIAMGRANT` database.

## Component Analysis
- **Core Components:**
  - **`APOLLO_WILLIAMGRANT.S3` schema:** Landing zone for raw data from Snowpipe. Schemas here can be dynamic.
  - **Snowflake Streams & Tasks:** The mechanism for transforming raw data from the `S3` schema to a structured "Gold" layer.
  - **"Gold" Schemas (e.g., `APOLLO_WILLIAMGRANT.VIP`, `APOLLO_WILLIAMGRANT.MASTER_DATA`):** These schemas house the cleaned, structured, and permanent tables that are ready for analytics.
  - **`dbt_williamgrant/models/staging`**: dbt models that perform the first layer of transformation within dbt.
  - **`dbt_williamgrant/models/sources.yml`**: The dbt file for declaring data sources.

## Architecture Options

### Option 1: Source Directly from the "Gold" Tables
- **Description:** Configure dbt's `sources.yml` to point exclusively to the stable, cleaned, and well-structured "Gold" tables located in schemas like `APOLLO_WILLIAMGRANT.VIP`. All raw data processing is handled by the upstream Snowflake pipeline before dbt is invoked.
- **Pros:** Cleanly decouples dbt from the ingestion pipeline, provides a stable and reliable interface for sources, aligns with data warehousing best practices (Bronze/Silver/Gold layers).
- **Cons:** All initial cleaning and structuring must be handled within Snowflake Streams/Tasks.

### Option 2: A Hybrid Approach - Source from Both S3 and Gold Tables
- **Description:** Some dbt models source from the Gold tables, while others source directly from the raw tables in the `APOLLO_WILLIAMGRANT.S3` schema for simpler transformations.
- **Pros:** Offers flexibility to use dbt for some initial data cleaning.
- **Cons:** Creates an inconsistent and confusing architecture. Exposes dbt to potentially unstable schemas in the raw landing zone, which could cause jobs to fail.

## Decision
- **Chosen Option:** **Option 1: Source Directly from the "Gold" Tables**
- **Rationale:** This approach establishes the most robust, maintainable, and clean architecture. It creates a clear "contract" where the ingestion pipeline is responsible for delivering production-ready Gold tables, and dbt is responsible for the subsequent business intelligence and analytics transformations. This clear separation of concerns is a hallmark of a mature data platform.

- **Implementation Considerations:**
  - The `snowflake_streams_and_tasks_raw_to_gold.sql` script must be fully implemented to handle the transformation for all required data sources.
  - The `sources.yml` file in the `dbt_williamgrant` project must be updated to remove any references to `S3` schema tables and point exclusively to tables in the `VIP`, `MASTER_DATA`, etc., schemas.
  
## Validation
- [✓] **Requirements Met**: Provides a stable and reliable sourcing mechanism for dbt.
- [✓] **Technical Feasibility**: This is a standard and recommended pattern for using dbt with an ELT pipeline.
- [✓] **Risk Assessment**: Low risk. It reduces the risk of dbt job failures due to unexpected changes in raw data formats. 