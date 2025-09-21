# Session Archive: Snowflake Hybrid Table Strategy & Implementation

**Date:** 2024-07-26
**Associated Task:** Phase 4: Stored Procedure & UDF Migration - Architectural Review

---

## 1. Project Brief

- **Name:** apollo-analytics
- **Description:** Data analytics and dbt models for Apollo project, handling data transformation, forecasting, and reporting for multiple tenants on PostgreSQL and Snowflake.
- **Business Goal:** Provide accurate and timely data insights to business users and support forecasting and depletion analysis.

---

## 2. Task Summary: Architecting for Hybrid Workloads

This session focused on a comprehensive review and implementation of the table architecture for the PostgreSQL to Snowflake migration. The primary goal was to ensure low-latency, transactional performance for application-facing workflows while correctly integrating with the dbt-based analytical pipeline.

**Key Implementations:**

1.  **Transactional Tables:** Confirmed and implemented `HYBRID TABLE` designation for all tables involved in direct, real-time application workflows (`MANUAL_INPUT_DEPLETIONS_FORECAST`, `DEPLETIONS_FORECAST_PUBLICATIONS`, `DEPLETIONS_FORECAST_PUBLISHED_FORECASTS`, etc.).
2.  **Analytical Table Optimization:** Optimized large, dbt-managed analytical tables (`depletions_forecast_init_draft`, `distributor_allocation_by_market_size_pack`) by adding `cluster_by` configurations.
3.  **Hybrid Usage Pattern Solution:** Designed and implemented a robust solution for the `depletions_forecast_primary_forecast_method` table, which is both created by dbt and updated transactionally by the application. This involved a manual DDL creation of the Hybrid Table and refactoring the dbt model to only perform incremental inserts.

---

## 3. Key Architectural Decisions (ADRs)

### ADR-003: Synchronous Forecast Publishing and Data Sync

- **Context:** The initial design for forecast publishing was asynchronous, which did not meet the business requirement for an instantaneous, atomic data sync upon promotion to consensus.
- **Decision:** The architecture was changed to use **Modular Stored Procedures with a Synchronous Sync**. The primary procedure (`sp_promote_forecast_to_consensus`) wraps the entire operation in a single transaction and directly calls an internal helper procedure (`_internal_sp_sync_consensus_to_next_month`) to perform the data sync via a `MERGE` statement.
- **Rationale:** This guarantees atomicity and instant data consistency, satisfying the core business requirement and preventing race conditions.

---

## 4. Final Reflection & Lessons Learned

*   **Lesson 1: Usage Patterns Dictate Architecture.** The most critical lesson was that a table's read/write pattern is more important than its origin (e.g., dbt vs. application). A table's actual usage in stored procedures and application calls must determine its underlying architecture.
*   **Lesson 2: The "Manual DDL + dbt DML" Pattern is Powerful.** When faced with a tooling limitation (dbt's lack of native Hybrid Table support), redefining the tool's role is a powerful strategy. Using a one-time manual DDL setup and then having dbt handle only incremental DML is a clean, effective pattern.
*   **Lesson 3: Simplicity Trumps Unnecessary Abstraction.** The final solution of a single, authoritative Hybrid Table was superior to an initial "shadow table" proposal because it reduced complexity in the data model and simplified queries.
*   **Lesson 4: Choose the Right Snowflake Optimization Tool.** We clearly distinguished between the use cases for `HYBRID TABLE` (transactional workloads) and `CLUSTER BY` (analytical workloads), ensuring each table was optimized correctly for performance and cost. 