# CREATIVE PHASE: Snowflake UDTF Performance Optimization

## 🎨🎨🎨 ENTERING CREATIVE PHASE: Architecture 🎨🎨🎨

### Component Description

**Component:** `APOLLO_WILLIAMGRANT.FORECAST.GET_DEPLETIONS_FORECAST` UDTF.

This User-Defined Table Function (UDTF) is designed to retrieve filtered depletions forecast data. It was migrated from PostgreSQL to Snowflake and is performing significantly slower in the new environment.

The function joins a large, analytical, standard Snowflake table (`DEPLETIONS_FORECAST_INIT_DRAFT`) with several smaller "transactional" Hybrid Tables that are modified by user actions in a front-end application. The goal is to get a unified view of forecast data, including manual adjustments and publication status, in real-time.

**Key Tables:**
*   **Standard (Analytical):** `DEPLETIONS_FORECAST_INIT_DRAFT`, `APOLLO_DIST_MASTER`, `DEPLETIONS_FORECAST_MONTHEND_PREDICTION`, `HYPERION_GSV_RATES_BY_CUSTOMER`.
*   **Hybrid (Transactional):** `MANUAL_INPUT_DEPLETIONS_FORECAST`, `DEPLETIONS_FORECAST_PUBLISHED_FORECASTS`, `DEPLETIONS_FORECAST_PUBLICATIONS`, `DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD`, `APOLLO_VARIANT_SIZE_PACK_TAG`.

### Requirements & Constraints

1.  **Performance:** The function's performance must be significantly improved to match or exceed the original PostgreSQL implementation.
2.  **Real-Time Data:** The query must return live data, reflecting the latest user edits from the Hybrid Tables. Materialized Views that are manually refreshed are not a viable solution.
3.  **Functionality:** The final solution must replicate the existing logic and return the same data structure.
4.  **Maintainability:** The proposed solution should be easy to understand and maintain.

### Problem Analysis: Why is it slow?

The primary reason for the performance issue lies in the fundamental architectural difference between how PostgreSQL and Snowflake's query engines operate, especially when mixing table types.

*   **PostgreSQL:** Operates on a row-by-row basis, and with proper indexing, it can perform very fast lookups and joins, even in complex queries. It's well-suited for this kind of mixed OLTP/OLAP workload in a single query if datasets are reasonably sized.
*   **Snowflake:** Uses a columnar store and a vectorized query execution engine, which is optimized for scanning and aggregating massive amounts of data in analytical queries. Hybrid Tables (Unistore) are a newer addition, providing a row-oriented store for fast OLTP-style point lookups and DML operations.

The current UDTF design forces Snowflake's analytical engine to perform what are essentially many small, repeated OLTP-style lookups against the Hybrid Tables for each slice of data it processes from the main analytical table (`DEPLETIONS_FORECAST_INIT_DRAFT`). This is an anti-pattern for a columnar engine. Each `LEFT JOIN` to a Hybrid Table on multiple key columns, without proper indexing, likely results in a full scan of that Hybrid Table, creating a massive performance bottleneck.

---

### Options Analysis

Here are three architectural options to address the performance problem.

#### Option 1: Indexing and Optimizing the Existing UDTF

This option involves the least amount of architectural change and focuses on optimizing the current implementation.

*   **Description:** Add composite primary keys and secondary indexes to all Hybrid Tables on the columns used in the `JOIN` conditions within the `udtf_get_depletions_forecast.sql` function. For example, for `MANUAL_INPUT_DEPLETIONS_FORECAST`, an index would be created on `(MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE)`.
*   **Pros:**
    *   Minimal code changes required.
    *   Easy to implement.
    *   Leverages the intended purpose of Hybrid Tables for fast lookups.
*   **Cons:**
    *   May not fully solve the problem if the core issue is the query plan itself (i.e., joining one very large analytical table to many smaller hybrid tables).
    *   Performance gains are dependent on the Snowflake optimizer choosing to use the indexes effectively for these joins.
    *   Maintaining many complex composite indexes could add overhead to the DML operations on the Hybrid Tables.

#### Option 2: Staged Data Retrieval with Intermediate Tables

This option refactors the query to avoid joining large analytical tables directly with multiple hybrid tables in a single complex query.

*   **Description:** Break the single monolithic query into stages.
    1.  Create a temporary, transient table containing only the required, pre-filtered data from the large analytical table (`DEPLETIONS_FORECAST_INIT_DRAFT`).
    2.  Join this smaller, filtered, transient table with the Hybrid Tables.
    This approach helps the optimizer by reducing the data volume at each join stage and allows it to work with more homogenous data sets (e.g., joining a small standard table to hybrid tables).
*   **Pros:**
    *   Significantly improves the chances of an optimal query plan.
    *   Reduces the complexity of the main `SELECT` statement.
    *   The logic becomes more modular and easier to debug.
*   **Cons:**
    *   Requires refactoring the UDTF into a Stored Procedure, as UDTFs don't support creating temporary tables.
    *   Introduces slightly more complexity with the management of transient tables.

#### Option 3: Separate Transactional and Analytical Data Flows

This is the most architecturally significant change, creating a clear separation between transactional data and analytical queries.

*   **Description:** Decouple the user-facing transactional tables from the analytical query path.
    1.  Users continue to interact with the Hybrid Tables for fast reads and writes.
    2.  Use Snowflake Streams and Tasks to capture changes from the Hybrid Tables (e.g., `MANUAL_INPUT_DEPLETIONS_FORECAST`) in near real-time.
    3.  A recurring Task merges these changes into a standard, analytical Snowflake table that is optimized for querying (e.g., `DEPLETIONS_FORECAST_ENRICHED`). This table would be a denormalized combination of the `INIT_DRAFT` data and the latest manual inputs/publication statuses.
    4.  The `GET_DEPLETIONS_FORECAST` function is rewritten as a simple `SELECT` from this new, optimized analytical table.
*   **Pros:**
    *   **Best performance:** Queries are extremely fast as they hit a single, pre-joined, columnar analytical table.
    *   **Scalability:** Completely separates OLTP and OLAP workloads, which is a best practice for data warehousing.
    *   The front-end application remains fast as it interacts only with the responsive Hybrid Tables.
*   **Cons:**
    *   **Complexity:** Requires setting up Streams and Tasks, which adds more moving parts to the architecture.
    *   **Near Real-Time, Not Real-Time:** There will be a small latency (e.g., 1-5 minutes, depending on the Task schedule) between a user's edit and when it appears in the analytical query. This may violate the "live data" requirement, depending on how strict that constraint is.
    *   More development effort to implement.

---

### Recommended Approach

**Recommendation: Option 2 - Staged Data Retrieval with Intermediate Tables.**

This approach provides the best balance of performance improvement and implementation effort while strictly adhering to the real-time data requirement. By refactoring the UDTF into a Stored Procedure, we can guide the Snowflake optimizer by breaking down the complex query into logical, manageable stages. This avoids the primary performance pitfall of joining a massive analytical table with multiple smaller, row-oriented hybrid tables in one go.

While Option 1 (indexing) should be done regardless, it may not be sufficient on its own. Option 3 is a powerful pattern but introduces a latency that seems to conflict with the project's hard requirement for "live data".

Therefore, starting with Option 2 gives us the highest probability of success with a reasonable amount of refactoring.

### Implementation Guidelines

1.  **Convert UDTF to Stored Procedure:**
    *   The function `GET_DEPLETIONS_FORECAST` will be converted into a Stored Procedure that returns a `TABLE(...)`.
2.  **Create a Filtered Transient Table:**
    *   Inside the SP, the first step will be to `CREATE OR REPLACE TEMPORARY TABLE DRAFT_FILTERED AS SELECT ... FROM DEPLETIONS_FORECAST_INIT_DRAFT WHERE ...` applying all the input parameter filters (`P_MARKETS`, `P_BRANDS`, etc.). This dramatically reduces the size of the initial dataset.
3.  **Implement Staged Joins:**
    *   The main query will now `SELECT ... FROM DRAFT_FILTERED ...` and then `LEFT JOIN` the various Hybrid Tables. The optimizer will be joining a much smaller table, leading to a more efficient query plan.
4.  **Add Indexes to Hybrid Tables (from Option 1):**
    *   As a complementary step, add secondary indexes to the Hybrid Tables on the columns used in the join keys. This will ensure that the lookups from the `DRAFT_FILTERED` table into the Hybrid tables are as fast as possible.

### Verification Checkpoint

*   The new Stored Procedure returns data with the exact same schema and values as the original UDTF.
*   Performance is measurably faster and meets user expectations.
*   The solution still provides real-time data from the Hybrid Tables.

## 🎨🎨🎨 EXITING CREATIVE PHASE 🎨🎨🎨 