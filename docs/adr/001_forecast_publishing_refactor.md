# Architecture Decision Record: Forecast Publishing Workflow

## Context
The project requires migrating a complex PostgreSQL function, `publish_division_forecasts`, to Snowflake. This function manages a two-stage forecast approval process (`review` and `consensus`) and includes logic to snapshot data and sync approved forecasts forward to the next cycle. A direct translation is inefficient and difficult to maintain in Snowflake.

- **System Requirements:**
  - The system must allow users to publish forecasts for a division into a 'review' state.
  - The system must allow authorized users to promote 'review' state forecasts to 'consensus'.
  - Consensus forecasts must be automatically used as the starting point for the next forecast cycle.
  - The process must be scalable, maintainable, and performant on Snowflake.
- **Technical Constraints:**
  - The solution must be implemented within Snowflake using its native features (SQL, Scripting, Tasks).
  - It needs to integrate with the existing `APOLLO_WILLIAMGRANT` database schema.
  - Must be callable from a backend API.

## Component Analysis
- **Core Components:**
  - **`depletions_forecast_init_draft`**: Source table containing raw, calculated forecast data.
  - **`manual_input_depletions_forecast`**: Table for user overrides, which take precedence.
  - **`depletions_forecast_publications`**: Tracks the status ('review', 'consensus') of a forecast for a given market and month.
  - **`depletions_forecast_published_forecasts`**: An append-only table that serves as a historical snapshot of all data that was part of a publication.
- **Interactions:**
  - A user action triggers the publication process.
  - The publication process reads from draft/manual tables and writes to the published/publications tables.
  - The consensus process updates statuses and triggers a data sync for the next forecast period.

## Architecture Options

### Option 1: The "Lift & Shift" - Refined Stored Procedure
- **Description:** Refine the existing procedural translation, fixing cursor logic and transaction management within a single, large stored procedure.
- **Pros:** Closest to original logic, encapsulates the entire process in one object.
- **Cons:** Remains complex, difficult to debug, and likely has poor performance due to its row-by-row nature.

### Option 2: The "Set-Based" - Modular SQL & Task-Based Approach
- **Description:** Decompose the workflow into smaller, dedicated stored procedures for 'review' and 'consensus' actions, using set-based SQL (`INSERT...SELECT`, `MERGE`). A separate, scheduled Snowflake Task handles the logic for syncing consensus forecasts forward.
- **Pros:** Highly performant and scalable, easier to maintain and debug, robust due to decoupled task-based sync.
- **Cons:** Spreads logic across multiple database objects (SPs and a Task), requires clear documentation.

### Option 3: The "dbt-centric" - Leveraging dbt Models & Hooks
- **Description:** Model the 'review' and 'consensus' states within dbt. The application would trigger dbt runs to materialize these models, and an `on-run-end` hook would handle the consensus sync.
- **Pros:** Consolidates logic within dbt, leveraging its version control and testing.
- **Cons:** Increases dependency on a dbt orchestrator, and application-triggered runs can be complex. The logic in hooks is harder to debug than a dedicated SP.

## Decision
- **Chosen Option:** **Option 2: The "Set-Based" - Modular SQL & Task-Based Approach**
- **Rationale:** This option provides the best combination of performance, scalability, and maintainability for a Snowflake environment. It leverages Snowflake's strength in set-based processing over procedural loops. Decoupling the consensus sync into an asynchronous Task makes the user-facing action faster and the overall system more resilient.

- **Implementation Considerations:**
  - Three main objects will be created:
    1.  `sp_publish_forecast_for_review(division, user_id, note)`: A stored procedure that uses set-based `INSERT` statements to snapshot the relevant forecast data.
    2.  `sp_promote_forecast_to_consensus(division, user_id, note)`: A stored procedure that uses `UPDATE` statements to change the status of publications. It will *not* handle the data sync directly.
    3.  `task_sync_consensus_forecasts`: A Snowflake Task running on a schedule (e.g., every 5 minutes) that identifies newly-consensed forecasts and uses a single `MERGE` statement to upsert them into the `manual_input_depletions_forecast` table for the next month.
  - A new timestamp or status column (`sync_status`) might be needed on the `depletions_forecast_publications` table to help the Task identify which records to process.
  
## Validation
- [✓] **Requirements Met**: All functional requirements are met. The asynchronous nature of the sync task is acceptable for this business process.
- [✓] **Technical Feasibility**: The approach uses standard, well-supported Snowflake features.
- [✓] **Risk Assessment**: Low risk. The modular design makes each part easier to test and validate. The asynchronous task can be monitored and has built-in retry mechanisms. 