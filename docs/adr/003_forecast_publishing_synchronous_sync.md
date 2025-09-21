# Architecture Decision Record: Synchronous Forecast Publishing and Data Sync

## Context
This ADR supersedes `001_forecast_publishing_refactor.md`. The initial design proposed an asynchronous Snowflake Task for syncing consensus forecast data. User feedback revealed a critical requirement: the sync must be **instantaneous, synchronous, and atomic** with the promotion to consensus, completing *before* the front-end application transitions to the next forecast period. An asynchronous process introduces an unacceptable delay and potential for race conditions.

- **System Requirements:**
  - The consensus promotion and data sync must occur as a single, atomic transaction.
  - The API call triggering the promotion must be blocking; it should not return until the data sync is complete.
  - The mechanism must be resilient to race conditions.

## Decision
- **Chosen Architecture:** **Modular Stored Procedures with Synchronous Sync**
- **Rationale:** This architecture meets the strict requirement for synchronous execution while retaining the benefits of modular, set-based SQL for performance and maintainability. It eliminates the asynchronous Task, which was the deal-breaker in the previous design.

### Architectural Components:

1.  **`sp_promote_forecast_to_consensus(...)`**: The primary, user-facing stored procedure.
    *   It wraps the entire logic in a single transaction (`BEGIN/COMMIT/ROLLBACK`).
    *   It performs set-based `UPDATE`s to promote publications to 'consensus' status.
    *   It then **synchronously** checks if all markets within a division have reached consensus.
    *   If the condition is met, it **directly calls** an internal helper procedure to perform the data sync.
    *   Execution is blocking and will not return to the caller until all steps are complete.

2.  **`_internal_sp_sync_consensus_to_next_month(...)`**: A dedicated, internal helper procedure.
    *   It contains a single, highly efficient `MERGE` statement.
    *   This statement atomically upserts the consensus forecast data from the current period into the draft records for the *next* forecast period.
    *   Being called directly within the parent procedure's transaction ensures atomicity for the entire operation.

## Validation
- [✓] **Instant Sync**: The sync is a direct, blocking `CALL` within the main procedure. There is no delay.
- [✓] **Atomicity**: The use of a single, overarching transaction guarantees that the promotion and data sync either both succeed or both fail.
- [✓] **Race Condition Prevention**: Snowflake's ACID transaction model ensures that concurrent executions will be serialized, preventing race conditions. The check to trigger the sync occurs inside the transaction, guaranteeing a consistent view of the data.
- [✓] **Performance**: The core logic uses set-based `UPDATE` and `MERGE` statements, which are highly optimized in Snowflake, ensuring the synchronous operation is as fast as possible. 