Excellent. This is the right way to approach it. Consolidating the "publish to review" and "promote to consensus" workflows into a single, intelligent stored procedure is a much cleaner and more maintainable design. It simplifies the API surface for the frontend and centralizes the business logic.

I have analyzed the source PostgreSQL function and the existing Snowflake procedures. Here is the implementation plan.

### CREATIVE PHASE: `sp_publish_division_forecast` Migration & Design

---

### 1. Analysis of Source Logic (`pgfunction__publish_division_forecasts.sql`)

The PostgreSQL function is a multi-purpose tool with two primary execution paths controlled by the `p_publication_status` parameter:

*   **`'review'` Workflow:** This is a granular, division-specific action. It gathers all markets for a division, creates a new publication group, and then iterates through each market to either create a new "review" publication or update an existing one. For each market, it archives the current state of manual edits and the primary automated forecast into the `published_forecasts` table.
*   **`'consensus'` Workflow:** This is a promotion action.
    *   If a specific `division` is provided, it promotes all "review" publications for that division to "consensus".
    *   If `division` is `NULL`, it acts as a "superuser" mode, finding *all* divisions with items in review and promoting each one to consensus.
    *   Critically, after a successful promotion to consensus, it triggers a sync process (`smart_save_forecast` in Postgres) to carry forward the consensus numbers to the next forecast period.

### 2. Proposed Snowflake Architecture (`sp_publish_division_forecast`)

I will create a single, new stored procedure that replicates and optimizes this logic for Snowflake. It will gracefully handle all cases within one procedure call, making it the single point of entry for all publishing actions.

<mermaid>
graph TD
    A[START: CALL sp_publish_division_forecast] --> B{Check p_publication_status};
    B -- "'review'" --> C[Review Workflow];
    B -- "'consensus'" --> D[Consensus Workflow];

    subgraph C[Review Workflow for a Single Division]
        C1[Get/Create Publication Group] --> C2[Loop Through Markets in Division];
        C2 --> C3{Publication Exists?};
        C3 -- "No" --> C4[Create New 'review' Publication];
        C3 -- "Yes" --> C5[Update Existing 'review' Publication];
        C4 & C5 --> C6[Archive Manual & Draft Forecasts];
        C6 --> C7[Update Manual Inputs to 'review' status];
        C7 --> C8[Log Success for Market];
    end

    subgraph D[Consensus Workflow]
        D1{Is p_division NULL?};
        D1 -- "Yes (Superuser)" --> D2[Find all divisions in 'review'];
        D1 -- "No" --> D3[Use specified division];
        D2 --> D4[Loop Through Divisions];
        D3 --> D4;
        subgraph D4 [For Each Division...]
            D5[Promote all 'review' publications to 'consensus'] --> D6[Update manual inputs to 'consensus' status];
            D6 --> D7{Did this complete the division?};
            D7 -- "Yes" --> D8[CALL _INTERNAL_SP_SYNC_CONSENSUS_TO_NEXT_MONTH];
            D7 -- "No" --> D9[Log Promotion];
            D8 --> D9;
        end
    end
    
    C8 --> Z[Return Log of All Actions];
    D9 --> Z;
    D4 --> E{All divisions done};
    E -- "Yes" --> F[CALL _INTERNAL_SP_UPDATE_VALID_FGMD_STATUS];
    F --> Z;


</mermaid>

### 3. Detailed Implementation Plan

This plan will be executed when you approve the build.

1.  **Create New File:** A new file, `tenants/pg_func_migration/forecast_publishing_workflow/sp_publish_division_forecast.sql`, will be created.

2.  **Procedure Signature & Setup:**
    *   The procedure will accept the same parameters as the source function.
    *   It will `RETURNS TABLE(...)` to provide a detailed log of actions taken.
    *   A temporary table will be created at the start to hold these log messages.

3.  **Implement 'review' Workflow:**
    *   I will use a `MERGE` statement where possible to efficiently find or create the `DEPLETIONS_FORECAST_PUBLICATION_GROUPS` record.
    *   The core logic will loop through the markets of the specified division. Inside the loop, it will perform the necessary `INSERT`/`UPDATE`/`DELETE` statements to correctly archive the manual and draft forecasts into the `DEPLETIONS_FORECAST_PUBLISHED_FORECASTS` table.

4.  **Implement 'consensus' Workflow:**
    *   The logic will be adapted from the existing `sp_promote_to_consensus.sql` because it uses a more efficient, set-based approach than the source function's loops.
    *   A top-level `IF :P_DIVISION IS NULL` block will handle the superuser "promote all" case by looping through the necessary divisions and applying the promotion logic to each.
    *   For each division promoted, it will perform the crucial check to see if all its markets are now in consensus. If so, it will call `_INTERNAL_SP_SYNC_CONSENSUS_TO_NEXT_MONTH` to handle the data sync.

5.  **Integrate Global State Update:**
    *   After the 'consensus' workflow completes (for one or all divisions), a final call to `_INTERNAL_SP_UPDATE_VALID_FGMD_STATUS()` will be made. This ensures the platform's official forecast month is updated immediately after the promotions that might have triggered a change.

6.  **Deprecate Old Procedure:**
    *   Once the new, unified procedure is in place, the now-redundant `sp_promote_to_consensus.sql` file will be deleted to avoid confusion and code duplication.

This plan will result in a single, robust, and maintainable stored procedure that handles all publishing-related business logic, centralizes control flow, and correctly maintains the real-time state of the platform. Please let me know if you approve, and I will proceed with the build.