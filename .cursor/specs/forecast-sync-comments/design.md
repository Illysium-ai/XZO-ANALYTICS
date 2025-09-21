### Overview
The internal sync procedure `FORECAST._INTERNAL_SP_SYNC_CONSENSUS_TO_NEXT_MONTH` performs a MERGE from the consensus month to the next FGMD. The current INSERT branch sets `COMMENT` to only the auto-sync banner and does not append the source comment, which caused the screenshot case where `2025-08-01` rows show `Auto-synced...` without `Original: ...`. Additionally, a later change removed a comment-based update predicate, so existing rows with equal volumes do not have comments refreshed.

- Root cause: INSERT branch omits `Original: <source.COMMENT>`; updates only consider volume changes.
- Impact: Comments present in the consensus month are lost on first-time inserts, and may not refresh later when volumes are unchanged.

### Current logic (evidence)
```57:73:tenants/dbt_williamgrant/backend_functions/sf_forecast_publishing_workflow/sp_internal_sync_consensus_to_next_month.sql
            target.COMMENT = 'Auto-synced from ' || :P_CURRENT_FGMD || ' consensus. Original: ' || COALESCE(source.COMMENT, ''),
            target.UPDATED_AT = CURRENT_TIMESTAMP(),
            target.CURRENT_VERSION = target.CURRENT_VERSION + 1 -- Only increment if actually changed
    WHEN NOT MATCHED THEN
        INSERT (
            MARKET_NAME, MARKET_CODE, DISTRIBUTOR_NAME, DISTRIBUTOR_ID,
            BRAND, BRAND_ID, VARIANT, VARIANT_ID, VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID,
            FORECAST_YEAR, MONTH, FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE,
            MANUAL_CASE_EQUIVALENT_VOLUME, UPDATED_BY_USER_ID, FORECAST_STATUS, COMMENT,
            UPDATED_AT, CURRENT_VERSION
        ) VALUES (
            source.MARKET_NAME, source.MARKET_CODE, source.DISTRIBUTOR_NAME, source.DISTRIBUTOR_ID,
            source.BRAND, source.BRAND_ID, source.VARIANT, source.VARIANT_ID, source.VARIANT_SIZE_PACK_DESC, source.VARIANT_SIZE_PACK_ID,
            source.FORECAST_YEAR, source.MONTH, source.FORECAST_METHOD, :V_FUTURE_FGMD,
            source.MANUAL_CASE_EQUIVALENT_VOLUME, :P_USER_ID, 'draft', 'Auto-synced from ' || :P_CURRENT_FGMD || ' consensus.',
            CURRENT_TIMESTAMP(), 1
        );
```

### Commit history (evidence)
- 08cdabe97c7beaab846e12ec97a6ecbc2b34dbf7 (2025-07-23): Commented out comment-based update predicate, leaving only volume difference to trigger updates. Message: "simplify the update condition by commenting out an unused check on the comment field." Affects update sensitivity, not the insert omission.
- a99b67c63775588dcd354c02ec10dfb8f7f7c182 (2025-06-23): Introduced the procedure with the INSERT `COMMENT` missing the `Original: <source>` suffix. This is the origin of the loss on first insert.
- 927e80706ec11803273fe3680c422f99c498dc93 (2025-07-08): Added predicate to avoid unnecessary updates but still consider comment state; later relaxed in 08cdabe9.

### Design changes
- D-201 (R-102,R-105): Append source comments on INSERT using a null-safe concatenation.
  - `COMMENT = 'Auto-synced from ' || :P_CURRENT_FGMD || ' consensus.' || NVL2(source.COMMENT, ' Original: ' || source.COMMENT, '')`
- D-202 (R-103,R-104,R-106): Broaden UPDATE predicate to include comment mismatch:
  - Add `OR COALESCE(target.COMMENT, '') != ('Auto-synced from ' || :P_CURRENT_FGMD || ' consensus.' || NVL2(source.COMMENT, ' Original: ' || source.COMMENT, ''))`
- D-203 (R-104): Keep versions MERGE unchanged; it already prevents duplicates.

### Sequence
```mermaid
sequenceDiagram
  participant UI as UI
  participant SP as sp_publish_division_forecast
  participant INT as _INTERNAL_SP_SYNC_CONSENSUS_TO_NEXT_MONTH
  participant T as MANUAL_INPUT_DEPLETIONS_FORECAST
  UI->>SP: Promote to consensus
  SP->>INT: CALL with P_CURRENT_FGMD, division(s), user
  INT->>T: MERGE (MATCHED -> UPDATE; NOT MATCHED -> INSERT)
  Note over INT,T: Insert/Update set COMMENT to banner + optional "Original: <source>"
  INT-->>SP: return counts
```

### Risks and mitigations
- Slightly higher UPDATE count due to comment drift checks (⚠️). Mitigate by limiting to exact computed string comparison.
- Very long source comments may exceed column size (⚠️). Mitigate by truncating to column length if needed (out of scope here; verify column size).

### Traceability
- D-201 → R-102, R-105
- D-202 → R-103, R-104, R-106
- D-203 → R-104
