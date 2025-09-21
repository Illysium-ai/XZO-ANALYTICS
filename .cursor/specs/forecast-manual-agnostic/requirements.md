# Forecast Manual Inputs — Method-Agnostic Refactor Requirements

## Scope
Make manual forecast inputs method-agnostic (persist regardless of selected forecast method), align behavior to existing budget workflow, and adapt affected procedures and views.

## Requirements (EARS)
- R-101 — Save behavior
  - WHEN a user submits manual forecast entries for a given `FORECAST_GENERATION_MONTH_DATE` and SKU/Distributor/Month,
    THE SYSTEM SHALL upsert one manual override row keyed by `(FGMD, MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH)` regardless of the chosen forecast method.
  - Acceptance:
    - Upserting the same keys with a different `forecast_method` yields a single row (no duplicates) representing the latest value.

- R-102 — Duplicate detection in payload
  - WHEN multiple payload records represent the same keys `(market_code, customer_id, variant_size_pack_id, forecast_year, month)` ignoring method, 
    THE SYSTEM SHALL reject the save with a clear duplicate error.
  - Acceptance:
    - Error is raised and no partial updates occur.

- R-103 — Read/join semantics
  - WHEN retrieving depletions forecasts for any method, 
    THE SYSTEM SHALL apply manual overrides (if present) method-agnostically by joining on keys excluding `FORECAST_METHOD`.
  - Acceptance:
    - Views expose: base volume, manual volume, and a derived `OVERRIDDEN_CASE_EQUIVALENT_VOLUME = COALESCE(manual, base)`.

- R-104 — Primary method tracking
  - WHEN a user changes or selects a primary forecast method, 
    THE SYSTEM SHALL persist it in `DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD` without impacting manual override semantics.
  - Acceptance:
    - Manual rows persist unaltered; primary method table updates independently.

- R-105 — Consensus→Next-month sync
  - WHEN syncing consensus to next FGMD, 
    THE SYSTEM SHALL copy manual overrides method-agnostically (deduped across methods) into a single manual row per keys in the next month.
  - Acceptance:
    - No duplicate manual rows are produced for the next FGMD across different methods.

- R-106 — Versioning
  - WHEN a manual override is updated, 
    THE SYSTEM SHALL append a new version row reflecting the new state.
  - Acceptance:
    - Version numbers increase by one and are queryable for history.

- R-108 — Backward compatibility
  - WHEN downstream queries rely on existing columns, 
    THE SYSTEM SHALL keep existing fields intact and add new derived columns where needed to avoid breaking consumers.
  - Acceptance:
    - No failing dbt models introduced; marts compile successfully.

## Assumptions / Defaults
- Manual override sentinel stored value will be `FORECAST_METHOD = 'MANUAL'` for newly saved rows.
- Budget workflow remains unchanged; we mirror its method-agnostic pattern.
- No change to published snapshots logic; this only affects manual inputs and base views.
