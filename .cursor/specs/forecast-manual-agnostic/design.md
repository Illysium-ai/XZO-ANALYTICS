# Design — Forecast Manual Inputs Method-Agnostic

## Overview
Manual forecast overrides must persist regardless of the selected forecast method. We will standardize storage to a single row per monthly grain (FGMD + Market + Distributor + SKU + Year + Month) and read overrides method-agnostically. We will continue tracking the primary forecast method separately.

## Key Changes (trace → R-###)
- Stored Procedure `FORECAST.SP_BATCH_SAVE_FORECASTS` (R-101, R-102, R-106)
  - Upsert manual rows ignoring `FORECAST_METHOD` in the key; set `FORECAST_METHOD = 'MANUAL'` on write.
  - Duplicate detection ignores method in GROUP BY.
  - Versioning continues, keyed to the normalized row.
  - Primary method update continues via `DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD`.

- Views `vw_get_depletions_base` and `vw_get_depletions_base_chains` (R-103, R-108)
  - Left join manual overrides by keys excluding method.
  - Provide derived `OVERRIDDEN_CASE_EQUIVALENT_VOLUME = COALESCE(m.MANUAL_CASE_EQUIVALENT_VOLUME, f.CASE_EQUIVALENT_VOLUME)` while keeping existing columns.

- Sync Procedure `FORECAST._INTERNAL_SP_SYNC_CONSENSUS_TO_NEXT_MONTH` (R-105, R-106)
  - Source deduplication ignores method; target uses `FORECAST_METHOD = 'MANUAL'` and keys excluding method.
  - Versioning via single-row target.

## Data contracts / Keys
- Manual overrides key: `(FORECAST_GENERATION_MONTH_DATE, MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH)`
- Sentinel for method in manual table: `'MANUAL'`
- Primary method table remains unchanged.

## Diagram
```mermaid
sequenceDiagram
  participant UI as UI
  participant SP as SP_BATCH_SAVE_FORECASTS
  participant MF as MANUAL_INPUT_DEPLETIONS_FORECAST
  participant PF as DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD
  participant V as vw_get_depletions_base

  UI->>SP: Save payload (JSON)
  SP->>MF: MERGE on keys (ignore method); set FORECAST_METHOD='MANUAL'
  SP-->>MF: INSERT/UPDATE + version append
  SP->>PF: UPDATE primary method (from payload)
  UI->>V: Query grid
  V->>MF: LEFT JOIN by keys (ignore method)
  V-->>UI: Expose base, manual, overridden volume
```

## Decisions / Trade-offs
- Store sentinel `'MANUAL'` instead of dropping column: avoids DDL changes and minimizes migration risk.
- Deduplicate by latest `UPDATED_AT`: clear and available across records; alternative (max version) is similar but less universal.
- Keep backward-compatible columns and add derived field: reduces downstream breakage.

## Risks and Mitigations
- Risk: Existing duplicate rows across methods could conflict with new joins.
  - Mitigation: Historical cleanup will be handled offline; joins also qualify to prefer `'MANUAL'` if present.
- Risk: Downstream models relying on method-specific manual rows.
  - Mitigation: Add derived column and keep original columns; document change.
- Risk: Consensus sync could create duplicates if not deduped.
  - Mitigation: Source dedupe and target MERGE on method-agnostic keys with sentinel method.
