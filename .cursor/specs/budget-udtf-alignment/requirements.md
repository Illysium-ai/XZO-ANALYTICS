### Feature: Budget Depletions UDTF Alignment

- Scope: Update `FORECAST.UDTF_GET_DEPLETIONS_BUDGET` to align with `FORECAST.UDTF_GET_DEPLETIONS_FORECAST` columns and behavior, excluding specified columns.

#### EARS Requirements
- R-001 — WHEN `P_BUDGET_CYCLE_DATE` is provided, THE SYSTEM SHALL return rows only for that cycle date.
  - Acceptance: Calling with `P_BUDGET_CYCLE_DATE = X` returns only rows with `BUDGET_CYCLE_DATE = X`.
- R-002 — WHEN optional filter arrays (`P_MARKETS`, `P_CUSTOMERS`, `P_VARIANT_SIZE_PACK_IDS`) are provided, THE SYSTEM SHALL filter using `ARRAY_CONTAINS` semantics identical to the forecast UDTF.
  - Acceptance: Filters behave inclusively; empty arrays or NULL skip filtering.
- R-003 — WHEN `P_FORECAST_METHOD` is provided, THE SYSTEM SHALL filter to only matching budget forecast method.
  - Acceptance: Only rows with `FORECAST_METHOD = P_FORECAST_METHOD` are returned.
- R-004 — WHEN `P_ONLY_PRIMARY` is `TRUE`, THE SYSTEM SHALL return rows only for the primary method determined by `FORECAST.DEPLETIONS_BUDGET_PRIMARY_METHOD`; otherwise, return all.
  - Acceptance: For `TRUE`, returned rows have `FORECAST_METHOD` matching the primary mapping.
- R-005 — WHEN returning results, THE SYSTEM SHALL replicate the core forecast UDTF columns except the following exclusions: `GROUP_ID`, `PUBLICATION_ID`, `TAG_ID`, `TAG_NAME`, `PROJECTED_CASE_EQUIVALENT_VOLUME`, `PREV_PUBLISHED_CASE_EQUIVALENT_VOLUME`, `GROSS_SALES_VALUE`, `PY_GROSS_SALES_VALUE`.
  - Acceptance: Output includes the following columns, in this order:
    - MARKET_ID, MARKET, MARKET_AREA_NAME, CUSTOMER_ID, CUSTOMER,
      BRAND, VARIANT, VARIANT_ID, VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID,
      YEAR, MONTH, FORECAST_METHOD, BUDGET_CYCLE_DATE,
      DATA_TYPE, IS_MANUAL_INPUT, FORECAST_STATUS, CURRENT_VERSION,
      COMMENT, CASE_EQUIVALENT_VOLUME, PY_CASE_EQUIVALENT_VOLUME,
      CY_3M_CASE_EQUIVALENT_VOLUME, CY_6M_CASE_EQUIVALENT_VOLUME, CY_12M_CASE_EQUIVALENT_VOLUME,
      PY_3M_CASE_EQUIVALENT_VOLUME, PY_6M_CASE_EQUIVALENT_VOLUME, PY_12M_CASE_EQUIVALENT_VOLUME,
      GSV_RATE
- R-006 — WHEN computing numeric aggregates, THE SYSTEM SHALL aggregate consistently with the forecast UDTF (e.g., use `ROUND(SUM(...), 2)` and `ANY_VALUE` where applicable).
  - Acceptance: Aggregates and rounding match forecast behavior for comparable fields.
- R-007 — WHEN `P_FORECAST_METHOD` is NULL, THE SYSTEM SHALL return only the primary method rows (consistent with forecast UDTF behavior that defaults to primary when method is not specified).
  - Acceptance: For NULL method and `P_ONLY_PRIMARY` falsey, default to primary method results.

#### Notes / Defaults
- If `VW_GET_BUDGET_BASE` lacks a required dimension (e.g., `MARKET_AREA_NAME`), default to NULL for that column without failing.
- `YEAR` equals budget forecast year (`FORECAST_YEAR`).
- `CUSTOMER_ID` corresponds to the first five characters of `DISTRIBUTOR_ID`.
