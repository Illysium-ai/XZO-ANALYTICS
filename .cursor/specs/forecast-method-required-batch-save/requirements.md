# Forecast Method Required in Batch Save Budgets

## Scope
Adds validation to `FORECAST.SP_BATCH_SAVE_BUDGETS` so each JSON record must include `market_code` and a valid `forecast_method` from a fixed allowed set.

## EARS Requirements
- R-001: WHEN `FORECAST.SP_BATCH_SAVE_BUDGETS` is invoked, THE SYSTEM SHALL require each element of `P_BUDGETS_JSON` to include `market_code` (non-empty).
- R-002: WHEN `FORECAST.SP_BATCH_SAVE_BUDGETS` is invoked, THE SYSTEM SHALL require each element of `P_BUDGETS_JSON` to include `forecast_method` (non-empty) and ensure it is one of: `three_month`, `six_month`, `twelve_month`, `flat`, `run_rate`.
- R-003: WHEN any record fails R-001 or R-002, THE SYSTEM SHALL raise `missing_required_fields_ex` (-20013) with the message: "Missing required field. Please provide market_code and a valid forecast_method in each record (allowed: three_month, six_month, twelve_month, flat, run_rate)."
- R-004: WHEN validation passes, THE SYSTEM SHALL proceed with existing behavior (MERGE to `FORECAST.MANUAL_INPUT_DEPLETIONS_BUDGET`, write versions, update primary method) unchanged.

## Acceptance Criteria
- AC-1 (R-001, R-002, R-003): Submitting a payload with any record missing `forecast_method` raises -20013 with the message in R-003.
- AC-2 (R-002, R-003): Submitting a payload with any record where `forecast_method` is not one of the allowed values raises -20013 with the message in R-003.
- AC-3 (R-004): Submitting a payload where all records have allowed `forecast_method` values succeeds and returns the standard success message.
- AC-4 (R-004): Existing validations for volume-related fields remain unchanged and only trigger when `manual_case_equivalent_volume` is present.

## Notes
- The allowed values are intentionally lowercase and canonical. Clients must send one of these exact values.
