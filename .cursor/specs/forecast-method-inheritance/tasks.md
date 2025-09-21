# Forecast Method Inheritance — Tasks

- T-001 — Add market-level fallback CTE and integrate selection logic
  - Linked: R-001, R-002, R-003, R-004
  - Steps:
    1) Create CTE that selects prior FGMD market/VSP method tied to the minimum distributor_id
    2) Left join into `forecast_data_with_potential_method`
    3) Update CASE to use order: previous_consensus → distributor carry-over → market-level via min(distributor_id) → six_month
  - Expected: New distributors inherit market method when present; tests validate outcomes
  - Test hints: Create fixture with multiple prior methods; verify min(distributor_id) method is chosen
  - Complexity: Medium
  - Status: completed
  - Notes: SQL edits applied to `depletions_forecast_primary_forecast_method.sql` with min(distributor_id) selection; `dbt compile` succeeded.

- T-002 — Validate incremental behavior and uniqueness
  - Linked: R-005, R-006
  - Steps:
    1) Run dbt for one FGMD; re-run for same FGMD; verify no changes to existing rows
    2) Confirm unique key remains satisfied
  - Expected: No churn on re-run; only new rows inserted
  - Complexity: Low
  - Status: in-progress

- T-003 — Update docs and changelog
  - Linked: All
  - Steps:
    1) Add brief note in `docs/` or model description about inheritance behavior
    2) Update `CHANGELOG.md`
  - Expected: Clear operator-facing notes
  - Complexity: Low
  - Status: todo

- T-004 — Optional: Add model tests
  - Linked: R-001..R-004
  - Steps:
    1) Add assertions in `tests/` for deterministic selection and fallback order
  - Expected: Regression coverage
  - Complexity: Medium
  - Status: todo
