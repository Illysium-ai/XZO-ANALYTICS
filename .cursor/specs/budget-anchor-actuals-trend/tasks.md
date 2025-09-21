# Tasks — Budget Anchor & Actuals-Based Factors

- T-001 (R-001): Derive `Y` from `P_BUDGET_CYCLE_DATE`; set `ANCHOR`/`PY_ANCHOR` variables. Status: completed
  - Steps: Update DECLARE; set `V_Y`, `V_ANCHOR_DATE`, `V_PY_ANCHOR_DATE`.
  - Outcome: Anchor equals `P_BUDGET_CYCLE_DATE`.

- T-002 (R-006): Validate baseline exists for `P_BUDGET_CYCLE_DATE`. Status: completed
  - Steps: COUNT consensus rows; raise `missing_baseline_ex` if 0.

- T-003 (R-002,R-003): Replace `monthly_data` to source CY actuals from RAD up to anchor; keep planned filter. Status: completed
  - Steps: Use `VIP.RAD_DISTRIBUTOR_LEVEL_SALES`; compute rolling sums.

- T-004 (R-002): Ensure PY rolling sums from RAD up to `PY_ANCHOR`. Status: completed

- T-005 (R-005): Generate `fm` months for Jan..Dec of `Y+1`. Status: completed

- T-006 (R-004): Apply constant factors to all 12 months by scaling CY published totals; preserve run_rate behavior. Status: completed

- T-007 (R-007): Keep `CY_*`, `PY_*`, and `RUN_RATE_3M` columns populated as-of anchor. Status: completed

- T-008 (R-008): Preserve zero seeding and primary method seeding. Status: completed

- T-009: Tests & verification. Status: completed
  - Steps: In dev, run `CALL FORECAST.SP_GENERATE_BUDGET('2025-07-01', 'dev_user');` and validate:
    - Months are 2026-01..12
    - Trend factors consistent across months per method
    - Run rate constant equals 3-month avg as-of 2025-07-01
    - Balvenie/USANY1: Budget PY on keys equals PF total on keys; zero-seeded rows show non-null PY

- T-010: Update CHANGELOG and PR description. Status: todo

- T-011 (R-009): Backfill `PY_CASE_EQUIVALENT_VOLUME` for zero-seeded rows from PF baseline. Status: completed
  - Steps: Add `pfz` CTE to zero-seed statement; join by `(market_code, distributor_id, variant_size_pack_id, month)`; set PY from PF.
  - Outcome: Budget PY equals PF for matched keys; zero-seeded rows no longer have PY NULL.

- T-012 (R-009,R-002,R-003): PF-driven domain and calendarized RAD windows. Status: completed
  - Steps: Domain from PF (cycle consensus) with planned/exclusions/eligibility; add 12-month calendar; compute CY/PY windows and factors at anchor.
  - Outcome: Full PF coverage; Balvenie/USANY1 PY aligns with PF (delta ~ 0).
