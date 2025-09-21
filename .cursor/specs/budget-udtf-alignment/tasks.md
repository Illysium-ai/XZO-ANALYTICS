### Tasks — Budget Depletions UDTF Alignment

- T-001 (todo): Validate `VW_GET_BUDGET_BASE` columns and map to target schema
  - Links: R-005
  - Steps: DESCRIBE VIEW; document mappings; identify gaps (set to NULL if missing)
  - Outcome: Confirmed field availability and final mapping table
  - Test hints: Quick SELECT from view to sample required fields
  - Complexity: S

- T-002 (completed): Update UDTF return schema to aligned columns and types
  - Links: R-005
  - Steps: Edit `udtf_get_depletions_budget.sql` RETURNS TABLE; add columns in target order
  - Outcome: Compilable function signature
  - Test hints: Create/replace function in dev; DESCRIBE FUNCTION output
  - Complexity: M; Dep: T-001

- T-003 (completed): Implement Stage 1 filters mirroring forecast UDTF
  - Links: R-001, R-002, R-003
  - Steps: Apply `ARRAY_CONTAINS`/NULL-safe predicates with casts; ensure budget-cycle filter
  - Outcome: Filtered base CTE
  - Test hints: Spot-check counts with/without filters
  - Complexity: S; Dep: T-002

- T-004 (completed): Implement primary-method join gating via `P_ONLY_PRIMARY`
  - Links: R-004, R-007
  - Steps: Join to `DEPLETIONS_BUDGET_PRIMARY_METHOD` with conditional inclusion
  - Outcome: Default-to-primary when `P_FORECAST_METHOD` is NULL; parameter made optional (default NULL)
  - Test hints: Compare row counts for TRUE vs FALSE (logic now ignores flag when method is NULL)
  - Complexity: S; Dep: T-003

- T-005 (completed): Implement aggregation and output projection
  - Links: R-005, R-006
  - Steps: `ROUND(SUM(...),2)` aggregates; `ANY_VALUE` for rates; `NULL` projections for missing dims
  - Outcome: Final SELECT aligned to target schema and order; excluded specified columns; updated view to include market_area_name from apollo_dist_master join; replaced all NULL projections with actual field mappings
  - Test hints: Column existence/order; numeric rounding; view updated via dbt run
  - Complexity: M; Dep: T-004

- T-006 (completed): Dev deploy and validation
  - Links: all
  - Steps: Execute via `snow sql -f`; function created successfully in `APOLLO_DEVELOPMENT.FORECAST`
  - Outcome: Function available in dev; signature verified by successful create
  - Test hints: Sample calls differing by filters
  - Complexity: S; Dep: T-005

- T-007 (todo): Documentation and changelog
  - Links: all
  - Steps: Update README/docs; record changes
  - Outcome: Clear guidance for consumers
  - Complexity: XS; Dep: T-006

- T-008 (todo): PR with spec references
  - Links: all
  - Steps: Commit, open PR referencing this spec
  - Outcome: Reviewed and merged changes
  - Complexity: XS; Dep: T-007
