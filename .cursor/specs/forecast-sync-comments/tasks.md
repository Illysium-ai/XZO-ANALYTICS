### Tasks for feature: forecast-sync-comments

- T-301 (completed): Update `_INTERNAL_SP_SYNC_CONSENSUS_TO_NEXT_MONTH` INSERT/UPDATE comment logic
  - Links: R-102, R-103, R-104, R-105, R-106
  - Steps:
    1) Edit INSERT values to append `NVL2(source.COMMENT, ' Original: ' || source.COMMENT, '')` to the banner.
    2) Extend MATCHED predicate to include comment mismatch, and set `target.COMMENT` using the same expression as INSERT.
    3) Keep versions MERGE unchanged.
  - Outcome: New FGMD rows include `Original:` when source comment exists; existing rows refresh comments even when volume is unchanged.
  - Test hints: Re-run on a QA branch with a controlled market; verify strings and versioning.
  - Complexity: S
  - Dependencies: none

- T-302 (todo): Add QA check query and dbt test for comment propagation
  - Links: R-101, R-102, R-103
  - Steps: Create a dbt test or analysis that, given `P_CURRENT_FGMD` and keys, asserts presence of `Original:` when source comment is not null.
  - Outcome: Guardrail to catch regressions.
  - Complexity: S
  - Dependencies: T-301

- T-303 (in-progress): Optional backfill for the most recent cycle to restore missing `Original:` suffix
  - Links: R-102, R-105
  - Steps: Write a one-time SQL script to update next-FGMD rows where `COMMENT LIKE 'Auto-synced from <P_CURRENT_FGMD> consensus.'` and the corresponding source comment is non-null; set the comment to the banner + `Original: <source>`.
  - Outcome: Aligns production data with expected behavior.
  - Complexity: M
  - Dependencies: T-301

- T-304 (todo): Documentation update in `docs/adr` and runbook
  - Links: R-106
  - Steps: Document the invariant for comment propagation and idempotency.
  - Outcome: Shared understanding; easier audits.
  - Complexity: XS
  - Dependencies: T-301
