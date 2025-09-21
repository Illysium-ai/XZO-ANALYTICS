-- Snowflake SQL UDF for is_budget_approved
-- Target Schema: APOLLO_DEVELOPMENT.FORECAST


CREATE OR REPLACE FUNCTION FORECAST.UDF_IS_BUDGET_APPROVED(
    P_BUDGET_CYCLE_DATE DATE
)
RETURNS BOOLEAN
LANGUAGE SQL
COMMENT = 'Checks if a budget cycle is approved (locked) for the given market and budget cycle date (FGMD). Returns TRUE if approved, otherwise FALSE.'
AS
$$
    SELECT EXISTS (
        SELECT 1
        FROM FORECAST.DEPLETIONS_BUDGET_APPROVALS a
        -- Market-level locks: either lock is global or scoped by market via join to presence in working set
        WHERE a.BUDGET_CYCLE_DATE = P_BUDGET_CYCLE_DATE
    )
$$;

COMMENT ON FUNCTION FORECAST.UDF_IS_BUDGET_APPROVED(DATE) IS
'Checks if a budget cycle is approved (locked). Returns TRUE if any matching approval exists for the cycle.';
