CREATE OR REPLACE PROCEDURE FORECAST.SP_APPROVE_BUDGET(
    P_BUDGET_CYCLE_DATE DATE,
    P_APPROVED_BY_USER_ID VARCHAR,
    P_APPROVAL_NOTE TEXT,
    P_LOCK BOOLEAN
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    already_approved_ex EXCEPTION (-20001, 'Budget cycle is already approved.');
    V_COUNT INTEGER;
BEGIN
    IF (:P_LOCK) THEN
        -- Approve (lock) path: insert approval record if not present
        SELECT COUNT(*) INTO :V_COUNT
        FROM FORECAST.DEPLETIONS_BUDGET_APPROVALS
        WHERE BUDGET_CYCLE_DATE = :P_BUDGET_CYCLE_DATE;

        IF (V_COUNT > 0) THEN
            RAISE already_approved_ex;
        END IF;

        INSERT INTO FORECAST.DEPLETIONS_BUDGET_APPROVALS (
            BUDGET_CYCLE_DATE, APPROVED_BY_USER_ID, APPROVAL_NOTE
        ) VALUES (
            :P_BUDGET_CYCLE_DATE, :P_APPROVED_BY_USER_ID, :P_APPROVAL_NOTE
        );

        RETURN 'SUCCESS: Approved (locked) budget for cycle ' || TO_VARCHAR(:P_BUDGET_CYCLE_DATE, 'YYYY-MM-DD') || ' by ' || :P_APPROVED_BY_USER_ID || '.';
    ELSE
        -- Unlock path: remove approval record if present
        SELECT COUNT(*) INTO :V_COUNT
        FROM FORECAST.DEPLETIONS_BUDGET_APPROVALS
        WHERE BUDGET_CYCLE_DATE = :P_BUDGET_CYCLE_DATE;

        IF (V_COUNT = 0) THEN
            RETURN 'SUCCESS: Budget already unlocked for cycle ' || TO_VARCHAR(:P_BUDGET_CYCLE_DATE, 'YYYY-MM-DD') || '.';
        END IF;

        DELETE FROM FORECAST.DEPLETIONS_BUDGET_APPROVALS
        WHERE BUDGET_CYCLE_DATE = :P_BUDGET_CYCLE_DATE;

        RETURN 'SUCCESS: Unlocked budget for cycle ' || TO_VARCHAR(:P_BUDGET_CYCLE_DATE, 'YYYY-MM-DD') || ' by ' || :P_APPROVED_BY_USER_ID || '.';
    END IF;
END;
$$;
