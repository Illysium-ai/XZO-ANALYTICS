
CREATE OR REPLACE PROCEDURE FORECAST.SP_GENERATE_BUDGET(
    P_BUDGET_CYCLE_DATE DATE,
    P_USER_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    budget_approved_ex EXCEPTION (-20021, 'Budget cycle is approved (locked); cannot regenerate.');
    missing_baseline_ex EXCEPTION (-20022, 'No baseline published forecasts for provided budget cycle date.');
    V_Y INTEGER;
    V_ANCHOR_DATE DATE;
    V_PY_ANCHOR_DATE DATE;
BEGIN
    -- Lock check
    IF (FORECAST.UDF_IS_BUDGET_APPROVED(:P_BUDGET_CYCLE_DATE)) THEN
        RAISE budget_approved_ex;
    END IF;

    -- Determine CY and validate baseline exists for the cycle
    SELECT EXTRACT(YEAR FROM :P_BUDGET_CYCLE_DATE)::INTEGER INTO :V_Y;

    IF (NOT EXISTS (
        SELECT 1
        FROM FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS pf
        JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
          ON pf.PUBLICATION_ID = p.PUBLICATION_ID
        WHERE p.PUBLICATION_STATUS = 'consensus'
          AND pf.FORECAST_GENERATION_MONTH_DATE = :P_BUDGET_CYCLE_DATE
    )) THEN
        RAISE missing_baseline_ex;
    END IF;

    SELECT 
        :P_BUDGET_CYCLE_DATE,
        DATEADD(YEAR, -1, :P_BUDGET_CYCLE_DATE)
    INTO :V_ANCHOR_DATE, :V_PY_ANCHOR_DATE;

    -- Purge existing generated rows for this cycle
    DELETE FROM FORECAST.DEPLETIONS_BUDGET_GENERATED WHERE BUDGET_CYCLE_DATE = :P_BUDGET_CYCLE_DATE;

    -- After core generation, backfill planned product zero coverage and default primary methods (now performed via realtime sync during tag updates)
    -- CALL FORECAST.SP_SEED_ZERO_FOR_PLANNED_PRODUCTS_BUDGET(:P_BUDGET_CYCLE_DATE, :P_USER_ID);

    -- Generate into target table using parity logic
    INSERT INTO FORECAST.DEPLETIONS_BUDGET_GENERATED (
        MARKET_NAME, MARKET_CODE, DISTRIBUTOR_NAME, DISTRIBUTOR_ID,
        BRAND, BRAND_ID, VARIANT, VARIANT_ID, VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID,
        FORECAST_YEAR, MONTH, FORECAST_MONTH_DATE, FORECAST_METHOD,
        CASE_EQUIVALENT_VOLUME, PY_CASE_EQUIVALENT_VOLUME,
        CY_3M_SUM, CY_6M_SUM, CY_12M_SUM, PY_3M_SUM, PY_6M_SUM, PY_12M_SUM,
        RUN_RATE_3M, TREND_FACTOR_3M, TREND_FACTOR_6M, TREND_FACTOR_12M,
        DATA_SOURCE, BUDGET_CYCLE_DATE
    )
    WITH run_details AS (
        SELECT 
            :P_BUDGET_CYCLE_DATE AS latest_complete_month_date,
            :P_BUDGET_CYCLE_DATE AS budget_cycle_date
    ),
    vsp_attributes AS (
        SELECT DISTINCT
            variant_size_pack_id,
            brand,
            brand_id,
            variant,
            variant_id,
            variant_size_pack_desc
        FROM MASTER_DATA.APOLLO_SKU_MASTER
    ),
    -- PF-driven domain keys for the cycle (filter planned/exclusions/eligibility)
    pf_domain AS (
        SELECT DISTINCT
            pf.MARKET_NAME,
            pf.MARKET_CODE,
            pf.DISTRIBUTOR_NAME,
            pf.DISTRIBUTOR_ID,
            va.BRAND,
            va.BRAND_ID,
            va.VARIANT,
            va.VARIANT_ID,
            va.VARIANT_SIZE_PACK_DESC,
            pf.VARIANT_SIZE_PACK_ID
        FROM FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS pf
        JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
          ON pf.PUBLICATION_ID = p.PUBLICATION_ID
        JOIN vsp_attributes va ON pf.VARIANT_SIZE_PACK_ID = va.variant_size_pack_id
        LEFT JOIN MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG tagp
          ON tagp.VARIANT_SIZE_PACK_ID = pf.VARIANT_SIZE_PACK_ID
        JOIN MASTER_DATA.APOLLO_DIST_MASTER dm
          ON dm.MARKET_CODE = pf.MARKET_CODE
         AND dm.DISTRIBUTOR_ID = pf.DISTRIBUTOR_ID
        WHERE p.PUBLICATION_STATUS = 'consensus'
          AND pf.FORECAST_GENERATION_MONTH_DATE = :P_BUDGET_CYCLE_DATE
          AND pf.FORECAST_YEAR = :V_Y
          AND dm.IS_DEPLETIONS_ELIGIBLE = 1
          AND COALESCE(LOWER(tagp.IS_PLANNED), 'true') <> 'false'
          AND (tagp.MARKET_CODE_EXCLUSIONS IS NULL OR ARRAY_SIZE(tagp.MARKET_CODE_EXCLUSIONS) = 0 OR NOT ARRAY_CONTAINS(pf.MARKET_CODE::VARIANT, tagp.MARKET_CODE_EXCLUSIONS))
          AND (tagp.CUSTOMER_ID_EXCLUSIONS IS NULL OR ARRAY_SIZE(tagp.CUSTOMER_ID_EXCLUSIONS) = 0 OR NOT ARRAY_CONTAINS(pf.DISTRIBUTOR_ID::VARIANT, tagp.CUSTOMER_ID_EXCLUSIONS))
    ),
    -- 12-month calendar ending at anchor for windowed RAD sums
    cal AS (
        SELECT
            mm.mon AS month_num,
            DATEADD(MONTH, mm.mon - 12, rd.latest_complete_month_date)::DATE AS month_date,
            rd.latest_complete_month_date
        FROM (SELECT seq4()+1 AS mon FROM TABLE(GENERATOR(ROWCOUNT => 12))) mm
        JOIN run_details rd ON 1=1
    ),
    -- CY RAD volumes aligned to calendar (missing months as 0 via COALESCE)
    rad_cy AS (
        SELECT
            d.MARKET_CODE,
            d.DISTRIBUTOR_ID,
            d.VARIANT_SIZE_PACK_ID,
            d.MARKET_NAME,
            d.DISTRIBUTOR_NAME,
            d.BRAND,
            d.BRAND_ID,
            d.VARIANT,
            d.VARIANT_ID,
            d.VARIANT_SIZE_PACK_DESC,
            c.month_num,
            c.month_date,
            COALESCE(SUM(r.CASE_EQUIVALENT_QUANTITY), 0) AS cy_volume
        FROM pf_domain d
        JOIN cal c ON 1=1
        LEFT JOIN vip.rad_distributor_level_sales r
          ON r.MARKET_CODE = d.MARKET_CODE
         AND r.DISTRIBUTOR_ID = d.DISTRIBUTOR_ID
         AND r.VARIANT_SIZE_PACK_ID = d.VARIANT_SIZE_PACK_ID
         AND r.MONTH_DATE = c.month_date
        GROUP BY d.MARKET_CODE, d.DISTRIBUTOR_ID, d.VARIANT_SIZE_PACK_ID,
                 d.MARKET_NAME, d.DISTRIBUTOR_NAME, d.BRAND, d.BRAND_ID, d.VARIANT, d.VARIANT_ID, d.VARIANT_SIZE_PACK_DESC,
                 c.month_num, c.month_date
    ),
    cy_rolling_sums AS (
        SELECT
            rc.*,
            -- SUM(cy_volume) OVER (PARTITION BY market_code, distributor_id, variant_size_pack_id ORDER BY month_num ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cy_3m_sum,
            -- SUM(cy_volume) OVER (PARTITION BY market_code, distributor_id, variant_size_pack_id ORDER BY month_num ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS cy_6m_sum,
            -- SUM(cy_volume) OVER (PARTITION BY market_code, distributor_id, variant_size_pack_id ORDER BY month_num ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS cy_12m_sum

            sum(cy_volume) over (
            partition by market_code, distributor_id, variant_size_pack_id
            order by month_date
            RANGE BETWEEN INTERVAL '2 months' PRECEDING AND CURRENT ROW
            ) as cy_3m_sum,
            sum(cy_volume) over (
            partition by market_code, distributor_id, variant_size_pack_id
            order by month_date
            RANGE BETWEEN INTERVAL '5 months' PRECEDING AND CURRENT ROW
            ) as cy_6m_sum,
            sum(cy_volume) over (
            partition by market_code, distributor_id, variant_size_pack_id
            order by month_date
            RANGE BETWEEN INTERVAL '11 months' PRECEDING AND CURRENT ROW
            ) as cy_12m_sum
        FROM rad_cy rc
    ),
    -- PY RAD volumes aligned to prior-year calendar months
    rad_py AS (
        SELECT
            d.MARKET_CODE,
            d.DISTRIBUTOR_ID,
            d.VARIANT_SIZE_PACK_ID,
            c.month_num,
            DATEADD(YEAR, -1, c.month_date) AS py_month_date,
            COALESCE(SUM(r.CASE_EQUIVALENT_QUANTITY), 0) AS py_volume
        FROM pf_domain d
        JOIN cal c ON 1=1
        LEFT JOIN vip.rad_distributor_level_sales r
          ON r.MARKET_CODE = d.MARKET_CODE
         AND r.DISTRIBUTOR_ID = d.DISTRIBUTOR_ID
         AND r.VARIANT_SIZE_PACK_ID = d.VARIANT_SIZE_PACK_ID
         AND r.MONTH_DATE = DATEADD(YEAR, -1, c.month_date)
        GROUP BY d.MARKET_CODE, d.DISTRIBUTOR_ID, d.VARIANT_SIZE_PACK_ID, c.month_num, py_month_date
    ),
    py_rolling_sums AS (
        SELECT
            rp.*,
            -- SUM(py_volume) OVER (PARTITION BY market_code, distributor_id, variant_size_pack_id ORDER BY month_num ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS py_3m_sum,
            -- SUM(py_volume) OVER (PARTITION BY market_code, distributor_id, variant_size_pack_id ORDER BY month_num ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS py_6m_sum,
            -- SUM(py_volume) OVER (PARTITION BY market_code, distributor_id, variant_size_pack_id ORDER BY month_num ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS py_12m_sum

            sum(py_volume) over (
            partition by market_code, distributor_id, variant_size_pack_id
            order by py_month_date
            RANGE BETWEEN INTERVAL '2 months' PRECEDING AND CURRENT ROW
            ) as py_3m_sum,
            sum(py_volume) over (
            partition by market_code, distributor_id, variant_size_pack_id
            order by py_month_date
            RANGE BETWEEN INTERVAL '5 months' PRECEDING AND CURRENT ROW
            ) as py_6m_sum,
            sum(py_volume) over (
            partition by market_code, distributor_id, variant_size_pack_id
            order by py_month_date
            RANGE BETWEEN INTERVAL '11 months' PRECEDING AND CURRENT ROW
            ) as py_12m_sum
        FROM rad_py rp
    ),
    base_factors AS (
        SELECT
            d.MARKET_NAME, d.MARKET_CODE, d.DISTRIBUTOR_NAME, d.DISTRIBUTOR_ID,
            d.BRAND, d.BRAND_ID, d.VARIANT, d.VARIANT_ID, d.VARIANT_SIZE_PACK_DESC, d.VARIANT_SIZE_PACK_ID,
            rd.latest_complete_month_date AS latest_complete_month_date,
            CAST(cy.cy_3m_sum / 3 AS NUMBER(28,6)) AS run_rate_3m,
            CAST(LEAST(COALESCE(cy.cy_3m_sum / NULLIF(py.py_3m_sum, 0), 1.0), 1.5) AS NUMBER(28,6)) AS trend_factor_3m,
            CAST(LEAST(COALESCE(cy.cy_6m_sum / NULLIF(py.py_6m_sum, 0), 1.0), 1.5) AS NUMBER(28,6)) AS trend_factor_6m,
            CAST(LEAST(COALESCE(cy.cy_12m_sum / NULLIF(py.py_12m_sum, 0), 1.0), 1.5) AS NUMBER(28,6)) AS trend_factor_12m
        FROM pf_domain d
        JOIN run_details rd ON 1=1
        JOIN cy_rolling_sums cy
          ON cy.market_code = d.market_code
         AND cy.distributor_id = d.distributor_id
         AND cy.variant_size_pack_id = d.variant_size_pack_id
         AND cy.month_num = 12
        LEFT JOIN py_rolling_sums py
          ON py.market_code = d.market_code
         AND py.distributor_id = d.distributor_id
         AND py.variant_size_pack_id = d.variant_size_pack_id
         AND py.month_num = 12
    ),
    tfu AS (
        SELECT market_name, market_code, distributor_name, distributor_id, brand, brand_id, variant, variant_id, variant_size_pack_desc, variant_size_pack_id,
               latest_complete_month_date, 'three_month' AS forecast_method, trend_factor_3m AS trend_factor FROM base_factors
        UNION ALL SELECT market_name, market_code, distributor_name, distributor_id, brand, brand_id, variant, variant_id, variant_size_pack_desc, variant_size_pack_id,
               latest_complete_month_date, 'six_month', trend_factor_6m FROM base_factors
        UNION ALL SELECT market_name, market_code, distributor_name, distributor_id, brand, brand_id, variant, variant_id, variant_size_pack_desc, variant_size_pack_id,
               latest_complete_month_date, 'twelve_month', trend_factor_12m FROM base_factors
        UNION ALL SELECT market_name, market_code, distributor_name, distributor_id, brand, brand_id, variant, variant_id, variant_size_pack_desc, variant_size_pack_id,
               latest_complete_month_date, 'flat', CAST(1.0 AS NUMBER(28,6)) FROM base_factors
        UNION ALL SELECT market_name, market_code, distributor_name, distributor_id, brand, brand_id, variant, variant_id, variant_size_pack_desc, variant_size_pack_id,
               latest_complete_month_date, 'run_rate', run_rate_3m FROM base_factors
    ),
    fm AS (
        SELECT
            DATE_FROM_PARTS(:V_Y + 1, mon, 1)::DATE AS forecast_month_date,
            (:V_Y + 1)::INTEGER AS forecast_year,
            mon::INTEGER AS month,
            rd.budget_cycle_date
        FROM (SELECT seq4()+1 AS mon FROM TABLE(GENERATOR(ROWCOUNT => 12))) mm
        CROSS JOIN run_details rd
    ),
    pya AS (
        SELECT
            pf.MARKET_CODE,
            pf.DISTRIBUTOR_ID,
            pf.VARIANT_SIZE_PACK_ID,
            pf.FORECAST_YEAR,   
            pf.MONTH AS month,
            CAST(pf.CASE_EQUIVALENT_VOLUME AS NUMBER(28,6)) AS py_case_equivalent_volume
        FROM FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS pf
        JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
          ON pf.PUBLICATION_ID = p.PUBLICATION_ID
        WHERE p.PUBLICATION_STATUS = 'consensus'
          AND pf.FORECAST_GENERATION_MONTH_DATE = :P_BUDGET_CYCLE_DATE
    )
    SELECT
        tfu.market_name, tfu.market_code, tfu.distributor_name, tfu.distributor_id,
        tfu.brand, tfu.brand_id, tfu.variant, tfu.variant_id, tfu.variant_size_pack_desc, tfu.variant_size_pack_id,
        fm.forecast_year, fm.month, fm.forecast_month_date, tfu.forecast_method,
        CASE WHEN tfu.forecast_method = 'run_rate' THEN CAST(GREATEST(COALESCE(tfu.trend_factor, 0.0), 0.0) AS NUMBER(28,6))
             ELSE CAST(GREATEST(COALESCE(p.py_case_equivalent_volume, 0.0) * COALESCE(tfu.trend_factor, 1.0), 0.0) AS NUMBER(28,6)) END AS case_equivalent_volume,
        CAST(COALESCE(p.py_case_equivalent_volume, 0.0) AS NUMBER(28,6)) AS py_case_equivalent_volume,
        CAST(rs.cy_3m_sum AS NUMBER(28,6)) AS cy_3m_sum,
        CAST(rs.cy_6m_sum AS NUMBER(28,6)) AS cy_6m_sum,
        CAST(rs.cy_12m_sum AS NUMBER(28,6)) AS cy_12m_sum,
        CAST(prs.py_3m_sum AS NUMBER(28,6)) AS py_3m_sum,
        CAST(prs.py_6m_sum AS NUMBER(28,6)) AS py_6m_sum,
        CAST(prs.py_12m_sum AS NUMBER(28,6)) AS py_12m_sum,
        bf.run_rate_3m, bf.trend_factor_3m, bf.trend_factor_6m, bf.trend_factor_12m,
        'logic_driven' AS data_source,
        fm.budget_cycle_date
    FROM tfu
    JOIN fm ON 1=1
    LEFT JOIN pya p ON p.market_code = tfu.market_code AND p.distributor_id = tfu.distributor_id AND p.variant_size_pack_id = tfu.variant_size_pack_id AND p.forecast_year+1 = fm.forecast_year AND p.month = fm.month
    LEFT JOIN cy_rolling_sums rs ON rs.market_code = tfu.market_code AND rs.distributor_id = tfu.distributor_id AND rs.variant_size_pack_id = tfu.variant_size_pack_id AND rs.month_num = 12
    LEFT JOIN py_rolling_sums prs ON prs.market_code = tfu.market_code AND prs.distributor_id = tfu.distributor_id AND prs.variant_size_pack_id = tfu.variant_size_pack_id AND prs.py_month_date = :V_PY_ANCHOR_DATE
    LEFT JOIN base_factors bf ON bf.market_code = tfu.market_code AND bf.distributor_id = tfu.distributor_id AND bf.variant_size_pack_id = tfu.variant_size_pack_id AND bf.latest_complete_month_date = :V_ANCHOR_DATE;

    -- Zero-seed planned products missing for this cycle (handles newly exposed since last publication)
    INSERT INTO FORECAST.DEPLETIONS_BUDGET_GENERATED (
        MARKET_NAME, MARKET_CODE, DISTRIBUTOR_NAME, DISTRIBUTOR_ID,
        BRAND, BRAND_ID, VARIANT, VARIANT_ID, VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID,
        FORECAST_YEAR, MONTH, FORECAST_MONTH_DATE, FORECAST_METHOD,
        CASE_EQUIVALENT_VOLUME, PY_CASE_EQUIVALENT_VOLUME,
        CY_3M_SUM, CY_6M_SUM, CY_12M_SUM, PY_3M_SUM, PY_6M_SUM, PY_12M_SUM,
        RUN_RATE_3M, TREND_FACTOR_3M, TREND_FACTOR_6M, TREND_FACTOR_12M,
        DATA_SOURCE, BUDGET_CYCLE_DATE
    )
    WITH planned_universe AS (
        SELECT
            adm.MARKET_CODE,
            adm.DISTRIBUTOR_ID,
            avst.VARIANT_SIZE_PACK_ID,
            avst.VARIANT_SIZE_PACK_DESC
        FROM MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG avst
        CROSS JOIN (
            SELECT DISTINCT d.MARKET_CODE, d.DISTRIBUTOR_ID
            FROM MASTER_DATA.APOLLO_DIST_MASTER d
            WHERE d.IS_DEPLETIONS_ELIGIBLE = 1
        ) adm
        WHERE LOWER(avst.IS_PLANNED) = 'true'
          AND (avst.MARKET_CODE_EXCLUSIONS IS NULL OR ARRAY_SIZE(avst.MARKET_CODE_EXCLUSIONS) = 0 OR NOT ARRAY_CONTAINS(adm.MARKET_CODE::VARIANT, avst.MARKET_CODE_EXCLUSIONS))
          AND (avst.CUSTOMER_ID_EXCLUSIONS IS NULL OR ARRAY_SIZE(avst.CUSTOMER_ID_EXCLUSIONS) = 0 OR NOT ARRAY_CONTAINS(adm.DISTRIBUTOR_ID::VARIANT, avst.CUSTOMER_ID_EXCLUSIONS))
    ),
    -- Product-level dedupe since APOLLO_SKU_MASTER is more granular than VSP
    vsp_attributes AS (
        SELECT DISTINCT
            variant_size_pack_id,
            brand,
            brand_id,
            variant,
            variant_id
        FROM MASTER_DATA.APOLLO_SKU_MASTER
    ),
    -- Distributor names per market_code + distributor_id
    distributor_names AS (
        SELECT DISTINCT
            market_code,
            market_name,
            distributor_id,
            distributor_name
        FROM MASTER_DATA.APOLLO_DIST_MASTER
        WHERE is_depletions_eligible = 1
    ),
    months AS (
        SELECT (:V_Y + 1)::INTEGER AS y
    ),
    zeros AS (
        SELECT 
            d.MARKET_NAME,
            u.MARKET_CODE,
            d.DISTRIBUTOR_NAME,
            u.DISTRIBUTOR_ID,
            s.BRAND,
            s.BRAND_ID,
            s.VARIANT,
            s.VARIANT_ID,
            u.VARIANT_SIZE_PACK_DESC,
            u.VARIANT_SIZE_PACK_ID,
            m.y AS FORECAST_YEAR,
            mon AS MONTH,
            DATE_FROM_PARTS(m.y, mon, 1)::DATE AS FORECAST_MONTH_DATE,
            mth.forecast_method,
            0.0::NUMBER AS CASE_EQUIVALENT_VOLUME,
            NULL::NUMBER AS PY_CASE_EQUIVALENT_VOLUME,
            NULL::NUMBER AS CY_3M_SUM,
            NULL::NUMBER AS CY_6M_SUM,
            NULL::NUMBER AS CY_12M_SUM,
            NULL::NUMBER AS PY_3M_SUM,
            NULL::NUMBER AS PY_6M_SUM,
            NULL::NUMBER AS PY_12M_SUM,
            NULL::NUMBER AS RUN_RATE_3M,
            NULL::NUMBER AS TREND_FACTOR_3M,
            NULL::NUMBER AS TREND_FACTOR_6M,
            NULL::NUMBER AS TREND_FACTOR_12M,
            'zero_seeded' AS DATA_SOURCE,
            :P_BUDGET_CYCLE_DATE AS BUDGET_CYCLE_DATE
        FROM planned_universe u
        JOIN distributor_names d
          ON d.MARKET_CODE = u.MARKET_CODE
         AND d.DISTRIBUTOR_ID = u.DISTRIBUTOR_ID
        LEFT JOIN vsp_attributes s
          ON s.VARIANT_SIZE_PACK_ID = u.VARIANT_SIZE_PACK_ID
        CROSS JOIN months m
        CROSS JOIN (SELECT seq4()+1 AS mon FROM TABLE(GENERATOR(ROWCOUNT => 12))) mm
        cross join (
            select 'three_month' as forecast_method
            union all select 'six_month'
            union all select 'twelve_month'
            union all select 'flat'
            union all select 'run_rate'
        ) mth
    )
    SELECT z.*
    FROM zeros z
    LEFT JOIN FORECAST.DEPLETIONS_BUDGET_GENERATED g
      ON g.MARKET_CODE = z.MARKET_CODE
     AND g.DISTRIBUTOR_ID = z.DISTRIBUTOR_ID
     AND g.VARIANT_SIZE_PACK_ID = z.VARIANT_SIZE_PACK_ID
     AND g.FORECAST_YEAR = z.FORECAST_YEAR
     AND g.MONTH = z.MONTH
     AND g.BUDGET_CYCLE_DATE = z.BUDGET_CYCLE_DATE
    WHERE g.VARIANT_SIZE_PACK_ID IS NULL;

    -- Seed budget primary methods for this cycle from published consensus methodology (one-time per key)
    INSERT INTO FORECAST.DEPLETIONS_BUDGET_PRIMARY_METHOD (
        BUDGET_CYCLE_DATE, MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID, FORECAST_METHOD, UPDATED_BY_USER_ID, UPDATED_AT
    )
    SELECT
        :P_BUDGET_CYCLE_DATE AS BUDGET_CYCLE_DATE,
        g.MARKET_CODE,
        g.DISTRIBUTOR_ID,
        g.VARIANT_SIZE_PACK_ID,
        COALESCE(pm.FORECAST_METHOD, 'six_month') AS FORECAST_METHOD,
        :P_USER_ID AS UPDATED_BY_USER_ID,
        CURRENT_TIMESTAMP() AS UPDATED_AT
    FROM (
        SELECT DISTINCT MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID
        FROM FORECAST.DEPLETIONS_BUDGET_GENERATED
        WHERE BUDGET_CYCLE_DATE = :P_BUDGET_CYCLE_DATE
    ) g
    LEFT JOIN (
        SELECT
            pf.MARKET_CODE,
            pf.DISTRIBUTOR_ID,
            pf.VARIANT_SIZE_PACK_ID,
            MIN(pf.FORECAST_METHOD) AS FORECAST_METHOD
        FROM FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS pf
        JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
          ON pf.PUBLICATION_ID = p.PUBLICATION_ID
        WHERE p.PUBLICATION_STATUS = 'consensus'
          AND pf.FORECAST_GENERATION_MONTH_DATE = :P_BUDGET_CYCLE_DATE
        GROUP BY pf.MARKET_CODE, pf.DISTRIBUTOR_ID, pf.VARIANT_SIZE_PACK_ID
    ) pm
      ON pm.MARKET_CODE = g.MARKET_CODE
     AND pm.DISTRIBUTOR_ID = g.DISTRIBUTOR_ID
     AND pm.VARIANT_SIZE_PACK_ID = g.VARIANT_SIZE_PACK_ID
    LEFT JOIN FORECAST.DEPLETIONS_BUDGET_PRIMARY_METHOD existing
      ON existing.BUDGET_CYCLE_DATE = :P_BUDGET_CYCLE_DATE
     AND existing.MARKET_CODE = g.MARKET_CODE
     AND existing.DISTRIBUTOR_ID = g.DISTRIBUTOR_ID
     AND existing.VARIANT_SIZE_PACK_ID = g.VARIANT_SIZE_PACK_ID
    WHERE existing.BUDGET_CYCLE_DATE IS NULL;

    RETURN 'SUCCESS: Generated budget for ' || TO_VARCHAR(:P_BUDGET_CYCLE_DATE, 'YYYY-MM-DD');
END;
$$;
