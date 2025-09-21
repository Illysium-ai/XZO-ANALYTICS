{{
  config(
    materialized = 'table'
  )
}}

with 
state_mapping AS (
    SELECT t.market_name,
      t.state_code
      FROM ( 
        VALUES 
          ('Alabama'::VARCHAR,'AL'::VARCHAR), ('Alaska'::VARCHAR,'AK'::VARCHAR), 
          ('Arizona'::VARCHAR,'AZ'::VARCHAR), ('Arkansas'::VARCHAR,'AR'::VARCHAR), 
          ('California'::VARCHAR,'CA'::VARCHAR), ('Colorado'::VARCHAR,'CO'::VARCHAR), 
          ('Connecticut'::VARCHAR,'CT'::VARCHAR), ('Delaware'::VARCHAR,'DE'::VARCHAR), 
          ('District Of Columbia'::VARCHAR,'DC'::VARCHAR), ('Florida'::VARCHAR,'FL'::VARCHAR), 
          ('Georgia (USA)'::VARCHAR,'GA'::VARCHAR), ('Hawaii'::VARCHAR,'HI'::VARCHAR), 
          ('Idaho'::VARCHAR,'ID'::VARCHAR), ('Illinois'::VARCHAR,'IL'::VARCHAR), 
          ('Indiana'::VARCHAR,'IN'::VARCHAR), ('Iowa'::VARCHAR,'IA'::VARCHAR), 
          ('Kansas'::VARCHAR,'KS'::VARCHAR), ('Kentucky'::VARCHAR,'KY'::VARCHAR), 
          ('Louisiana'::VARCHAR,'LA'::VARCHAR), ('Maine'::VARCHAR,'ME'::VARCHAR), 
          ('Maryland - All Other'::VARCHAR,'MDO'::VARCHAR), ('Maryland - Montgomery County'::VARCHAR,'MDC'::VARCHAR), 
          ('Massachusetts'::VARCHAR,'MA'::VARCHAR), ('Michigan'::VARCHAR,'MI'::VARCHAR), 
          ('Minnesota'::VARCHAR,'MN'::VARCHAR), ('Mississippi'::VARCHAR,'MS'::VARCHAR), 
          ('Missouri'::VARCHAR,'MO'::VARCHAR), ('Montana'::VARCHAR,'MT'::VARCHAR), 
          ('Nebraska'::VARCHAR,'NE'::VARCHAR), ('Nevada'::VARCHAR,'NV'::VARCHAR), 
          ('New Hampshire'::VARCHAR,'NH'::VARCHAR), ('New Jersey'::VARCHAR,'NJ'::VARCHAR), 
          ('New Mexico'::VARCHAR,'NM'::VARCHAR), ('New York Metro'::VARCHAR,'NYM'::VARCHAR), 
          ('Upstate New York'::VARCHAR,'NYU'::VARCHAR), ('North Carolina'::VARCHAR,'NC'::VARCHAR), 
          ('North Dakota'::VARCHAR,'ND'::VARCHAR), ('Ohio'::VARCHAR,'OH'::VARCHAR), 
          ('Oklahoma'::VARCHAR,'OK'::VARCHAR), ('Oregon'::VARCHAR,'OR'::VARCHAR), 
          ('Pan USA'::VARCHAR,'USA'::VARCHAR), ('Pennsylvania'::VARCHAR,'PA'::VARCHAR), 
          ('Rhode Island'::VARCHAR,'RI'::VARCHAR), ('South Carolina'::VARCHAR,'SC'::VARCHAR), 
          ('South Dakota'::VARCHAR,'SD'::VARCHAR), ('Tennessee'::VARCHAR,'TN'::VARCHAR), 
          ('Texas'::VARCHAR,'TX'::VARCHAR), ('Utah'::VARCHAR,'UT'::VARCHAR), 
          ('Vermont'::VARCHAR,'VT'::VARCHAR), ('Virginia'::VARCHAR,'VA'::VARCHAR), 
          ('Washington'::VARCHAR,'WA'::VARCHAR), ('West Virginia'::VARCHAR,'WV'::VARCHAR), 
          ('Wisconsin'::VARCHAR,'WI'::VARCHAR), ('Wyoming'::VARCHAR,'WY'::VARCHAR)
      ) t(market_name, state_code)
  )
SELECT
  CASE
      WHEN main.market LIKE '% - % - %' THEN (split_part(main.market, ' - ', 2) || ' - ') || split_part(main.market, ' - ', 3)
      ELSE split_part(main.market, ' - ', 2)
  END AS market_name,
  COALESCE(sm.state_code, 'UNKNOWN'::VARCHAR) AS market_code,
  main.market AS market_hyperion,
  main.market_coding,
  main.market_id,
  (SELECT ARRAY_AGG(DISTINCT OBJECT_CONSTRUCT('customer_actual_data', sub.customer_actual_data, 'customer_id', sub.customer_id, 'customer_coding', sub.customer_coding, 'customer_stat_level', sub.customer_stat_level, 'customer_stat_level_id', sub.customer_stat_level_id, 'customer_stat_level_coding', sub.customer_stat_level_coding, 'planning_member_id', sub.planning_member_id, 'planning_member_coding', sub.planning_member_coding))
      FROM {{ ref('stg_hyperion__customer_master') }} sub
    WHERE sub.market = main.market) AS customers
FROM {{ ref('stg_hyperion__customer_master') }} main
LEFT JOIN state_mapping sm ON
  CASE
      WHEN main.market LIKE '% - % - %' THEN (split_part(main.market, ' - ', 2) || ' - ') || split_part(main.market, ' - ', 3)
      ELSE split_part(main.market, ' - ', 2)
  END = sm.market_name
GROUP BY sm.market_name, sm.state_code, main.market, main.market_coding, main.market_id