CREATE OR REPLACE TABLE APP_RISK.APP_RISK.API_PAYMENTS AS
  (
     SELECT DISTINCT PT.UNIT_TOKEN
           , DU.BEST_AVAILABLE_MERCHANT_TOKEN
           , PAYMENT_TRX_RECOGNIZED_AT::DATE AS PAYMENT_TRX_RECOGNIZED_AT
           , DU.BUSINESS_CATEGORY
    , SUM(AMOUNT_BASE_UNIT/100) AS AMOUNT_BASE_UNIT
    FROM PAYMENTS_DW.PUBLIC.PAYMENT_TRANSACTIONS PT
    JOIN APP_BI.PENTAGON.DIM_USER DU
    ON DU.USER_TOKEN = PT.UNIT_TOKEN
    AND DU.IS_UNIT = 1
    WHERE HAS_SUCCESSFUL_CAPTURE = 1
    AND IS_GPV = 1
    AND IS_VOIDED = 0
    AND IS_REFUNDED = 0
    AND IS_CARD_PAYMENT = 1
    AND PAYMENT_TRX_RECOGNIZED_AT::DATE >= '2020-03-02'---DATEADD('DAYS', -92, CURRENT_DATE)
    AND PAY_WITH_SQUARE_ENTRY_METHOD IN ('EXTERNAL_API','EXTERNAL_API_ON_FILE')
    GROUP BY 1,2,3,4
);


CREATE OR REPLACE TABLE APP_RISK.APP_RISK.SQUARE_ONLINE_MERCHANTS as 
 (

SELECT TOKEN AS MERCHANT_TOKEN
                     , MIN(SQUARE_ONLINE_SIGNUP_AT ) AS SQUARE_ONLINE_SIGNUP_DATETIME
                      FROM RISKMANAGER.WD_MYSQL_RISKMANAGER_001__RISKMANAGER_PRODUCTION.MERCHANTS
                      WHERE INITIALLY_ENROLLED_AT IS NOT NULL
  
  group by 1

);

-----------attach rates for the paid risk manager (OF THE ELIGIBLE SELLER BASE HOW MANY ARE ENROLLED IN PAID PRODUCT)
CREATE OR REPLACE TABLE APP_RISK.APP_RISK.RM_PAID_ATTACH_RATE AS 
(
SELECT date_trunc(week, report_date) as week_start_date
        , API.BUSINESS_CATEGORY AS MCC
  
      , case when som.SQUARE_ONLINE_SIGNUP_DATETIME is not null then 1 else 0 end as is_square_online
  
        , IFNULL(dugs.merchant_sub_segment,'Inactive') AS gpv_segment
        ,COUNT(DISTINCT (CASE WHEN RM.MERCHANT_TOKEN IS NOT NULL AND 
                         ((RM.STATE IN ('in_trial', 'subscribed') and ONBOARD_DATE  <= WEEK_END_DATE_MONDAY_START)
                         OR (RM.STATE IN ('trial_cancelled','churned') AND OFFBOARD_DATE  BETWEEN DATEADD('days', -84, WEEK_BEGIN_DATE_MONDAY_START) AND WEEK_END_DATE_MONDAY_START))
                       THEN RM.MERCHANT_TOKEN ELSE NULL END)) AS RM_ENROLLED
       , COUNT(DISTINCT API.BEST_AVAILABLE_MERCHANT_TOKEN) AS TOTAL_MERCHANTS 
       , RM_ENROLLED/TOTAL_MERCHANTS AS PAID_PRODUCT_ADOPTION_RATE
       , RM_ENROLLED*10000/TOTAL_MERCHANTS AS PAID_PRODUCT_ADOPTION_RATE_IN_BPS
FROM app_bi.app_bi_dw.dim_date DD

  LEFT JOIN APP_RISK.APP_RISK.API_PAYMENTS API
ON PAYMENT_TRX_RECOGNIZED_AT ::DATE BETWEEN  DATEADD('days', -92, report_date) AND report_date

  LEFT JOIN app_bi.app_bi_dw.dim_user_gpv_segment dugs
ON ((API.unit_token = dugs.user_token) AND (report_date::DATE BETWEEN dugs.effective_begin AND dugs.effective_end))

  LEFT JOIN APP_RISK.APP_RISK.RM_ENROLLED_MERCHANTS RM
ON RM.MERCHANT_TOKEN = API.BEST_AVAILABLE_MERCHANT_TOKEN
  
  left join app_risk.app_risk.square_online_merchants as som 
on som.merchant_token = api.BEST_AVAILABLE_MERCHANT_TOKEN
  
WHERE DD.WEEK_BEGIN_DATE_MONDAY_START BETWEEN    DATEADD('days', 84, '2020-03-02') AND DATE_TRUNC('WEEK', CURRENT_DATE)
GROUP BY 1,2,3,4
);

----risk evaluation attach rates ((OF THE SELLERS WHO  HAD MODERATE TO HIGH RISK PAYMENT IN TRAILING 90 DAYS, % OF SELLERS WHO INTERACTED WITH MODERATE/HIGH RISK PAYMENTS IN TRANSACTIONS BLADE))
CREATE OR REPLACE TABLE APP_RISK.APP_RISK.RM_RISKEVAL_ATTACH_RATE AS
(WITH RISKY_PAYMENTS AS
(SELECT DISTINCT DU.BEST_AVAILABLE_MERCHANT_TOKEN AS MERCHANT_TOKEN
 , DU.USER_TOKEN
 , BILL_TOKEN
,  CREATED_AT::DATE AS RISKY_PAYMENT_DATE
FROM riskarbiter.raw_oltp.payment_risk_evaluation pre
 LEFT JOIN APP_BI.PENTAGON.DIM_USER DU
 ON DU.USER_TOKEN = PRE.UNIT_TOKEN
WHERE risk_level != 'NORMAL'
),
transactions_active as
(SELECT DISTINCT SPLIT_PART(webpage_path,'/',5) AS dashboard_bill_token
, subject_merchant_token AS merchant_token
, min(pv.u_recorded_at::date) AS earliest_engagement
FROM eventstream2.catalogs.page_view AS pv
WHERE pv.u_app_name = 'dashboard'
    AND pv.u_recorded_at >= '2020-03-01'
    AND pv.u_catalog_name = 'page_view'
    AND pv.page_view_description ilike 'Transactions: Show%'
    AND dashboard_bill_token IS NOT NULL
GROUP BY 1,2
) 
SELECT date_trunc(WEEK, dd.report_date) AS WEEK_START_DATE
       ,IFNULL(dugs.merchant_sub_segment,'Inactive') AS gpv_segment
       , case when som.SQUARE_ONLINE_SIGNUP_DATETIME is not null then 1 else 0 end as is_square_online
  
       ,API.BUSINESS_CATEGORY AS MCC
       ,count(distinct API.BEST_AVAILABLE_MERCHANT_TOKEN) as total
       ,count(distinct case when earliest_engagement IS NOT NULL then API.BEST_AVAILABLE_MERCHANT_TOKEN else null end) as ACTIVE
       ,ACTIVE/total AS RISK_EVALUATION_ATTACH_RATES  
FROM  app_bi.app_bi_dw.dim_date dd
LEFT JOIN APP_RISK.APP_RISK.api_payments API
 ON API.PAYMENT_TRX_RECOGNIZED_AT between dateadd(day, -92, dd.report_date) and dd.report_date  
LEFT JOIN  risky_payments base2 
ON base2.RISKY_PAYMENT_DATE = API.PAYMENT_TRX_RECOGNIZED_AT
AND BASE2.USER_TOKEN = API.UNIT_TOKEN
LEFT JOIN app_risk.app_risk.rm_enrolled_merchants rem
    ON api.BEST_AVAILABLE_MERCHANT_TOKEN = rem.merchant_token
LEFT JOIN TRANSACTIONS_ACTIVE TA
ON TA.dashboard_bill_token = BASE2.BILL_TOKEN
 LEFT JOIN app_bi.app_bi_dw.dim_user_gpv_segment dugs
ON ((API.UNIT_TOKEN = dugs.user_token) AND (report_date::DATE BETWEEN dugs.effective_begin AND dugs.effective_end))
 
left join app_risk.app_risk.square_online_merchants as som 
on som.merchant_token = api.BEST_AVAILABLE_MERCHANT_TOKEN
 
WHERE rem.merchant_token IS NULL
and report_date between '2020-05-01' and current_date
and PAYMENT_TRX_RECOGNIZED_AT >= '2020-03-01'
group by 1, 2, 3, 4
HAVING TOTAL>0
);


DROP TABLE APP_RISK.APP_RISK.API_PAYMENTS;
DROP TABLE APP_RISK.APP_RISK.SQUARE_ONLINE_MERCHANTS;

/*
CREATE OR REPLACE APP_RISK.APP_RISK.RM_PAID_ATTACH_RATE AS
SELECT * FROM APP_RISK.APP_RISK.RM_PAID_ATTACH_RATE_LOADING;


CREATE OR REPLACE APP_RISK.APP_RISK.RM_RISKEVAL_ATTACH_RATE AS
SELECT * FROM APP_RISK.APP_RISK.RM_RISKEVAL_ATTACH_RATE_LOADING;


DROP TABLE APP_RISK.APP_RISK.RM_RISKEVAL_ATTACH_RATE_LOADING;
DROP TABLE APP_RISK.APP_RISK.RM_PAID_ATTACH_RATE_LOADING;

*/
