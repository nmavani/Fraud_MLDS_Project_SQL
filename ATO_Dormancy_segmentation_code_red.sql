------This analysis is in response to defining ATO dormancy strategy for SQ 
-------
use personal_nmavani;
------- get all ATO
create or replace temp table ato_sellers as
select *
from APP_RISK.APP_RISK.ATO_MERCHANT_SUMMARY am
where  earliest_confirmed_ato_hashtag_at <= '2022-08-01';


------- get all sketchylogin
create or replace temp table sketchylogin as
select  ral.target_token,
ral.created_at,
ral.comment,
rm.unit_token as fp_unit_token, 
SUBSTRING(comment,POSITION('IP', ral.comment) + 3) AS IP ,
split_part(SUBSTRING(comment,POSITION('IP', ral.comment) + 3), ' #', 1) as ip_cleaned, 
rm.ip as rm_ip
FROM REGULATOR.RAW_OLTP.AUDIT_LOGS AS ral
left join fivetran.app_risk.retroactive_sketchylogin_update_july_2022 rm ---remove kasada false positives
on ral.target_token = rm.unit_token
--and rm.ip = split_part(SUBSTRING(comment,POSITION('IP', ral.comment) + 3), ' #', 1)
and ral.created_at::date between '2022-07-11' and '2022-07-29'
WHERE (ral.comment ILIKE '%#sketchylogin%' OR  ral.action_name ='Ato::Watchlist')
AND ral.created_at::date <= '2022-08-01'


-------- Recall for sketchylogin

select date_trunc(month, earliest_confirmed_ato_hashtag_at) as ato_month,
count(distinct ato.user_token) as Confirmed_ato, 
count(distinct sk.target_token) as sketchylogin_sellers
from ato_sellers ato
left join sketchylogin sk
on sk.target_token = ato.user_token
and created_at::date between dateadd(days, -30, earliest_confirmed_ato_hashtag_at) and dateadd(days, 30, earliest_confirmed_ato_hashtag_at)
and fp_unit_token is null
group by 1
order by 1 desc


select date_trunc(year, earliest_confirmed_ato_hashtag_at) as ato_month,
count(distinct ato.user_token) as Confirmed_ato, 
count(distinct sk.target_token) as sketchylogin_sellers
from ato_sellers ato
left join sketchylogin sk
on sk.target_token = ato.user_token
and created_at::date between dateadd(days, -30, earliest_confirmed_ato_hashtag_at) and dateadd(days, 30, earliest_confirmed_ato_hashtag_at)
and fp_unit_token is null
group by 1
order by 1 desc

-------- Preicison for sketchylogin


select date_trunc(month, created_at) as sketchylogin_month,
count(distinct sk.target_token) as sketchylogin_ato,
count(distinct ato.user_token) as Confirmed_ato 
from sketchylogin sk
left join ato_sellers ato
on sk.target_token = ato.user_token
and earliest_confirmed_ato_hashtag_at::date between dateadd(days, -30, created_at) and dateadd(days, 30, created_at)
where fp_unit_token is   null
and created_at < '2022-07-01'
group by 1


select date_trunc(year, created_at) as sketchylogin_month,
count(distinct sk.target_token) as sketchylogin_ato,
count(distinct ato.user_token) as Confirmed_ato 
from sketchylogin sk
left join ato_sellers ato
on sk.target_token = ato.user_token
and earliest_confirmed_ato_hashtag_at::date between dateadd(days, -30, created_at) and dateadd(days, 30, created_at)
where fp_unit_token is   null
and created_at < '2022-07-01'
group by 1



-----merchant locked
create or replace temp table merchant_locked as
select merchant_id, 
count(distinct c.person_token) as total_person_locked
, count(distinct e.person_id) as total_person
, case when total_person_locked = total_person then 1 else 0 end as is_locked 
from roster.merchants.employees e
left join multipass.raw_oltp.credentials c
on c.PERSON_TOKEN = e.person_id
and LOCKED_AT  is not null
group by 1

---% of merchants have single person operating vs multiple

create or replace temp table merchants_persons as 
select e.MERCHANT_ID , is_locked
, count(distinct person_id) as total_persons
, count(distinct case when is_active then person_id else null end ) as active_persons
, count(distinct case when IS_AUTHORIZED_USER = TRUE then person_id else null end ) as authorized_persons
from roster.merchants.employees e
left join merchant_locked ml
on ml.MERCHANT_ID= e.MERCHANT_ID
group by 1,2

//select * from merchants_persons
//where total_persons = 0

select case when total_persons = 1 then '1. 1'
            when total_persons < 5 then '2. <5'
            when total_persons < 10  then '3. <10'
            when total_persons < 20  then '4. <20'
            when total_persons < 50  then  '5. <50' 
            else '6. 50+' end as total_person_buckets,
count(distinct merchant_id),
count(distinct case when is_locked=1 then merchant_id else null end) as locked_merchants
from merchants_persons
group by 1


------------------------------------------------------Money Movement Dormancy
use personal_nmavani;

-----get days seller had card payment and their last card payment date
create or replace temp table seller_dormancy as
select unit_token, PAYMENT_TRX_RECOGNIZED_DATE, CARD_PAYMENT_COUNT, 
lag(PAYMENT_TRX_RECOGNIZED_DATE,1, NULL)  over (partition by unit_token order by PAYMENT_TRX_RECOGNIZED_DATE) as previous_payment_date
, datediff(days, previous_payment_date, PAYMENT_TRX_RECOGNIZED_DATE) as difference
from app_bi.pentagon.aggregate_seller_daily_payment_summary 
where CARD_PAYMENT_COUNT > 0;

-----get more info for the seller
create or replace temp table seller_dormancy_detailed as
select sumry.first_card_payment_date
,  sd.*
, du.is_currently_frozen 
, du.is_currently_deactivated
, sumry.latest_card_payment_date
, du.user_created_at_date
, sumry.lifetime_gpv_amount_base_unit_usd/100 as lifetime_gpv
, du.currently_frozen_at_date
, du.best_available_merchant_token
, du.currently_deactivated_at_date
, du.user_token
, du.is_deleted
from seller_dormancy sd
left join app_bi.pentagon.aggregate_seller_lifetime_summary sumry
on sd.unit_token = sumry.user_token
right join app_bi.pentagon.dim_user du
on du.user_token = sd.unit_token
where du.is_unit = 1;

create or replace table personal_nmavani.public.payment_act_history as
(select * from seller_dormancy_detailed
)

-----get latest dormancy status for the seller
create or replace temp table dormancy_final as 
select user_token
, best_available_merchant_token
, first_card_payment_date
, latest_card_payment_date
, dd.is_currently_frozen
, dd.is_currently_deactivated
, dd.is_deleted
, fa.unit_token as fa_user
, datediff(days , latest_card_payment_date, current_date ) as dormancy_from_today
, max(difference) as maximum_dormancy_in_between
, case when first_card_payment_date is null then NULL
       when MAXIMUM_DORMANCY_IN_BETWEEN is null then dormancy_from_today
       when dormancy_from_today > maximum_dormancy_in_between then dormancy_from_today
       else maximum_dormancy_in_between end as dormant_final
from seller_dormancy_detailed dd
left join app_risk.app_risk.fake_account_units fa
on fa.unit_token = dd.user_token
group by 1,2,3,4,5,6,7,8,9;

create or replace table personal_nmavani.public.payment_latest as
select * from dormancy_final


---------Non payment money movement
create or replace temp table BB_EXITS as (
  select distinct  CREATED_AT::date as  latest_dt
      , substr(balance_token,19,13) as unit_token
  FROM  BIZBANK.RAW_OLTP.UNIFIED_ACTIVITIES
  WHERE 
    NOT ( ACTIVITY_TYPE  ilike any ('%CHARGEBACK%', '%CANCELLATION%', 'UNKNOWN'))
    AND ACTIVITY_STATE='SETTLED'
    and CREATED_AT < '2022-09-13'
);

create or replace temp table NON_BB_EXITS as (
  select distinct DATEADD(HOUR,-5,TO_TIMESTAMP(createdat_instantusec/1e6)) as latest_dt
  , merchant_token as unit_token
  FROM ledger.raw_feeds.bank_settlement_entry
  where DATEADD(HOUR,-5,TO_TIMESTAMP(createdat_instantusec/1e6)) < '2022-09-13'
  and verificationdeposit = 'FALSE'
);


---------Non payment money movement

create or replace table personal_nmavani.public.non_payment_act_history as
(select * , 'bb_exit' as type
 from BB_EXITS
 union all
 select * , 'ledger' as type
 from NON_BB_EXITS
)

create or replace temp table personal_nmavani.public.non_payment_money_movement_latest as
select unit_token, max(latest_dt) as latest_dt
from 
(select * from BB_EXITS
 union all
 select * from NON_BB_EXITS
)
group by 1


-----------combined 
create or replace table personal_nmavani.public.money_movement_dormancy as
select * ,  
greatest(coalesce(LATEST_CARD_PAYMENT_DATE, latest_dt), coalesce(latest_dt, LATEST_CARD_PAYMENT_DATE)) as last_money_movement_date
from personal_nmavani.public.payment_latest df
left join personal_nmavani.public.non_payment_money_movement_latest mm
on df.user_token = mm.unit_token


------------------------------------------------------Activity Dormancy
--------get login history
create or replace TEMP TABLE MERCHANT_LOGIN_ALL_EVENTS AS
SELECT DISTINCT subject_merchant_token , SUBJECT_PERSON_TOKEN, u_recorded_at::date as record_date
FROM eventstream2.catalogs.multipass_event
WHERE subject_merchant_token IS NOT NULL
AND MULTIPASS_EVENT_SUCCESS = TRUE

--------get es2 latest activity dates for the merchant (page clicks and mobile clicks are good indicator)
create or replace TEMP TABLE MERCHANT_ACTIVITY_EVENTS AS
SELECT DISTINCT subject_merchant_token, SUBJECT_PERSON_TOKEN, MAX(MAX_ACTIVITY) AS MAX_ACTIVITY_DATE FROM
(SELECT DISTINCT subject_merchant_token , SUBJECT_PERSON_TOKEN, MAX( u_recorded_at) AS MAX_ACTIVITY
FROM eventstream2.catalogs.page_click
WHERE  subject_merchant_token IS NOT NULL
GROUP BY 1,2
UNION ALL
SELECT DISTINCT subject_merchant_token , SUBJECT_PERSON_TOKEN, MAX( u_recorded_at) AS MAX_ACTIVITY
FROM eventstream2.catalogs.mobile_click
WHERE  subject_merchant_token IS NOT NULL
GROUP BY 1,2
)
GROUP BY 1,2

-----combine mobile/page and login
CREATE OR REPLACE TABLE PERSONAL_NMAVANI.PUBLIC.MERCHANT_LAST_ACTIVITY_merchant AS
SELECT SUBJECT_MERCHANT_TOKEN, MAX( DATE ) AS LAST_ACTIVITY_DATE FROM
(SELECT SUBJECT_MERCHANT_TOKEN , MAX_ACTIVITY_DATE AS DATE
 FROM MERCHANT_ACTIVITY_EVENTS
 UNION ALL
 SELECT SUBJECT_MERCHANT_TOKEN , MAX(record_date) AS DATE
 FROM MERCHANT_LOGIN_ALL_EVENTS
 GROUP BY 1
)
 GROUP BY 1;


-----combine mobile/page and login with es1 
create or replace TABLE personal_nmavani.public.merchant_last_activity_including_es1 as
SELECT SUBJECT_MERCHANT_TOKEN, MAX( DATE ) AS LAST_ACTIVITY_DATE FROM
(SELECT SUBJECT_MERCHANTTOKEN AS SUBJECT_MERCHANT_TOKEN , MAX(LAST_ACTIVITY_DATE) AS DATE
 FROM app_risk.app_risk_test.es1_merchants    ----this table was spun up #snowflaake team https://square.slack.com/archives/CDP29PU2J/p1663091821460029
 GROUP BY 1
 UNION ALL
 SELECT SUBJECT_MERCHANT_TOKEN , MAX(LAST_ACTIVITY_DATE) AS DATE
 FROM PERSONAL_NMAVANI.PUBLIC.MERCHANT_LAST_ACTIVITY_merchant
 GROUP BY 1
 )
 GROUP BY 1;
 
 


------------------------------------------------------COMBINING ALL

create or replace temp table personal_nmavani.public.seller_money_movement_login_dormancy_MERCHANT as
select * , 
case when dormancy_from_today is null then NULL
            when dormancy_from_today < 180 then '1. <180'
            when dormancy_from_today < 365 then '2. <1 year'
            when dormancy_from_today < 545 then '3. <1.5 year'
            when dormancy_from_today < 730 then '4. <2 year'
            when dormancy_from_today < 1460 then '5. <4 year'
            else '6. 4+ year' end payment_dormancy_category,
datediff(days, LAST_ACTIVITY_DATE, current_date) as last_act_dormancy,
case when last_act_dormancy is null then NULL
            when last_act_dormancy < 180 then '1. <180'
            when last_act_dormancy < 365 then '2. <1 year'
            when last_act_dormancy < 545 then '3. <1.5 year'
            when last_act_dormancy < 730 then '4. <2 year'
            when last_act_dormancy < 1460 then '5. <4 year'
            else '6. 4+ year' end last_act_dormancy_category,
datediff(days, last_money_movement_date, current_date) as last_money_movement_dormancy,
case when last_money_movement_dormancy is null then NULL
            when last_money_movement_dormancy < 180 then '1. <180'
            when last_money_movement_dormancy < 365 then '2. <1 year'
            when last_money_movement_dormancy < 545 then '3. <1.5 year'
            when last_money_movement_dormancy < 730 then '4. <2 year'
            when last_money_movement_dormancy < 1460 then '5. <4 year'
            else '6. 4+ year' end last_money_movement_dormancy_category
, case when sk.created_at > last_money_movement_date then 1 else 0 end as sketchylogin_post_latest_money_movement
, case when sk.created_at is not null then 1 else 0 end as sketchylogin
, case when ato.EARLIEST_CONFIRMED_ATO_HASHTAG_AT > last_money_movement_date then 1 else 0 end as ato_post_latest_money_movement
, case when ato.EARLIEST_CONFIRMED_ATO_HASHTAG_AT is not null then 1 else 0 end as ato
, case when LAST_ACTIVITY_DATE is not null then 1 else 0 end as activity
, case when LAST_ACTIVITY_DATE > last_money_movement_date and datediff(days,last_money_movement_date, LAST_ACTIVITY_DATE)> 90 then 1 else 0 end as activity_post_90_day_from_money_movement
, case when LAST_ACTIVITY_DATE > last_money_movement_date and datediff(days,last_money_movement_date, LAST_ACTIVITY_DATE)> 540 then 1 else 0 end as activity_post_540_day_from_money_movement
, case when LAST_ACTIVITY_DATE > last_money_movement_date then 1 else 0 end as activity_post_payment
, case when LAST_ACTIVITY_DATE is null then 1 else 0 end as no_activity
from  personal_nmavani.public.money_movement_dormancy df
left join PERSONAL_NMAVANI.PUBLIC.merchant_last_activity_including_es1 me
on me.SUBJECT_merchant_TOKEN = df.best_available_merchant_token
left join merchant_locked ml
on ml.merchant_id = df.best_available_merchant_token
left join (SELECT sk.*, du.best_available_merchant_token as mt FROM sketchylogin  SK 
           LEFT JOIN APP_BI.PENTAGON.DIM_USER DU 
           ON DU.USER_TOKEN = SK.target_token
           where fp_unit_token is null) SK
on sk.mt = df.best_available_merchant_token
left join (SELECT sk.user_token as ato_user, du.best_available_merchant_token as mtoken, EARLIEST_CONFIRMED_ATO_HASHTAG_AT
           FROM ato_sellers  SK 
           LEFT JOIN APP_BI.PENTAGON.DIM_USER DU 
           ON DU.USER_TOKEN = SK.user_token) ato
on ato.mtoken = df.best_available_merchant_token
where (df.is_deleted is null or df.is_deleted != 1)
and  ( df.is_currently_deactivated <= 0 or df.is_currently_deactivated is null )
and  ( df.is_currently_frozen <= 0 or df.is_currently_frozen is null )
--and last_money_movement_date is not null
;


-------------------money_movement_dormancy and other risk states
select last_money_movement_dormancy_category 
, count(distinct user_token) all_users 
, count(distinct case when is_locked = 1 then user_token else null end ) as locked_users
, count(distinct case when activity_post_90_day_from_money_movement = 1 then user_token else null end ) as activity_post_90_day_from_money_movement_users
, count(distinct case when activity_post_payment = 1 then user_token else null end ) as activity_post_money_movement_users
, count(distinct case when sketchylogin_post_latest_money_movement = 1 then user_token else null end ) as sketchy_post_money_movement_users
, count(distinct case when no_activity = 1 then user_token else null end ) as no_activity_post_money_movement_users
, count(distinct case when activity_post_540_day_from_money_movement = 1 then user_token else null end ) as activity_post_540_day_from_money_movement_users
, count(distinct case when ato_post_latest_money_movement = 1 then user_token else null end ) as ato_post_money_movement_users
, count(distinct case when ato = 1 then user_token else null end ) as ato_users
from personal_nmavani.public.seller_money_movement_login_dormancy_MERCHANT dm
where last_money_movement_date is not null
group by 1
order by 1 


-------------------money_movement_dormancy and other risk states for sellers who never moved money
select last_money_movement_dormancy_category 
, count(distinct user_token) all_users 
, count(distinct case when is_locked = 1 then user_token else null end ) as locked_users
, count(distinct case when activity = 1 then user_token else null end ) as activity_users
, count(distinct case when sketchylogin = 1 then user_token else null end ) as sketchy_login_users
, count(distinct case when no_activity = 1 then user_token else null end ) as no_activity_post_money_movement_users
, count(distinct case when ato = 1 then user_token else null end ) as ato_users
from personal_nmavani.public.seller_money_movement_login_dormancy_MERCHANT dm
where last_money_movement_date is  null
group by 1

-------------------money_movement_dormancy distribution
select last_money_movement_dormancy_category 
, count(distinct user_token) all_users 
, count(distinct case when is_locked = 0 then user_token else null end ) as non_locked_users
from personal_nmavani.public.seller_money_movement_login_dormancy_MERCHANT dm
group by 1


select count(distinct user_token) from app_bi.pentagon.dim_user df
where not ((df.is_deleted is null or df.is_deleted != 1)
and  ( df.is_currently_deactivated <= 0 or df.is_currently_deactivated is null )
and  ( df.is_currently_frozen <= 0 or df.is_currently_frozen is null ))
and is_unit = 1


------------money movenet x activity dormancy
select last_money_movement_dormancy_category , last_act_dormancy_category, count(distinct user_token) all_users 
, count(distinct case when is_locked = 0 then user_token else null end ) as non_locked_users
from personal_nmavani.public.seller_money_movement_login_dormancy_MERCHANT dm
group by 1,2



---------GPV enabled from payment dormant cohort

with chargebacks_usd as
( select cb.PAYMENT_CREATED_AT::date as payment_dt,
 cb.user_token, 
 sum(NVL(CB.CHARGEBACK_CENTS * Cer.exchange_rate_fxd * cer.base_unit_to_unit_multiplier,0)) as chargebacks_usd
from app_risk.app_risk.chargebacks cb 
left join (select unit_token , PAYMENT_TRX_RECOGNIZED_DATE, PREVIOUS_PAYMENT_DATE from  personal_nmavani.public.payment_act_history ah1
           where ah1.PAYMENT_TRX_RECOGNIZED_DATE  <= '2022-03-01'
           and datediff(days,  PREVIOUS_PAYMENT_DATE, ah1.PAYMENT_TRX_RECOGNIZED_DATE) > 540
          and PREVIOUS_PAYMENT_DATE is not null) ah2
on cb.user_token = ah2.unit_token
LEFT JOIN  APP_BI.APP_BI_DW.DIM_EXCHANGE_RATE cer
    ON cer.report_date = TO_DATE(cb.chargeback_date)
    AND cer.currency_code_base = cb.currency_code
    AND cer.currency_code_counter = 'USD'
group by 1,2
)
select date_trunc(month, ah1.PAYMENT_TRX_RECOGNIZED_DATE) as payment_month,
count(distinct ah1.unit_token),
sum(case when ah2.PAYMENT_TRX_RECOGNIZED_DATE< dateadd(days, 91, ah1.PAYMENT_TRX_RECOGNIZED_DATE) then ah2.GPV_PAYMENT_AMOUNT_BASE_UNIT_USD/100 else null end) as GPV_enabled_90,
sum(case when ah2.PAYMENT_TRX_RECOGNIZED_DATE< dateadd(days, 366, ah1.PAYMENT_TRX_RECOGNIZED_DATE) then ah2.GPV_PAYMENT_AMOUNT_BASE_UNIT_USD/100 else null end) as GPV_enabled_365,
sum(ah2.GPV_PAYMENT_AMOUNT_BASE_UNIT_USD/100) as overall_GPV_enabled, 
sum(case when cb.payment_dt < dateadd(days, 366, ah1.PAYMENT_TRX_RECOGNIZED_DATE) then cb.chargebacks_usd else null end) as cb_enabled_365,
sum(chargebacks_usd) as total_cb
from  (select unit_token , PAYMENT_TRX_RECOGNIZED_DATE, PREVIOUS_PAYMENT_DATE from  personal_nmavani.public.payment_act_history ah1
           where ah1.PAYMENT_TRX_RECOGNIZED_DATE  <= '2022-03-01'
           and datediff(days,  PREVIOUS_PAYMENT_DATE, ah1.PAYMENT_TRX_RECOGNIZED_DATE) > 540
          and PREVIOUS_PAYMENT_DATE is not null) ah1
left join  app_bi.pentagon.aggregate_seller_daily_payment_summary  ah2
on ah1.unit_token = ah2.unit_token
and ah2.PAYMENT_TRX_RECOGNIZED_DATE > ah1.PAYMENT_TRX_RECOGNIZED_DATE
left join chargebacks_usd cb
on cb.user_token = ah2.unit_token
and cb.payment_dt = ah2.PAYMENT_TRX_RECOGNIZED_DATE
group by 1





