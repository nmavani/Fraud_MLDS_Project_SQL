---this SQL will create table for all the risk manager seller's payments 

create or replace table app_risk.app_risk.risk_manager_payments_all as
select distinct
pre.payment_token
, payment_trx_recognized_at
, auth_intent_created_at
, pre.unit_token
, dw.country_code
, rm.merchant_token
, du.business_name
, du.business_category
, dw.PAN_FIDELIUS_TOKEN
, dw.currency_code
, amount_base_unit/100 as amount_base
, AUTH_INTENT_AMOUNT/100 as auth_amount
, TIP_AMOUNT_BASE_UNIT/100 as tip_amount
, DECLINE_REASONS
, dw.is_refunded
, dw.pan_masked
, is_gpv
, BIN_PROPERTIES_COUNTRY
, BIN_PROPERTIES_PREPAID_STATUS
, dw.CARD_BRAND
, TIP_AMOUNT_BASE_UNIT
, PAY_WITH_SQUARE_ENTRY_METHOD
, AUTH_SUCCESS
, AUTH_AVS_STATUS
, AUTH_CVV_STATUS
, onboard_date
, rm.state
, rm.offboard_date
, cb.type as chargeback_type
, cb.status
, chargeback_cents/100 as chargeback_amt
, loss_cents/100 as loss_amt
, max(case when risk_level = 'HIGH' then 'HIGH'
           when risk_level = 'MODERTATE' THEN 'MODERATE'
           else risk_level end) as risk_level
, max(case when ra.payment_token is not null then 1 else 0 end) as alerted
, max(case when cb.payment_token is not null then 1 else 0 end) as chargeback
, max(case when type = 'fraud' then 1 else 0 end) as fraud_chargeback
, max(case when ap.payment_token is not null then 1 else 0 end) as allowed
from riskarbiter.raw_oltp.payment_risk_evaluation  pre
join payments_dw.public.payment_transactions dw
on dw.payment_token = pre.payment_token
join app_bi.pentagon.dim_user du
on du.user_token = pre.unit_token
join app_risk.app_risk.rm_enrolled_merchants rm
on rm.merchant_token = du.best_available_merchant_token
left join app_risk.app_risk.chargebacks cb
on cb.payment_token = pre.payment_token
left join NIXLIST.RAW_OLTP.ALLOWED_PAYMENTS ap 
on ap.payment_token = pre.payment_token
left join RISKALERTS.RAW_OLTP.RISK_ALERTS  ra
on ra.payment_token = pre.payment_token
where is_card_payment = 1
and auth_success = True
and pre.created_at >= onboard_date
and auth_intent_created_at >= '2020-03-31'
and is_voided = 0
and rm.merchant_token  is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32;


drop table if exists app_risk.app_risk.risk_manager_payments;

create or replace table app_risk.app_risk.risk_manager_payments as
select * from app_risk.app_risk.risk_manager_payments_all ;


drop table app_risk.app_risk.risk_manager_payments_all;
