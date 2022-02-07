----okr 1 - Loss bps

with valid_fraud_disputes as (
select *
from bizbank.raw_oltp.transaction_disputes dsp
left join  (select transaction_token, currency_code, max(abs(amount_cents)/100) as amount, min(created_at) as transaction_created_at
           from bizbank.raw_oltp.unified_activities ua
           where activity_type ilike '%card_payment%'
           group by 1,2 ) ua
on ua.transaction_token = dsp.transaction_token
where reason_code = 'FRAUD_OR_UNAUTHORIZED'
AND year(transaction_date) = 2021
AND (
  (final_resolution in ('APPROVED','DENIED','CREDITED_FROM_SQUARE','REJECTED_BY_MARQETA'))--'VOIDED',--'REJECTED_BY_SQUARE',--'REFUNDED',
  OR
  ((final_resolution is null) and status in ('CLOSED','FILED','INITIATED','LOST','NETWORK_REJECTED','PREARBITRATION','REPRESENTMENT','WON')) --'WAITING_ON_CARD_HOLDER',--'NEW'
)), 
sq_card_transactions as (
select transaction_token
 , split_part(balance_token, '-c-', 2) as unit_token
 , currency_code
 , max(abs(amount_cents)/100) as amount
 , min(created_at) as created_at
 from bizbank.raw_oltp.unified_activities ua
 where activity_type ilike '%card_payment%'
 and ACTIVITY_STATE  in ('SETTLED' )
 group by 1,2,3
 having year(min(created_at)) = 2021)

select spn.transaction_month, dispute_amount, spend, dispute_amount*10000/spend as sq_loss_bps
from (select date_trunc(month, dsp.transaction_date) as transaction_month, 
      sum(case when (dsp.final_resolution ilike 'CREDITED_FROM_SQUARE' OR (dsp.status ilike 'lost' and dsp.PROVISIONAL_CREDIT_DEBITED_DATE is null)) then dsp.amount else null end) as dispute_amount
      from valid_fraud_disputes dsp
      group by 1) dsp
join  (select date_trunc(month, spn.created_at) as transaction_month,
       sum(amount) as spend
       from sq_card_transactions spn
       group by 1) spn
on dsp.transaction_month = spn.transaction_month

--------okr 3 (loss by seller from chipfallback and chip and pin transactions)
 ---------final
  with valid_fraud_disputes as (
select date_trunc('month',transaction_date) as transaction_month,
sum(dsp.dispute_amount) / 100.0 as valid_fraud_dispute_amount
from bizbank.raw_oltp.transaction_disputes dsp
left join  (select transaction_token, currency_code, max(abs(amount_cents)/100) as amount, min(created_at) as transaction_created_at
from bizbank.raw_oltp.unified_activities ua
where activity_type ilike '%card_payment%'
group by 1,2 ) ua
on ua.transaction_token = dsp.transaction_token
where reason_code = 'FRAUD_OR_UNAUTHORIZED'
AND year(transaction_date) = 2021
AND (
(final_resolution in ('APPROVED','DENIED','CREDITED_FROM_SQUARE','REJECTED_BY_MARQETA'))--'VOIDED',--'REJECTED_BY_SQUARE',--'REFUNDED',
OR
((final_resolution is null) and status in ('CLOSED','FILED','INITIATED','LOST','NETWORK_REJECTED','PREARBITRATION','REPRESENTMENT','WON')) --'WAITING_ON_CARD_HOLDER',--'NEW'
)
group by transaction_month
having year(transaction_month) = 2021
),

 fraud_disputes as (
select date_trunc('month',transaction_date) as transaction_month,
sum(dsp.dispute_amount) / 100.0 as fraud_dispute_amount
from bizbank.raw_oltp.transaction_disputes dsp
left join  (select transaction_token, currency_code, max(abs(amount_cents)/100) as amount, min(created_at) as transaction_created_at
from bizbank.raw_oltp.unified_activities ua
where activity_type ilike '%card_payment%'
group by 1,2 ) ua
on ua.transaction_token = dsp.transaction_token
where reason_code = 'FRAUD_OR_UNAUTHORIZED'
AND year(transaction_date) = 2021

group by transaction_month
having year(transaction_month) = 2021
),

seller_losses_fraud_disputes as (
select date_trunc('month',transaction_date) as transaction_month,
sum(dsp.dispute_amount) / 100.0 as seller_losses_amount
from bizbank.raw_oltp.transaction_disputes dsp
left join  (select transaction_token, currency_code, max(abs(amount_cents)/100) as amount, min(created_at) as transaction_created_at
from bizbank.raw_oltp.unified_activities ua
where activity_type ilike '%card_payment%'
group by 1,2 ) ua
on ua.transaction_token = dsp.transaction_token
where reason_code = 'FRAUD_OR_UNAUTHORIZED'
AND year(transaction_date) = 2021
AND final_resolution = 'REJECTED_BY_SQUARE'
AND (
(resolution_reason in ('PAYMENT_WITH_CHIP_AND_PIN', 'CHIP_FALLBACK'))
OR
( timediff('day', dsp.transaction_date, dsp.seller_disputed_date) > 90))

group by transaction_month
having year(transaction_month) = 2021
)

select vfd.*,
fd.fraud_dispute_amount,
slfd.seller_losses_amount
from valid_fraud_disputes vfd
left join fraud_disputes fd
on vfd.transaction_month = fd.transaction_month
left join seller_losses_fraud_disputes slfd
on vfd.transaction_month = slfd.transaction_month

order by vfd.transaction_month asc


------OKR 4 (fraud dispute rates by count)

with valid_disputes as (
select *
from bizbank.raw_oltp.transaction_disputes
where reason_code = 'FRAUD_OR_UNAUTHORIZED'
AND year(transaction_date) = 2021
AND (
(final_resolution in ('APPROVED','DENIED','CREDITED_FROM_SQUARE','REJECTED_BY_MARQETA'))--'VOIDED',--'REJECTED_BY_SQUARE',--'REFUNDED',
OR
((final_resolution is null) and status in ('CLOSED','FILED','INITIATED','LOST','NETWORK_REJECTED','PREARBITRATION','REPRESENTMENT','WON')) --'WAITING_ON_CARD_HOLDER',--'NEW'
)),

sq_card_transactions as (
select transaction_token
, split_part(balance_token, '-c-', 2) as unit_token
, currency_code
, max(abs(amount_cents)/100) as amount
, min(created_at) as ua_transaction_time
from bizbank.raw_oltp.unified_activities ua
where activity_type ilike '%card_payment%'
and ACTIVITY_STATE  in ('SETTLED' )
group by 1,2,3
having year(min(created_at)) = 2021)

select date_trunc('month',ifnull(sqt.ua_transaction_time, vd.transaction_date)) as transaction_month,
count(distinct sqt.transaction_token) as count_sq_card_transactions,
count(distinct vd.transaction_token) as count_sq_card_disputes,
10000 * count_sq_card_disputes / count_sq_card_transactions as bps_dispute_count
from valid_disputes vd
full outer join sq_card_transactions sqt
on vd.transaction_token = sqt.transaction_token
group by transaction_month
having year(transaction_month) = 2021
order by transaction_month asc


--------OKR 4.1 (fraud dispute rates by amount)


with valid_disputes as (
select *
from bizbank.raw_oltp.transaction_disputes
where reason_code = 'FRAUD_OR_UNAUTHORIZED'
AND year(transaction_date) = 2021
AND (
(final_resolution in ('APPROVED','DENIED','CREDITED_FROM_SQUARE','REJECTED_BY_MARQETA'))--'VOIDED',--'REJECTED_BY_SQUARE',--'REFUNDED',
OR
((final_resolution is null) and status in ('CLOSED','FILED','INITIATED','LOST','NETWORK_REJECTED','PREARBITRATION','REPRESENTMENT','WON')) --'WAITING_ON_CARD_HOLDER',--'NEW'
)),

sq_card_transactions as (
select transaction_token
, split_part(balance_token, '-c-', 2) as unit_token
, currency_code
, max(abs(amount_cents)/100) as amount
, min(created_at) as ua_transaction_time
from bizbank.raw_oltp.unified_activities ua
where activity_type ilike '%card_payment%'
and ACTIVITY_STATE  in ('SETTLED' )
group by 1,2,3
having year(min(created_at)) = 2021)

select date_trunc('month',ifnull(sqt.ua_transaction_time, vd.transaction_date)) as transaction_month,
sum(amount) as count_sq_card_transactions,
sum( case when  vd.transaction_token is not null then amount else null end) as count_sq_card_disputes,
10000 * count_sq_card_disputes / count_sq_card_transactions as bps_dispute_count
from sq_card_transactions sqt
left join  valid_disputes vd
on vd.transaction_token = sqt.transaction_token
group by transaction_month
having year(transaction_month) = 2021
order by transaction_month asc


---------okr 5 ( dispute counts per seller with disputes)
with valid_fraud_disputes as (
select *
from bizbank.raw_oltp.transaction_disputes
where reason_code = 'FRAUD_OR_UNAUTHORIZED'
AND year(transaction_date) = 2021
AND (
  (final_resolution in ('APPROVED','DENIED','CREDITED_FROM_SQUARE','REJECTED_BY_MARQETA'))--'VOIDED',--'REJECTED_BY_SQUARE',--'REFUNDED',
  OR
  ((final_resolution is null) and status in ('CLOSED','FILED','INITIATED','LOST','NETWORK_REJECTED','PREARBITRATION','REPRESENTMENT','WON')) --'WAITING_ON_CARD_HOLDER',--'NEW'
)),

all_fraud_disputes as (
select *
from bizbank.raw_oltp.transaction_disputes
where reason_code = 'FRAUD_OR_UNAUTHORIZED'
AND year(transaction_date) = 2021)

select date_trunc('month', td.transaction_date) as transaction_month,
count(distinct vd.transaction_token) as count_valid_fraud_disputes,
count(distinct vd.unit_token) as count_unique_sellers_filing_valid_fraud_disputes,
count(distinct fd.unit_token) as count_unique_sellers_filing_fraud_disputes,
count(distinct td.unit_token) as count_unique_sellers_filing_disputes
from bizbank.raw_oltp.transaction_disputes td
left join all_fraud_disputes fd
on td.transaction_token = fd.transaction_token
left join valid_fraud_disputes vd
on td.transaction_token = vd.transaction_token
where year(td.transaction_date) = 2021
and year(transaction_month) = 2021
group by transaction_month
order by transaction_month asc

--------okr 6 (bps of sellers with more than 50% of spend disputed)
with valid_disputes as (
select date_trunc('month', transaction_date) as transaction_month,
unit_token,
sum(dispute_amount) / 100.0 as total_fraud_dispute
from bizbank.raw_oltp.transaction_disputes
where reason_code = 'FRAUD_OR_UNAUTHORIZED'
AND year(transaction_date) = 2021
AND (
(final_resolution in ('APPROVED','DENIED','CREDITED_FROM_SQUARE','REJECTED_BY_MARQETA'))--'VOIDED',--'REJECTED_BY_SQUARE',--'REFUNDED',
OR
((final_resolution is null) and status in ('CLOSED','FILED','INITIATED','LOST','NETWORK_REJECTED','PREARBITRATION','REPRESENTMENT','WON')) --'WAITING_ON_CARD_HOLDER',--'NEW'
)
group by transaction_month, unit_token
),

sq_card_transactions as (
select date_trunc('month', created_at) as transaction_month, unit_token, sum(total_spend_amount) as total_spend_amount
from (
select transaction_token
, split_part(balance_token, '-c-', 2) as unit_token
, currency_code
, max(abs(amount_cents)/100) as total_spend_amount
, min(created_at) as created_at
from bizbank.raw_oltp.unified_activities ua
where activity_type ilike '%card_payment%'
and ACTIVITY_STATE  in ('SETTLED' )
group by 1,2,3
having year(min(created_at)) = 2021)
group by 1,2)

select sct.transaction_month,
count(distinct case when vd.total_fraud_dispute/sct.total_spend_amount > 0.5 then sct.unit_token else null end) as seller_with_50pct_more_disputes,
count(distinct sct.unit_token ) as total_active_sellers,
seller_with_50pct_more_disputes*10000/total_active_sellers as disp

from sq_card_transactions sct 
left join valid_disputes vd
on vd.transaction_month = sct.transaction_month
and vd.unit_token = sct.unit_token
where sct.total_spend_amount > 0
group by sct.transaction_month
order by sct.transaction_month asc

