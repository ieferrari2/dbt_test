{% set bad_runs = ['1635120007', '1631577605', '1649894702', '1650288925', '1650240302'] %}
-- Exclude data collected in bad runs

with

rds_inventory as (

select *
from {{var('raw_db')}}.public.static_cazoo_rds_inventory
),

s3_inventory as (

select * from {{ref('cazoo_s3_storage')}}
),

rds_inventory_clean as (

select
    run_id,
    to_date(to_timestamp(run_id)) as dt,
    created_at,
    car_id,
    make,
    model,
    model_year,
    display_variant,
    mileage,
    registration_year,
    is_for_subscription,
    is_for_purchase,
    is_promoted,
    pricing_full_price_currency,
    pricing_full_price_value,
    pricing_subscription_price_currency,
    pricing_subscription_price_value,
    null as page_number,
    'Probably GB' as trading_market,
    'RDS' as data_source
from rds_inventory
where
    run_id <= 1638403207 and
    run_id != 1634654177
    -- We had a failed run that resulted in duplicate cars for 2021-10-19
),

clean_s3_inventory as (

select
    run_id,
    dt,
    created_at,
    car_id,
    make,
    model,
    model_year,
    display_variant,
    mileage,
    registration_year,
    is_for_subscription,
    is_for_purchase,
    is_promoted,
    pricing_full_price_currency,
    pricing_full_price_value,
    pricing_subscription_price_currency,
    pricing_subscription_price_value,
    page_number,
    trading_market,
    'S3' as data_source
from s3_inventory
where
    run_id >= 1638489903
),

merged_data as (
select * from rds_inventory_clean
union
select * from clean_s3_inventory
),

final as (

select *
from merged_data
where
    run_id not in ('{{ "', '".join(bad_runs)}}')
)

select *
from final