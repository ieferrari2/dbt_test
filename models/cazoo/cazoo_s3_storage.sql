with raw_data as (

select *
from bod_dev_database.public.cazoo_s3_storage
),

final as (

select
    to_date(to_timestamp(run_id)) as dt,
    *
from raw_data
where
    -- Exclude bad runs
    run_id not in (1646179502)
)

select *
from final