with api_consensus as (

select * from {{var('raw')}}.public.visible_alpha_api_consensus
),

base_data as (

select
    ticker as bloomberg_ticker,
    data_period as va_period,
    data_relative_period as relative_period,
    date_from_parts(left(period_date,4), right(left(period_date,6),2), right(period_date,2)) as period_end_dt,
    data_param_id as parameter_id,
    parameter_name,
    source as data_type,
    data_value as value
from api_consensus
qualify
    row_number() over(partition by ticker, data_period, data_param_id, source order by run_id desc) = 1
order by 1,5,6,4
),

final as (

select *
from base_data
where
    -- Exclude bad ticker/parameter_id combinations
    not (bloomberg_ticker = 'ATVI US' and parameter_id = 4852)

)

select *
from final
