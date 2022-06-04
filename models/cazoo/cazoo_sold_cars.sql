with cazoo_all_cars as (

    select * from {{ref('cazoo_all_inventory')}}
),

last_observations as (

	select
		car_id,
		max(run_id) as run_id
	from cazoo_all_cars
	group by 1
	order by 2 desc
	)

select
	t.car_id,
	t.run_id,
    t2.dt,
	t2.make,
	t2.model,
	t2.is_for_purchase,
	t2.is_for_subscription,
	t2.pricing_full_price_currency,
	t2.pricing_full_price_value,
	t2.created_at,
    t2.trading_market
from last_observations as t
left join cazoo_all_cars as t2 on (t.car_id = t2.car_id and t.run_id = t2.run_id)
where t.run_id not in (select max(run_id) from cazoo_all_cars)