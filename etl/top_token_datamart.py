from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

"""
create or replace view view_trades24h
as
select 'default' as platform, to_timestamp(ee.created_at) as swap_time,
sender as swap_src_owner, src_token  as swap_src_token,
src_amount as swap_src_amount,dst_token as swap_dst_token, pool_fee, dst_amount as swap_dst_amount,
  case when src_token = '0:2c3a2ff6443af741ce653ae4ef2c85c2d52a9df84944bbe14d702c3131da3f14' then true else false end as src_is_native,
  case when dst_token = '0:2c3a2ff6443af741ce653ae4ef2c85c2d52a9df84944bbe14d702c3131da3f14' then true else false end as dst_is_native 
  from exchange_event ee
where to_timestamp(ee.created_at)  > now() - interval '1 day'

create or replace view view_trades24h_native
as
with trades_native_direct as (
select
  platform, swap_time, swap_src_owner as swap_owner, swap_src_token as token,
  swap_src_amount as amount_token, swap_dst_amount as amount_native,
  'sell' as direction
  from view_trades24h where dst_is_native
union all
select
  platform, swap_time, swap_src_owner as swap_owner, swap_dst_token as token,
  swap_dst_amount as amount_token, swap_src_amount as amount_native,
  'buy' as direction
  from view_trades24h where src_is_native
),  market_volume_direct as  (
  select token, round(sum(amount_native) / 1000000000) as market_volume_native from trades_native_direct
  group by 1
), last_trades_ranks as (
select
  swap_time, swap_src_token as token, swap_src_amount as amount_token, swap_dst_amount as amount_native,
  'sell' as direction, rank() over(partition by swap_src_token order by swap_time desc) as rank
  from view_trades24h where dst_is_native
union all
select
  swap_time, swap_dst_token as token, swap_dst_amount as amount_token, swap_src_amount as amount_native,
  'buy' as direction, rank() over(partition by swap_dst_token order by swap_time desc) as rank
  from view_trades24h where src_is_native
), prices as (
  select token, sum(amount_native) / sum(amount_token) as price_raw  from last_trades_ranks
  where rank < 4 -- last 3 trades
  group by 1 having sum(amount_token) > 0
), significant_prices as (
  select token, price_raw from prices
  join market_volume_direct using(token) 
  where market_volume_native > 100
), trades_native_indirect as (
select
  platform, swap_time, swap_src_owner as swap_owner, swap_src_token as token,
  swap_src_amount as amount_token, round(swap_dst_amount * price_raw) as amount_native,
  'sell' as direction
  from view_trades24h 
  join significant_prices on significant_prices.token = swap_dst_token
  where dst_is_native = false and src_is_native = false
  union all
select
  platform, swap_time, swap_src_owner as swap_owner, swap_dst_token as token,
  swap_dst_amount as amount_token, round(swap_src_amount * price_raw) as amount_native,
  'buy' as direction
  from view_trades24h
  join significant_prices on significant_prices.token = swap_src_token
  where dst_is_native = false and src_is_native = false
)
select * from trades_native_indirect 
union all
select * from trades_native_direct 

CREATE TABLE IF NOT EXISTS top_tokens_datamart (
    id bigserial NOT NULL primary key,
    build_time timestamp with time zone NOT NULL, 
    address varchar,
    symbol varchar, 
    price decimal(40, 20),
    market_volume decimal(40, 0),
    market_volume_rank bigint,
    active_traders_24 bigint
)
"""

@dag(
    schedule_interval="*/20 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    tags=['venom', 'tokens', 'datamart']
)
def venom_top_token_datamart():
      add_current_top_jettons = PostgresOperator(
        task_id="add_current_top_jettons",
        postgres_conn_id="venom_db",
        sql=[
            """
            insert into top_tokens_datamart(build_time, address, 
              symbol, price, market_volume,
              market_volume_rank, active_traders_24           
            )
     
     
            with market_volume as  (
            select token, round(sum(amount_native) / 1000000000) as market_volume from view_trades24h_native
            group by 1
            ), market_volume_rank as (
            select *, rank() over(order by market_volume desc) as market_volume_rank from market_volume
            ), last_trades_ranks as (
            select
            swap_time, swap_src_token as token, swap_src_amount as amount_token, swap_dst_amount as amount_native,
            'sell' as direction, rank() over(partition by swap_src_token order by swap_time desc) as rank
            from view_trades24h where dst_is_native
            union all
            select
            swap_time, swap_dst_token as token, swap_dst_amount as amount_token, swap_src_amount as amount_native,
            'buy' as direction, rank() over(partition by swap_dst_token order by swap_time desc) as rank
            from view_trades24h where src_is_native
            ), prices as (
            select token, sum(amount_native) / sum(amount_token) as price_raw  from last_trades_ranks
            where rank < 4 -- last 3 trades
            group by 1
            having sum(amount_token) > 0
            ), datamart as (
            select mv.*, ti.symbol, case
                when coalesce(ti.decimals, 9) = 9 then price_raw
                when ti.decimals < 9 then price_raw / (pow(10, 9 - ti.decimals))
                else price_raw * (pow(10, ti.decimals -9))
            end as price, ti.decimals  from market_volume_rank as mv
            join tokens_info ti on ti.id  = mv.token 
            join prices on prices.token = mv.token
            where market_volume_rank > 100 or market_volume > 10
            ), target_tokens as (
            select distinct token as address from datamart
            ), active_traders as (
            select  target_tokens.address as token, count(distinct swap_src_owner) as active_traders_24 
            from view_trades24h
            join target_tokens on target_tokens.address = view_trades24h.swap_src_token or target_tokens.address = view_trades24h.swap_dst_token
            group by 1
            )
            select  now() as build_time, token as address, 
            symbol, price,  
            market_volume, market_volume_rank, 
            at.active_traders_24   
            from datamart 
            join active_traders at using(token)
            """
        ]
    )


venom_top_token_datamart_dag = venom_top_token_datamart()