
CREATE TABLE public.messages (
	id varchar NOT NULL,
	src varchar NULL,
	dst varchar NULL,
	created_at int8 NULL,
	created_lt int8 NULL,
	fwd_fee int8 NULL,
	ihr_fee int8 NULL,
	import_fee int8 NULL,
	bounce bool NULL,
	bounced bool NULL,
	value int8 NULL,
	"type" varchar NULL,
	inserted_at timestamp NULL,
	CONSTRAINT messages_pkey PRIMARY KEY (id)
);

CREATE TABLE public.events (
	id varchar NOT NULL,
	function_id int8 NULL,
	created_at int8 NULL,
	"name" varchar NULL,
	"data" varchar NULL,
	inserted_at timestamp NULL,
	CONSTRAINT events_pkey PRIMARY KEY (id)
);

CREATE TABLE public.exchange_event (
	id varchar NOT NULL,
	created_at int8 NULL,
	sender varchar NULL,
	recipient varchar NULL,
	src_token varchar NULL,
	src_amount numeric(40) NULL,
	dst_token varchar NULL,
	dst_amount numeric(40) NULL,
	pool_fee numeric(40) NULL,
	beneficiary_fee numeric(40) NULL,
	beneficiary varchar NULL,
	inserted_at timestamp NULL,
	CONSTRAINT exchange_event_pkey PRIMARY KEY (id)
);

CREATE TABLE public.sync_event (
	id varchar NOT NULL,
	created_at int8 NULL,
	reserve0 numeric(40) NULL,
	reserve1 numeric(40) NULL,
	lp_supply numeric(40) NULL,
	inserted_at timestamp NULL,
	CONSTRAINT sync_event_pkey PRIMARY KEY (id)
);

CREATE TABLE public.tokens_info (
	id varchar NOT NULL,
	standard varchar NULL,
	symbol varchar NULL,
	"name" varchar NULL,
	decimals int8 NULL,
	created_at int8 NULL,
	root_owner varchar NULL,
	total_supply numeric(40) NULL,
	updated_at timestamp NULL,
	CONSTRAINT tokens_info_pkey PRIMARY KEY (id)
);

CREATE TABLE public.dex_pair (
	id varchar NOT NULL,
	token0 varchar NULL,
	token1 varchar NULL,
	lp varchar NULL,
	inserted_at timestamp NULL,
	sync_id varchar NULL,
	CONSTRAINT dex_pair_pkey PRIMARY KEY (id)
);


CREATE TABLE public.tvl_history (
	id serial,
	build_time timestamp NULL,
	pool varchar NULL,
	token varchar NULL,
	reserve decimal(40, 0),
	CONSTRAINT tvl_history_pkey PRIMARY KEY (id)
);


CREATE TABLE public.usd_tokens (
	"token" varchar NOT NULL
);
CREATE UNIQUE INDEX usd_tokens_token_idx ON public.usd_tokens ("token");


-- views
create or replace view venomics.dex_reserves
as
select dp.id, to_timestamp(se.created_at) as update_time , se.reserve0 , se.reserve1 , 
ti0.decimals  as token0_decimals,
ti1.decimals  as token1_decimals,
ti0.symbol  as token0_symbol,
ti1.symbol  as token1_symbol,
dp.token0, dp.token1 from dex_pair dp
join sync_event se on dp.sync_id  = se.id 
join tokens_info ti0 on ti0.id = dp.token0 
join tokens_info ti1 on ti1.id = dp.token1

create or replace view venomics.token_info
as select * from public.tokens_info ti 

create or replace view venomics.dex_swaps
as
select to_timestamp(ee.created_at) as time, sender as 	, 
src_token , ti1.symbol  as src_token_symbol, src_amount, ti1.decimals  as src_decimals,
dst_token , ti2.symbol  as dst_token_symbol, dst_amount, ti2.decimals  as dst_decimals
from exchange_event ee 
join tokens_info ti1 on ti1.id = src_token
join tokens_info ti2 on ti2.id = dst_token


create or replace view venomics.tvl_current
as
with native_based_pairs_raw as (
  select id, (reserve0  / pow(10, token0_decimals)) as tvl_native, token0 as token from venomics.dex_reserves r
  where r.token0 = '0:2c3a2ff6443af741ce653ae4ef2c85c2d52a9df84944bbe14d702c3131da3f14'
  union all
  select id, (reserve0  / pow(10, token0_decimals)) as tvl_native, token1 as token from venomics.dex_reserves r
  where r.token0 = '0:2c3a2ff6443af741ce653ae4ef2c85c2d52a9df84944bbe14d702c3131da3f14'
  union all 
  select id, (reserve1  / pow(10, token1_decimals)) as tvl_native, token0 as token from venomics.dex_reserves r
  where r.token1 = '0:2c3a2ff6443af741ce653ae4ef2c85c2d52a9df84944bbe14d702c3131da3f14'
  union all 
  select id, (reserve1  / pow(10, token1_decimals)) as tvl_native, token1 as token from venomics.dex_reserves r
  where r.token1 = '0:2c3a2ff6443af741ce653ae4ef2c85c2d52a9df84944bbe14d702c3131da3f14'
), native_based_pairs as (
select id, token, max(tvl_native) as tvl_native from native_based_pairs_raw
group by 1, 2
), latest_prices as (
  select address, build_time, price from top_tokens_datamart src
  where build_time = (select max(build_time) from top_tokens_datamart ttd where src.address = ttd.address)
), non_native_based_pairs_raw as (
  select r.id, round(reserve0 * lp0.price / pow(10, ti0.decimals)) as tvl_native,
  token0 as token
  from venomics.dex_reserves r
  join tokens_info ti0 on ti0.id = token0
  left join latest_prices lp0 on lp0.address = token0
  where token0 != '0:2c3a2ff6443af741ce653ae4ef2c85c2d52a9df84944bbe14d702c3131da3f14'
  and token1 != '0:2c3a2ff6443af741ce653ae4ef2c85c2d52a9df84944bbe14d702c3131da3f14'
  
  union all 
  
  select r.id, round(reserve1 * lp1.price / pow(10, ti1.decimals)) as tvl_native,
  token1 as token
  from venomics.dex_reserves r
  join tokens_info ti1 on ti1.id = token1
  left join latest_prices lp1 on lp1.address = token1
  where token0 != '0:2c3a2ff6443af741ce653ae4ef2c85c2d52a9df84944bbe14d702c3131da3f14'
  and token1 != '0:2c3a2ff6443af741ce653ae4ef2c85c2d52a9df84944bbe14d702c3131da3f14'
), non_native_based_pairs as (
  select id, token, max(tvl_native) as tvl_native from non_native_based_pairs_raw
  group by 1, 2
), all_pools as (
  select * from non_native_based_pairs
  union all
  select * from native_based_pairs
)
select * from all_pools