
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
	pool_fee int8 NULL,
	beneficiary_fee numeric(40) NULL,
	beneficiary varchar NULL,
	inserted_at timestamp NULL,
	CONSTRAINT exchange_event_pkey PRIMARY KEY (id)
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