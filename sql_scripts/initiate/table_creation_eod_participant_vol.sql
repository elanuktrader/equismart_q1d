-- Table: public.eod_participant_vol

-- DROP TABLE IF EXISTS public.eod_participant_vol;

CREATE TABLE IF NOT EXISTS raw_data.eod_participant_vol
(
    client_type character varying COLLATE pg_catalog."default" NOT NULL,
    future_index_long integer,
    future_index_short integer,
    future_stock_long integer,
    future_stock_short integer,
    option_index_call_long integer,
    option_index_put_long integer,
    option_index_call_short integer,
    option_index_put_short integer,
    option_stock_call_long integer,
    option_stock_put_long integer,
    option_stock_call_short integer,
    option_stock_put_short integer,
    total_long_contracts integer,
    total_short_contracts integer,
    date date NOT NULL,
    CONSTRAINT eod_participant_vol_pkey PRIMARY KEY (client_type, date)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.eod_participant_vol
    OWNER to postgres;