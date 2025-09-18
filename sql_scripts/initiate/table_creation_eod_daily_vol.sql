-- Table: public.eod_daily_vol

-- DROP TABLE IF EXISTS public.eod_daily_vol;

CREATE TABLE IF NOT EXISTS raw_data.eod_daily_vol
(
    "record type" character varying COLLATE pg_catalog."default",
    "sr no" integer,
    symbol character varying COLLATE pg_catalog."default" NOT NULL,
    series character(3) COLLATE pg_catalog."default",
    "quantity traded" bigint,
    "deliverable quantity" bigint,
    "del percent" real,
    "timestamp" date NOT NULL,
    CONSTRAINT eod_daily_vol_pkey PRIMARY KEY (symbol, "timestamp")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.eod_daily_vol
    OWNER to postgres;