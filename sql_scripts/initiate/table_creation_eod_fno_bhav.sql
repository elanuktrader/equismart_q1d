-- Table: public.eod_fno_bhav

-- DROP TABLE IF EXISTS public.eod_fno_bhav;

CREATE TABLE IF NOT EXISTS raw_data.eod_fno_bhav
(
    instrument character varying COLLATE pg_catalog."default",
    symbol character varying COLLATE pg_catalog."default" NOT NULL,
    expiry_dt date NOT NULL,
    open real,
    high real,
    low real,
    close real,
    settle_pr real,
    contracts integer,
    val_inlakh real,
    open_int bigint,
    chg_in_oi bigint,
    "timestamp" date NOT NULL,
    underlying_pr real,
    CONSTRAINT eod_fno_bhav_pkey PRIMARY KEY (symbol, expiry_dt, "timestamp")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.eod_fno_bhav
    OWNER to postgres;
-- Index: eod_fno_bhav_timestamp

-- DROP INDEX IF EXISTS public.eod_fno_bhav_timestamp;

CREATE INDEX IF NOT EXISTS eod_fno_bhav_timestamp
    ON raw_data.eod_fno_bhav USING btree
    ("timestamp" ASC NULLS LAST)
    TABLESPACE pg_default;