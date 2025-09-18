-- Table: public.eod_cm_bhav

-- DROP TABLE IF EXISTS public.eod_cm_bhav;

CREATE TABLE IF NOT EXISTS raw_data.eod_cm_bhav
(
    symbol character varying COLLATE pg_catalog."default" NOT NULL,
    series character(3) COLLATE pg_catalog."default",
    open real,
    high real,
    low real,
    close real,
    last real,
    prevclose real,
    tottrdqty bigint,
    tottrdval real,
    "timestamp" date NOT NULL,
    totaltrades integer,
    isin character varying COLLATE pg_catalog."default",
    avg_price real,
    deliv_qty bigint,
    deliv_per real,
    CONSTRAINT eod_cm_bhav_pkey PRIMARY KEY (symbol, "timestamp")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.eod_cm_bhav
    OWNER to postgres;
-- Index: eod_cm_bhav_idx_timestamp

-- DROP INDEX IF EXISTS public.eod_cm_bhav_idx_timestamp;

CREATE INDEX IF NOT EXISTS eod_cm_bhav_idx_timestamp
    ON raw_data.eod_cm_bhav USING btree
    ("timestamp" ASC NULLS LAST)
    TABLESPACE pg_default;
