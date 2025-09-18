DROP FUNCTION IF EXISTS compute_cm_summary(mov_avg_span integer, stock_symbol character varying,fetch_date date, insert_date date);
CREATE OR REPLACE FUNCTION compute_cm_summary(
    mov_avg_span integer, 
    stock_symbol character varying(50) default NULL,
    fetch_date DATE DEFAULT NULL,
    insert_date DATE DEFAULT NULL )
RETURNS TABLE (
    nse_symbol character varying(50),
    date date,
    spk_t_vr SMALLINT,
    spk_l_vr SMALLINT,
    spk_m_vr SMALLINT,
    spk_h_vr SMALLINT,
    spk_t_svr REAL,
    spk_l_svr REAL,
    spk_m_svr REAL,
    spk_h_svr REAL,
    vol_spk_l REAL,
    vol_spk_h REAL,
    srlv SMALLINT,
    srlv_time text,
    low_srlv SMALLINT,
    low_srlv_time text,
    high_srlv SMALLINT,
    high_srlv_time text,
    low_srlv_close INTEGER,
    high_srlv_close INTEGER,
    l30_volume bigint,
    srlv_volume bigint,
    spk_t_l30 SMALLINT,
    srlv_l30 SMALLINT
    
) LANGUAGE plpgsql AS
$$
BEGIN
    RETURN QUERY
    WITH feature_data AS (
        SELECT * 
        FROM compute_cm_features(mov_avg_span, stock_symbol,fetch_date)
        
    )
    SELECT 
        fd.nse_symbol,  -- Specify table alias
        fd."timestamp"::date,        -- Specify table alias
        SUM(fd.spk_t_vr)::SMALLINT AS spk_t_vr,
        SUM(fd.spk_l_vr)::SMALLINT AS spk_l_vr,
        SUM(fd.spk_m_vr)::SMALLINT AS spk_m_vr,
        SUM(fd.spk_h_vr)::SMALLINT AS spk_h_vr,
        ROUND(SUM(fd.spk_t_svr),2)::REAL AS spk_t_svr,
        ROUND(SUM(fd.spk_l_svr),2)::REAL AS spk_l_svr,
        ROUND(SUM(fd.spk_m_svr),2)::REAL AS spk_m_svr,
        ROUND(SUM(fd.spk_h_svr),2)::REAL AS spk_h_svr,
        ROUND(SUM(fd.vol_l_i),2)::REAL AS vol_spk_l,
        ROUND(SUM(fd.vol_h_i),2)::REAL AS vol_spk_h,
        SUM(fd.srlv_i)::SMALLINT AS srlv,
        STRING_AGG(fd.srlv_time, ', ') AS srlv_time,
        SUM(fd.low_srlv_i)::SMALLINT AS low_srlv,
        STRING_AGG(fd.low_srlv_time, ', ') AS low_srlv_time,
        SUM(fd.high_srlv_i)::SMALLINT AS high_srlv,
        STRING_AGG(fd.high_srlv_time, ', ') AS high_srlv_time,
        AVG(fd.low_srlv_close_i)::INTEGER AS low_srlv_close,
        AVG(fd.high_srlv_close_i)::INTEGER AS high_srlv_close,
        SUM(fd.l30_volume_i) AS l30_volume,
        SUM(fd.srlv_volume_i) AS srlv_volume,
        SUM(fd.spk_t_l30)::SMALLINT AS spk_t_l30,
        SUM(fd.srlv_l30)::SMALLINT AS srlv_l30
    
        

    FROM feature_data fd
    where (fd."timestamp"::date >= insert_date or  insert_date is NULL)
    GROUP BY fd.nse_symbol, fd."timestamp"::date
    ORDER BY fd.nse_symbol, fd."timestamp"::date;

END;
$$;


DROP FUNCTION IF EXISTS compute_cm_features(integer, character varying,date);
CREATE OR REPLACE FUNCTION compute_cm_features(
    mov_avg_span INTEGER, 
    stock_symbol CHARACTER VARYING(50) DEFAULT NULL,
    fetch_date DATE DEFAULT NULL)
RETURNS TABLE (
    nse_symbol CHARACTER VARYING(50),
    "timestamp" TIMESTAMP,
    open real,
    high real,
    low real,
    close real,
    volume INTEGER,
    day_high real,
    day_low real,
    day_range real,
    smavol numeric,
    volume_ratio numeric,
    --day_range real,
    --smavol_ratio FLOAT,
    ls BOOLEAN,
    hs BOOLEAN,
    lh BOOLEAN,
    hh BOOLEAN,
    small_rng boolean,
    spk_t_vr INTEGER,
    spk_l_vr INTEGER,
    spk_m_vr INTEGER,
    spk_h_vr INTEGER,
    vol_l_i numeric,
    vol_h_i numeric,
    spk_t_svr numeric,
    spk_l_svr numeric,
    spk_m_svr numeric,
    spk_h_svr numeric,
    srlv_i integer,
    srlv_time text,
    low_srlv_i integer,
    high_srlv_i integer,
    low_srlv_time text,
    low_srlv_close_i real,
    high_srlv_time text,
    high_srlv_close_i real,
    l30_volume_i integer,
    spk_t_l30 integer,
    srlv_l30 integer,
    srlv_volume_i integer
) LANGUAGE plpgsql AS
$$
BEGIN
    RAISE NOTICE 'Starting function execution: %', clock_timestamp();

    RETURN QUERY
    WITH 
        -- Step 1: Daily Data Computation
        daily_volume AS (
            SELECT
                nse.nse_symbol,
                nse."timestamp"::date AS trade_date,
                MAX(nse.high) AS day_high,
                MIN(nse.low) AS day_low,
                SUM(nse.volume) FILTER (WHERE nse.volume > 0) AS daily_volume

                --SUM(CASE WHEN nse.volume > 0 THEN nse.volume ELSE 0 END) AS daily_volume
                --SUM(volume) FILTER (WHERE volume > 0) AS daily_volume
            FROM raw_data.nse_stock_cm_data nse
            WHERE ((stock_symbol IS NULL OR nse.nse_symbol = stock_symbol) AND (nse."timestamp"::date>=fetch_date or fetch_date is NULL )  )
            GROUP BY nse.nse_symbol, nse."timestamp"::date
        ),
        moving_average AS (
            SELECT
                dv.*,
                AVG(dv.daily_volume) OVER (
                    PARTITION BY dv.nse_symbol
                    ORDER BY dv.trade_date
                    ROWS BETWEEN mov_avg_span PRECEDING AND 1 PRECEDING
                ) AS avg_volume_60_days
            FROM daily_volume dv
        ),
        shifted_average AS (
            SELECT
                ma.*,
                ma.day_high - ma.day_low AS day_range,
                LAG(ma.avg_volume_60_days) OVER (
                    PARTITION BY ma.nse_symbol
                    ORDER BY ma.trade_date
                ) AS shifted_avg_volume_60_days
            FROM moving_average ma
        ),
        -- Step 2: Join Daily Data with Minute-wise Data       
        joined_data AS (
            SELECT 
                nse.nse_symbol,
                nse."timestamp",
                nse.open,
                nse.high,
                nse.low,
                nse.close,
                nse.volume,
                --MAX(nse.high) OVER (PARTITION BY nse.nse_symbol, nse."timestamp"::date) AS day_high,
                --MIN(nse.low) OVER (PARTITION BY nse.nse_symbol, nse."timestamp"::date) AS day_low,
                sa.day_high,
                sa.day_low,
                sa.day_range,
                sa.shifted_avg_volume_60_days * 0.01648 AS smavol
                --MAX(nse.high) OVER (PARTITION BY nse.nse_symbol, nse.date) AS day_high,
                --MIN(nse.low) OVER (PARTITION BY nse.nse_symbol, nse.date) AS day_low

            FROM raw_data.nse_stock_cm_data nse
            LEFT JOIN shifted_average sa
                ON nse.nse_symbol = sa.nse_symbol
                AND nse."timestamp"::date = sa.trade_date
            WHERE ((stock_symbol IS NULL OR nse.nse_symbol = stock_symbol) AND (nse."timestamp"::date>=fetch_date or fetch_date is NULL) )
        ),
        -- Step 3: Compute Minute-wise Features
        stock_with_ratio AS (
            SELECT
                jd.*,
                jd.volume / NULLIF(jd.smavol, 0) AS volume_ratio
                --jd.day_high - jd.day_low AS day_range
            FROM joined_data jd
            WHERE (jd.smavol IS NOT NULL AND jd.smavol != 0)
        ),
        
        day_closure AS (
            SELECT
                sr.*,
                sr.close < ((sr.day_range) * 0.3 + sr.day_low) AS ls,
                sr.close > ((sr.day_range) * 0.7 + sr.day_low) AS hs,
                sr.close <= ((sr.day_range) * 0.5 + sr.day_low) AS lh,
                sr.close > ((sr.day_range) * 0.5 + sr.day_low) AS hh,
                (((sr.high - sr.low)/NULLIF((sr.high + sr.low),0))*100)<0.1 AS small_rng
            FROM stock_with_ratio sr
        ),
        process_threshold_vr AS (
            SELECT
                dc.*,
                CASE WHEN dc.volume > dc.smavol THEN 1 ELSE 0 END AS spk_t_vr,
                CASE WHEN (dc.volume > dc.smavol AND dc.ls = TRUE) THEN 1 ELSE 0 END AS spk_l_vr,
                CASE WHEN (dc.volume > dc.smavol AND dc.ls = FALSE AND dc.hs = FALSE) THEN 1 ELSE 0 END AS spk_m_vr,
                CASE WHEN (dc.volume > dc.smavol AND dc.hs = TRUE) THEN 1 ELSE 0 END AS spk_h_vr,
                CASE WHEN (dc.volume > dc.smavol AND dc.lh = TRUE) THEN dc.volume_ratio ELSE 0 END AS vol_l_i,
                CASE WHEN (dc.volume > dc.smavol AND dc.hh = TRUE) THEN dc.volume_ratio ELSE 0 END AS vol_h_i
            FROM day_closure dc
        ),
        process_threshold_svr AS (
            SELECT
                ptvr.*,
                CASE WHEN ptvr.spk_t_vr = 1 THEN ptvr.volume_ratio ELSE 0 END AS spk_t_svr,
                CASE WHEN ptvr.spk_l_vr = 1 THEN ptvr.volume_ratio ELSE 0 END AS spk_l_svr,
                CASE WHEN ptvr.spk_m_vr = 1 THEN ptvr.volume_ratio ELSE 0 END AS spk_m_svr,
                CASE WHEN ptvr.spk_h_vr = 1 THEN ptvr.volume_ratio ELSE 0 END AS spk_h_svr,
                CASE WHEN (ptvr.spk_t_vr=1 AND ptvr.small_rng=TRUE) THEN 1 ELSE 0 END AS srlv_i,
                CASE WHEN (ptvr.spk_t_vr=1 AND ptvr.small_rng=TRUE) THEN LEFT(to_char(ptvr."timestamp", 'HH24:MI'), 5) ELSE NULL END AS srlv_time

            FROM process_threshold_vr ptvr
        ),

        
        range_vs_vol_LMH AS (
            SELECT
                rvp.*,
                CASE WHEN (rvp.spk_l_vr=1 AND rvp.small_rng=TRUE) THEN 1 ELSE 0 END AS low_srlv_i,
                --CASE WHEN (rvp.spk_m_vr=1 AND rvp.small_rng=TRUE) THEN 1 ELSE 0 END AS med_srlv_i,
                CASE WHEN (rvp.spk_h_vr=1 AND rvp.small_rng=TRUE) THEN 1 ELSE 0 END AS high_srlv_i
            FROM process_threshold_svr rvp

        ),

        range_vs_vol AS (
            SELECT
                rvlmh.*,

                CASE WHEN rvlmh.low_srlv_i = 1 THEN  rvlmh.srlv_time ELSE NULL END  AS low_srlv_time,
                CASE WHEN rvlmh.low_srlv_i = 1 THEN rvlmh.close ELSE NULL END AS low_srlv_close_i,
                CASE WHEN rvlmh.high_srlv_i = 1 THEN  rvlmh.srlv_time ELSE NULL END  AS high_srlv_time,
                CASE WHEN rvlmh.high_srlv_i = 1 THEN rvlmh.close ELSE NULL END AS high_srlv_close_i,
                CASE WHEN (EXTRACT(HOUR FROM rvlmh."timestamp") = 15)   THEN rvlmh.volume ELSE 0 END AS l30_volume_i,
                CASE WHEN (EXTRACT(HOUR FROM rvlmh."timestamp") = 15 AND rvlmh.spk_t_vr=1)   THEN 1 ELSE 0 END AS spk_t_l30,
                CASE WHEN (EXTRACT(HOUR FROM rvlmh."timestamp") = 15 AND rvlmh.srlv_i=1)   THEN 1 ELSE 0 END AS srlv_l30,
                CASE WHEN rvlmh.srlv_i=1   THEN rvlmh.volume ELSE 0 END AS srlv_volume_i

                

            FROM range_vs_vol_LMH rvlmh

        )






    SELECT * FROM range_vs_vol;

    RAISE NOTICE 'Ending function execution: %', clock_timestamp();
END;
$$;




