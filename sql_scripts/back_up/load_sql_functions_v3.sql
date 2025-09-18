DROP FUNCTION IF EXISTS compute_features_v2(integer, character varying);
CREATE OR REPLACE FUNCTION compute_features_v2(mov_avg_span INTEGER, stock_symbol CHARACTER VARYING(50) DEFAULT NULL)
RETURNS TABLE (
    nse_symbol CHARACTER VARYING(50),
    "timestamp" TIMESTAMP,
    date DATE,
    "time" TIME,
    open real,
    high real,
    low real,
    close real,
    volume INTEGER,
    smavol numeric,
    day_high real,
    day_low real,
    volume_ratio numeric,
    day_range real,
    --smavol_ratio FLOAT,
    ls BOOLEAN,
    hs BOOLEAN,
    lh BOOLEAN,
    hh BOOLEAN,
    spk_t_vr INTEGER,
    spk_l_vr INTEGER,
    spk_m_vr INTEGER,
    spk_h_vr INTEGER,
    spk_t_svr numeric,
    spk_l_svr numeric,
    spk_m_svr numeric,
    spk_h_svr numeric,
    vol_l_i numeric,
    vol_h_i numeric,
    small_rng boolean,
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
                nse.date AS trade_date,
                SUM(nse.volume) FILTER (WHERE nse.volume > 0) AS daily_volume
                --SUM(CASE WHEN nse.volume > 0 THEN nse.volume ELSE 0 END) AS daily_volume
                --SUM(volume) FILTER (WHERE volume > 0) AS daily_volume
            FROM raw_data.nse_stock_data_non_fno nse
            GROUP BY nse.nse_symbol, nse.date
        ),
        moving_average AS (
            SELECT
                dv.nse_symbol,
                dv.trade_date,
                dv.daily_volume,
                AVG(dv.daily_volume) OVER (
                    PARTITION BY dv.nse_symbol
                    ORDER BY dv.trade_date
                    ROWS BETWEEN mov_avg_span PRECEDING AND 1 PRECEDING
                ) AS avg_volume_60_days
            FROM daily_volume dv
        ),
        shifted_average AS (
            SELECT
                ma.nse_symbol,
                ma.trade_date,
                ma.avg_volume_60_days,
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
                nse.date,
                nse."time",
                nse.open,
                nse.high,
                nse.low,
                nse.close,
                nse.volume,
                sa.shifted_avg_volume_60_days * 0.01648 AS smavol,
                --MAX(nse.high) OVER (PARTITION BY nse.nse_symbol, nse.date) AS day_high,
                --MIN(nse.low) OVER (PARTITION BY nse.nse_symbol, nse.date) AS day_low
                MAX(nse.high) OVER (PARTITION BY nse.nse_symbol, nse.date) AS day_high,
                MIN(nse.low) OVER (PARTITION BY nse.nse_symbol, nse.date) AS day_low
            FROM raw_data.nse_stock_data_non_fno nse
            LEFT JOIN shifted_average sa
                ON nse.nse_symbol = sa.nse_symbol
                AND nse.date = sa.trade_date
            WHERE (stock_symbol IS NULL OR nse.nse_symbol = stock_symbol)
        ),
        -- Step 3: Compute Minute-wise Features
        stock_with_ratio AS (
            SELECT
                jd.*,
                --jd.volume / NULLIF(jd.smavolume, 0) AS volume_ratio,
                jd.volume / NULLIF(jd.smavol, 0) AS volume_ratio,
                jd.day_high - jd.day_low AS day_range
            FROM joined_data jd
        ),
        /*avg_vol_ratio AS (
            SELECT
                sr.*,
                AVG(sr.volume_ratio) OVER (
                    PARTITION BY sr.nse_symbol
                    ORDER BY sr."timestamp"
                    ROWS BETWEEN mov_avg_span PRECEDING AND CURRENT ROW
                ) AS smavol_ratio
            FROM stock_with_ratio sr
        ),*/
        day_closure AS (
            SELECT
                sr.*,
                sr.close < ((sr.day_range) * 0.3 + sr.day_low) AS ls,
                sr.close > ((sr.day_range) * 0.7 + sr.day_low) AS hs,
                sr.close <= ((sr.day_range) * 0.5 + sr.day_low) AS lh,
                sr.close > ((sr.day_range) * 0.5 + sr.day_low) AS hh
            FROM stock_with_ratio sr
        ),
        process_threshold_vr AS (
            SELECT
                dc.*,
                CASE WHEN dc.volume > dc.smavol THEN 1 ELSE 0 END AS spk_t_vr,
                CASE WHEN (dc.volume > dc.smavol AND dc.ls = TRUE) THEN 1 ELSE 0 END AS spk_l_vr,
                CASE WHEN (dc.volume > dc.smavol AND dc.ls = FALSE AND dc.hs = FALSE) THEN 1 ELSE 0 END AS spk_m_vr,
                CASE WHEN (dc.volume > dc.smavol AND dc.hs = TRUE) THEN 1 ELSE 0 END AS spk_h_vr
            FROM day_closure dc
        ),
        process_threshold_svr AS (
            SELECT
                ptvr.*,
                CASE WHEN ptvr.spk_t_vr = 1 THEN ptvr.volume_ratio ELSE 0 END AS spk_t_svr,
                CASE WHEN ptvr.spk_l_vr = 1 THEN ptvr.volume_ratio ELSE 0 END AS spk_l_svr,
                CASE WHEN ptvr.spk_m_vr = 1 THEN ptvr.volume_ratio ELSE 0 END AS spk_m_svr,
                CASE WHEN ptvr.spk_h_vr = 1 THEN ptvr.volume_ratio ELSE 0 END AS spk_h_svr
            FROM process_threshold_vr ptvr
        ),

        process_volume_lh AS (
            SELECT 
                ptsvr.*,
                
                CASE WHEN (ptsvr.volume > ptsvr.smavol AND ptsvr.lh = TRUE) THEN ptsvr.volume_ratio ELSE 0 END AS vol_l_i,
                CASE WHEN (ptsvr.volume > ptsvr.smavol AND ptsvr.hh = TRUE) THEN ptsvr.volume_ratio ELSE 0 END AS vol_h_i,
                (((ptsvr.high - ptsvr.low)/(ptsvr.high + ptsvr.low))*100)<0.1 AS small_rng
            FROM process_threshold_svr ptsvr
        ),

        range_vs_vol_pre AS (
            SELECT 
                pvlh.*,
                
                CASE WHEN (pvlh.spk_t_vr=1 AND pvlh.small_rng=TRUE) THEN 1 ELSE 0 END AS srlv_i,
                CASE WHEN (pvlh.spk_t_vr=1 AND pvlh.small_rng=TRUE) THEN LEFT(to_char(pvlh."timestamp", 'HH24:MI'), 5) ELSE NULL END AS srlv_time

                

            FROM process_volume_lh pvlh




        ),
        range_vs_vol_LMH AS (
            SELECT
                rvp.*,
                CASE WHEN (rvp.spk_l_vr=1 AND rvp.small_rng=TRUE) THEN 1 ELSE 0 END AS low_srlv_i,
                --CASE WHEN (rvp.spk_m_vr=1 AND rvp.small_rng=TRUE) THEN 1 ELSE 0 END AS med_srlv_i,
                CASE WHEN (rvp.spk_h_vr=1 AND rvp.small_rng=TRUE) THEN 1 ELSE 0 END AS high_srlv_i
            FROM range_vs_vol_pre rvp

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



DROP FUNCTION IF EXISTS compute_daily_summary(mov_avg_span integer, stock_symbol character varying);
CREATE OR REPLACE FUNCTION compute_daily_summary(mov_avg_span integer, stock_symbol character varying(50) default NULL)
RETURNS TABLE (
    nse_symbol character varying(50),
    date date,
    spk_t_vr integer,
    spk_l_vr integer,
    spk_m_vr integer,
    spk_h_vr integer,
    spk_t_svr integer,
    spk_l_svr integer,
    spk_m_svr integer,
    spk_h_svr integer,
    vol_spk_l integer,
    vol_spk_h integer,
    srlv integer,
    low_srlv integer,
    high_srlv integer,
    low_srlv_close integer,
    high_srlv_close integer,
    l30_volume bigint,
    srlv_volume bigint,
    spk_t_l30 integer,
    srlv_l30 integer
    
    --vg_volume numeric,
    --avg_smavolume float,
    --avg_volume_ratio float,
    --avg_day_range float,
    --avg_smavol_ratio float
    --pct_ls float,
    --pct_hs float,
    --pct_lh float,
    --pct_hh float
) LANGUAGE plpgsql AS
$$
BEGIN
    RETURN QUERY
    WITH feature_data AS (
        SELECT * FROM compute_features_v2(mov_avg_span, stock_symbol)
    )
    SELECT 
        feature_data.nse_symbol,  -- Specify table alias
        feature_data.date,        -- Specify table alias
        SUM(feature_data.spk_t_vr)::integer AS spk_t_vr,
        SUM(feature_data.spk_l_vr)::integer AS spk_l_vr,
        SUM(feature_data.spk_m_vr)::integer AS spk_m_vr,
        SUM(feature_data.spk_h_vr)::integer AS spk_h_vr,
        SUM(feature_data.spk_t_svr)::integer AS spk_t_svr,
        SUM(feature_data.spk_l_svr)::integer AS spk_l_svr,
        SUM(feature_data.spk_m_svr)::integer AS spk_m_svr,
        SUM(feature_data.spk_h_svr)::integer AS spk_h_svr,

        SUM(feature_data.vol_l_i)::integer AS vol_spk_l,
        SUM(feature_data.vol_h_i)::integer AS vol_spk_h,
        SUM(feature_data.srlv_i)::integer AS srlv,
        SUM(feature_data.low_srlv_i)::integer AS low_srlv,
        SUM(feature_data.high_srlv_i)::integer AS high_srlv,
        AVG(feature_data.low_srlv_close_i)::integer AS low_srlv_close,
        AVG(feature_data.high_srlv_close_i)::integer AS high_srlv_close,
        SUM(feature_data.l30_volume_i) AS l30_volume,
        SUM(feature_data.srlv_volume_i) AS srlv_volume,
        SUM(feature_data.spk_t_l30)::integer AS spk_t_l30,
        SUM(feature_data.srlv_l30)::integer AS srlv_l30
        
        

        --AVG(feature_data.volume) AS avg_volume,
        --AVG(feature_data.smavolume) AS avg_smavolume,
        --AVG(feature_data.volume_ratio) AS avg_volume_ratio,
        --AVG(feature_data.day_range) AS avg_day_range,
        --AVG(feature_data.smavol_ratio) AS avg_smavol_ratio
        --100 * SUM(CASE WHEN feature_data.ls THEN 1 ELSE 0 END) / COUNT(*) AS pct_ls,
        --100 * SUM(CASE WHEN feature_data.hs THEN 1 ELSE 0 END) / COUNT(*) AS pct_hs,
        --100 * SUM(CASE WHEN feature_data.lh THEN 1 ELSE 0 END) / COUNT(*) AS pct_lh,
        --100 * SUM(CASE WHEN feature_data.hh THEN 1 ELSE 0 END) / COUNT(*) AS pct_hh
    FROM feature_data
    GROUP BY feature_data.nse_symbol, feature_data.date
    ORDER BY feature_data.nse_symbol, feature_data.date;

END;
$$;

