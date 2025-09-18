DROP FUNCTION IF EXISTS compute_fno_summary(mov_avg_span integer, stock_symbol character varying);
CREATE OR REPLACE FUNCTION compute_fno_summary(mov_avg_span integer, stock_symbol character varying(50) default NULL)
RETURNS TABLE (
    nse_symbol character varying(50),
    date date,
    f_spk_t_svr REAL,
    f_spk_l_svr REAL,
    f_spk_m_svr REAL,
    f_spk_h_svr REAL,
    f_vol_spk_l REAL,
    f_vol_spk_h REAL,
    t_spk_oi_form REAL,
    t_spk_oi_cover REAL,
    l_spk_oi_form REAL,
    m_spk_oi_form REAL,
    h_spk_oi_form REAL,
    l_spk_oi_cover REAL,
    m_spk_oi_cover REAL,
    h_spk_oi_cover REAL,
    f_l30_volume REAL,
    l30_oi_form REAL,
    l30_oi_cover REAL
) LANGUAGE plpgsql AS
$$
BEGIN
    RETURN QUERY
    WITH feature_data AS (
        SELECT * FROM compute_fno_features(mov_avg_span, stock_symbol)
    )
    SELECT 
        fd.nse_symbol,  -- Specify table alias
        fd."timestamp"::date,        -- Specify table alias
        ROUND(SUM(fd.f_spk_t_svr), 2)::REAL AS f_spk_t_svr,
        ROUND(SUM(fd.f_spk_l_svr), 2)::REAL AS f_spk_l_svr,
        ROUND(SUM(fd.f_spk_m_svr), 2)::REAL AS f_spk_m_svr,
        ROUND(SUM(fd.f_spk_h_svr), 2)::REAL AS f_spk_h_svr,
        ROUND(SUM(fd.vol_l_i), 2)::REAL AS f_vol_spk_l,
        ROUND(SUM(fd.vol_h_i), 2)::REAL AS f_vol_spk_h,
        ROUND(SUM(fd.oi_form_t), 2)::REAL AS t_spk_oi_form,
        ROUND(SUM(fd.oi_cover_t), 2)::REAL AS t_spk_oi_cover,
        ROUND(SUM(fd.oi_form_l), 2)::REAL AS l_spk_oi_form,
        ROUND(SUM(fd.oi_form_m), 2)::REAL AS m_spk_oi_form,
        ROUND(SUM(fd.oi_form_h), 2)::REAL AS h_spk_oi_form,
        ROUND(SUM(fd.oi_cover_l), 2)::REAL AS l_spk_oi_cover,
        ROUND(SUM(fd.oi_cover_m), 2)::REAL AS m_spk_oi_cover,
        ROUND(SUM(fd.oi_cover_h), 2)::REAL AS h_spk_oi_cover,
        ROUND(SUM(fd.l30_volume_i), 2)::REAL AS f_l30_volume,
        ROUND(SUM(fd.l30_oi_form), 2)::REAL AS l30_oi_form,
        ROUND(SUM(fd.l30_oi_cover), 2)::REAL AS l30_oi_cover 
    FROM feature_data fd
    GROUP BY fd.nse_symbol, fd."timestamp"::date
    ORDER BY fd.nse_symbol, fd."timestamp"::date;
END;
$$;



DROP FUNCTION IF EXISTS compute_fno_features(integer, character varying);
CREATE OR REPLACE FUNCTION compute_fno_features(mov_avg_span INTEGER, stock_symbol CHARACTER VARYING(50) DEFAULT NULL)
RETURNS TABLE (
    nse_symbol CHARACTER VARYING(50),
    "timestamp" TIMESTAMP,
    open real,
    high real,
    low real,
    close real,
    t_volume bigint,
    t_coi bigint,
    t_oi bigint,
    day_high real,
    day_low real,
    day_range real,
    smavol numeric,
    mov_avg_oi_form numeric,
    mov_avg_oi_cover numeric,
    volume_ratio numeric,
    t_coi_percent numeric,
    ls BOOLEAN,
    hs BOOLEAN,
    lh BOOLEAN,
    hh BOOLEAN,
    f_spk_t_svr numeric,
    f_spk_l_svr numeric,
    f_spk_m_svr numeric,
    f_spk_h_svr numeric,
    vol_l_i numeric,
    vol_h_i numeric,
    oi_form_t numeric,
    oi_cover_t numeric,
    oi_form_l numeric,
    oi_form_m numeric,
    oi_form_h numeric,
    oi_cover_l numeric,
    oi_cover_m numeric,
    oi_cover_h numeric,
    l30_volume_i bigint,
    l30_oi_form numeric,
    l30_oi_cover numeric

   
) LANGUAGE plpgsql AS
$$
BEGIN
    RAISE NOTICE 'Starting function execution: %', clock_timestamp();

    RETURN QUERY
    WITH 

        aggregated_table AS (
            SELECT
                rd.nse_symbol,
                rd."timestamp",
                MAX(CASE WHEN rd.fut_series = 'F1' THEN rd.open END) AS open,
                MAX(CASE WHEN rd.fut_series = 'F1' THEN rd.high END) AS high,
                MAX(CASE WHEN rd.fut_series = 'F1' THEN rd.low END) AS low,
                MAX(CASE WHEN rd.fut_series = 'F1' THEN rd.close END) AS close,
                SUM(rd.volume) AS total_volume,
                SUM(rd.coi) AS total_coi,
                SUM(rd.oi) AS total_oi
            FROM raw_data.nse_stock_fno_data rd
            WHERE ((stock_symbol IS NULL OR rd.nse_symbol = stock_symbol) AND rd."timestamp"::time>'09:15:00' )
            GROUP BY rd.nse_symbol, rd."timestamp"
        ),
        -- Step 1: Daily Data Computation
        daily_volume AS (
            SELECT
                nse_fno.nse_symbol,
                nse_fno."timestamp"::date AS trade_date,
                SUM(nse_fno.total_volume) AS daily_volume,
                MAX(nse_fno.high) AS day_high,
                MIN(nse_fno.low) AS day_low,
                AVG(CASE WHEN nse_fno.total_coi > 0 THEN nse_fno.total_coi ELSE 0 END) AS avg_oi_form,
                AVG(CASE WHEN nse_fno.total_coi < 0 THEN nse_fno.total_coi ELSE 0 END) AS avg_oi_cover
                --SUM(volume) FILTER (WHERE volume > 0) AS daily_volume
            FROM aggregated_table nse_fno
            GROUP BY nse_fno.nse_symbol, nse_fno."timestamp"::date
        ),
        moving_average AS (
            SELECT
                dv.*,
                AVG(dv.daily_volume) OVER (
                    PARTITION BY dv.nse_symbol
                    ORDER BY dv.trade_date
                    ROWS BETWEEN mov_avg_span PRECEDING AND 1 PRECEDING
                ) AS avg_volume_60_days,

                AVG(dv.avg_oi_form) OVER (
                    PARTITION BY dv.nse_symbol
                    ORDER BY dv.trade_date
                    ROWS BETWEEN mov_avg_span PRECEDING AND 1 PRECEDING
                ) AS ma_oi_form,

                AVG(dv.avg_oi_cover) OVER (
                    PARTITION BY dv.nse_symbol
                    ORDER BY dv.trade_date
                    ROWS BETWEEN mov_avg_span PRECEDING AND 1 PRECEDING
                ) AS ma_oi_cover
                
            FROM daily_volume dv
        ),
        shifted_average AS (
            SELECT
                ma.*,
                ma.day_high - ma.day_low AS day_range,
                LAG(ma.avg_volume_60_days) OVER (
                    PARTITION BY ma.nse_symbol
                    ORDER BY ma.trade_date
                ) AS shifted_avg_volume_60_days,
                LAG(ma.ma_oi_form) OVER (
                    PARTITION BY ma.nse_symbol
                    ORDER BY ma.trade_date
                ) AS ma_oi_form_s,
                LAG(ma.ma_oi_cover) OVER (
                    PARTITION BY ma.nse_symbol
                    ORDER BY ma.trade_date
                ) AS ma_oi_cover_s
            FROM moving_average ma
        ),

       
        -- Step 2: Join Daily Data with Minute-wise Data       
        joined_data AS (
            SELECT 
                nse_fno_min.nse_symbol,
                nse_fno_min."timestamp",
                nse_fno_min.open,
                nse_fno_min.high,
                nse_fno_min.low,
                nse_fno_min.close,
                nse_fno_min.total_volume,
                nse_fno_min.total_coi,
                nse_fno_min.total_oi,
                sa.day_high,
                sa.day_low,
                sa.day_range,
                sa.shifted_avg_volume_60_days * 0.01648 AS smavol,
                sa.ma_oi_form_s as mov_avg_oi_form,
                sa.ma_oi_cover_s as mov_avg_oi_cover
                --MAX(nse.high) OVER (PARTITION BY nse.nse_symbol, nse.date) AS day_high,
                --MIN(nse.low) OVER (PARTITION BY nse.nse_symbol, nse.date) AS day_low
                
            FROM aggregated_table nse_fno_min
            LEFT JOIN shifted_average sa
                ON nse_fno_min.nse_symbol = sa.nse_symbol
                AND nse_fno_min."timestamp"::date = sa.trade_date
            WHERE (stock_symbol IS NULL OR nse_fno_min.nse_symbol = stock_symbol)
        ),
        -- Step 3: Compute Minute-wise Features
        stock_with_ratio AS (
            SELECT
                jd.*,
                --jd.volume / NULLIF(jd.smavolume, 0) AS volume_ratio,
                jd.total_volume / NULLIF(jd.smavol, 0) AS volume_ratio,
                ((jd.total_coi*1.0)/NULLIF(jd.total_oi,0))*100.0 AS t_coi_percent
                
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
                sr.close  < ((sr.day_range) * 0.3 + sr.day_low) AS ls,
                sr.close  > ((sr.day_range) * 0.7 + sr.day_low) AS hs,
                sr.close <= ((sr.day_range) * 0.5 + sr.day_low) AS lh,
                sr.close  > ((sr.day_range) * 0.5 + sr.day_low) AS hh
            FROM stock_with_ratio sr
        ),
        process_threshold AS (
            SELECT
                dc.*,
                CASE WHEN dc.total_volume > dc.smavol THEN dc.volume_ratio ELSE 0 END AS f_spk_t_svr,
                CASE WHEN (dc.total_volume > dc.smavol AND dc.ls = TRUE) THEN dc.volume_ratio ELSE 0 END AS f_spk_l_svr,
                CASE WHEN (dc.total_volume > dc.smavol AND dc.ls = FALSE AND dc.hs = FALSE) THEN dc.volume_ratio ELSE 0 END AS f_spk_m_svr,
                CASE WHEN (dc.total_volume > dc.smavol AND dc.hs = TRUE) THEN dc.volume_ratio ELSE 0 END AS f_spk_h_svr,
                CASE WHEN (dc.total_volume > dc.smavol AND dc.lh = TRUE) THEN dc.volume_ratio ELSE 0 END AS vol_l_i,
                CASE WHEN (dc.total_volume > dc.smavol AND dc.hh = TRUE) THEN dc.volume_ratio ELSE 0 END AS vol_h_i,
                CASE WHEN (dc.t_coi_percent>1 AND dc.t_coi_percent<10) THEN dc.t_coi_percent ELSE 0 END as oi_form_t, 
                CASE WHEN (dc.t_coi_percent<-1 AND dc.t_coi_percent>-10) THEN dc.t_coi_percent ELSE 0 END as oi_cover_t
            FROM day_closure dc
        ),
        process_scoi AS (
            SELECT
                pt.*,
                CASE WHEN (pt.oi_form_t != 0 AND pt.ls = TRUE) THEN pt.t_coi_percent ELSE 0 END AS oi_form_l,
                CASE WHEN (pt.oi_form_t != 0 AND pt.ls = FALSE AND pt.hs = FALSE) THEN pt.t_coi_percent ELSE 0 END AS oi_form_m,
                CASE WHEN (pt.oi_form_t != 0 AND pt.hs = TRUE) THEN pt.t_coi_percent ELSE 0 END AS oi_form_h,
                CASE WHEN (pt.oi_cover_t != 0 AND pt.ls = TRUE) THEN pt.t_coi_percent ELSE 0 END AS oi_cover_l,
                CASE WHEN (pt.oi_cover_t != 0 AND pt.ls = FALSE AND pt.hs = FALSE) THEN pt.t_coi_percent ELSE 0 END AS oi_cover_m,
                CASE WHEN (pt.oi_cover_t != 0 AND pt.hs = TRUE) THEN pt.t_coi_percent ELSE 0 END AS oi_cover_h,
                CASE WHEN (EXTRACT(HOUR FROM pt."timestamp") = 15) THEN pt.total_volume ELSE 0 END AS l30_volume_i,
                CASE WHEN (EXTRACT(HOUR FROM pt."timestamp") = 15) THEN pt.oi_form_t ELSE 0 END AS l30_oi_form,
                CASE WHEN (EXTRACT(HOUR FROM pt."timestamp") = 15) THEN pt.oi_cover_t  ELSE 0 END AS l30_oi_cover
            FROM process_threshold pt
        )

        


        /*process_volume_lh AS (
            SELECT 
                ptsvr.*,
                
                
                
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

        )*/






    SELECT * FROM process_scoi; 

    RAISE NOTICE 'Ending function execution: %', clock_timestamp();
END;
$$;


/*
DROP FUNCTION IF EXISTS compute_fno_summary(mov_avg_span integer, stock_symbol character varying);
CREATE OR REPLACE FUNCTION compute_fno_summary(mov_avg_span integer, stock_symbol character varying(50) default NULL)
RETURNS TABLE (
    nse_symbol character varying(50),
    date date,
    f_spk_t_svr numeric,
    f_spk_l_svr numeric,
    f_spk_m_svr numeric,
    f_spk_h_svr numeric,
    f_vol_spk_l numeric,
    f_vol_spk_h numeric,
    t_spk_oi_form numeric,
    t_spk_oi_cover numeric,
    l_spk_oi_form numeric,
    m_spk_oi_form numeric,
    h_spk_oi_form numeric,
    l_spk_oi_cover numeric,
    m_spk_oi_cover numeric,
    h_spk_oi_cover numeric,
    f_l30_volume numeric,
    l30_oi_form numeric,
    l30_oi_cover numeric

   
    
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
        SELECT * FROM compute_fno_features(mov_avg_span, stock_symbol)
    )
    SELECT 
        fd.nse_symbol,  -- Specify table alias
        fd."timestamp"::date,        -- Specify table alias
        SUM(fd.f_spk_t_svr) AS f_spk_t_svr,
        SUM(fd.f_spk_l_svr) AS f_spk_l_svr,
        SUM(fd.f_spk_m_svr) AS f_spk_m_svr,
        SUM(fd.f_spk_h_svr) AS f_spk_h_svr,
        SUM(fd.vol_l_i) AS f_vol_spk_l,
        SUM(fd.vol_h_i) AS f_vol_spk_h,
        SUM(fd.oi_form_t) AS t_spk_oi_form,
        SUM(fd.oi_cover_t) AS t_spk_oi_cover,
        SUM(fd.oi_form_l) AS l_spk_oi_form,
        SUM(fd.oi_form_m) AS m_spk_oi_form,
        SUM(fd.oi_form_h) AS h_spk_oi_form,
        SUM(fd.oi_cover_l) AS l_spk_oi_cover,
        SUM(fd.oi_cover_m) AS m_spk_oi_cover,
        SUM(fd.oi_cover_h) AS h_spk_oi_cover,
        SUM(fd.l30_volume_i) AS f_l30_volume,
        SUM(fd.l30_oi_form) AS l30_oi_form,
        SUM(fd.l30_oi_cover) AS l30_oi_cover 
    FROM feature_data fd
    GROUP BY fd.nse_symbol, fd."timestamp"::date
    ORDER BY fd.nse_symbol, fd."timestamp"::date;

END;
$$; */


