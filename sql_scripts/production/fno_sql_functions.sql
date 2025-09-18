DROP FUNCTION IF EXISTS compute_fno_summary(mov_avg_span integer, stock_symbols TEXT[],fetch_date date,insert_date date);
CREATE OR REPLACE FUNCTION compute_fno_summary(
    mov_avg_span integer, 
    stock_symbols TEXT[] default NULL,
    fetch_date DATE DEFAULT NULL,
    insert_date DATE DEFAULT NULL)
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
    f_l30_volume NUMERIC,
    l30_oi_form REAL,
    l30_oi_cover REAL
) LANGUAGE plpgsql AS
$$
BEGIN
    RETURN QUERY
    WITH feature_data AS (
        SELECT * FROM compute_fno_features(mov_avg_span, stock_symbols,fetch_date)
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
        ROUND(SUM(fd.l30_volume_i),0) AS f_l30_volume,
        ROUND(SUM(fd.l30_oi_form), 2)::REAL AS l30_oi_form,
        ROUND(SUM(fd.l30_oi_cover), 2)::REAL AS l30_oi_cover 
    FROM feature_data fd
    where (fd."timestamp"::date >= insert_date or  insert_date is NULL)
    GROUP BY fd.nse_symbol, fd."timestamp"::date
    ORDER BY fd.nse_symbol, fd."timestamp"::date;
END;
$$;



DROP FUNCTION IF EXISTS compute_fno_features(integer, TEXT[],date);
CREATE OR REPLACE FUNCTION compute_fno_features(
    mov_avg_span INTEGER, 
    stock_symbols TEXT[] DEFAULT NULL,
    fetch_date DATE DEFAULT NULL)
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
            WHERE ((stock_symbols IS NULL OR rd.nse_symbol = ANY(stock_symbols)) AND rd."timestamp"::time>'09:15:00' AND (rd."timestamp"::date>=fetch_date or fetch_date is NULL ) )
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
            WHERE ((stock_symbols IS NULL OR nse_fno_min.nse_symbol = ANY(stock_symbols)) AND (nse_fno_min."timestamp"::date>=fetch_date or fetch_date is NULL ))
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

        


       
    SELECT * FROM process_scoi; 

    RAISE NOTICE 'Ending function execution: %', clock_timestamp();
END;
$$;




