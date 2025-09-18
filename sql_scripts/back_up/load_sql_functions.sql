CREATE OR REPLACE FUNCTION volume_data_processing_sma(
    mov_avg_span integer,
    stock_symbol character varying(50) DEFAULT NULL
) RETURNS TABLE (
    nse_symbol character varying(50),
    "timestamp" timestamp,
    date date,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume integer,
    smavolume numeric,
    volume_ratio numeric,
    smavol_ratio numeric,
    ls boolean,
    hs boolean,
    lh boolean,
    hh boolean
) LANGUAGE plpgsql AS
$$
BEGIN
    RAISE NOTICE 'Starting function execution: %', clock_timestamp();
    RETURN QUERY
    WITH stock_data AS (
        SELECT
            nse.nse_symbol,
            nse."timestamp",
            nse.date,
            nse.open,
            nse.high,
            nse.low,
            nse.close,
            nse.volume,
            AVG(nse.volume) OVER (PARTITION BY nse.nse_symbol ORDER BY nse."timestamp" ROWS BETWEEN mov_avg_span PRECEDING AND CURRENT ROW) AS smavolume,
            MAX(nse.high) OVER (PARTITION BY nse.date) AS day_high,
            MIN(nse.low) OVER (PARTITION BY nse.date) AS day_low
        FROM raw_data.nse_stock_data_non_fno nse
        WHERE stock_symbol IS NULL OR nse.nse_symbol = stock_symbol  -- Filter for specific symbol if provided
    )
    SELECT
        sd.nse_symbol,
        sd."timestamp",
        sd.date,
        sd.open,
        sd.high,
        sd.low,
        sd.close,
        sd.volume,
        sd.smavolume,
        ROUND(sd.volume / sd.smavolume, 4) AS volume_ratio,
        ROUND(AVG(ROUND(sd.volume / sd.smavolume, 4)) OVER (PARTITION BY sd.nse_symbol ORDER BY sd."timestamp" ROWS BETWEEN mov_avg_span PRECEDING AND CURRENT ROW), 4) AS smavol_ratio,
        sd.close < (sd.day_high - sd.day_low) * 0.3 + sd.day_low AS ls,
        sd.close > (sd.day_high - sd.day_low) * 0.7 + sd.day_low AS hs,
        sd.close <= (sd.day_high - sd.day_low) * 0.5 + sd.day_low AS lh,
        sd.close > (sd.day_high - sd.day_low) * 0.5 + sd.day_low AS hh
    FROM stock_data sd;
    RAISE NOTICE 'Ending function execution: %', clock_timestamp();
END;
$$;



CREATE OR REPLACE FUNCTION volume_features(
    mov_avg_span integer,
    stock_symbol character varying(50) DEFAULT NULL
) RETURNS TABLE (
    nse_symbol character varying(50),
    "timestamp" timestamp,
    date date,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume integer,
    smavolume numeric,
    volume_ratio numeric,
    smavol_ratio numeric,
    day_range numeric,
    ls boolean,
    hs boolean,
    lh boolean,
    hh boolean
) LANGUAGE plpgsql AS
$$
BEGIN
    RAISE NOTICE 'Starting function execution: %', clock_timestamp();
    RETURN QUERY
    WITH stock_data AS (
        SELECT
            nse.nse_symbol,
            nse."timestamp",
            nse.date,
            nse.open,
            nse.high,
            nse.low,
            nse.close,
            nse.volume,
            AVG(nse.volume) OVER (
                PARTITION BY nse.nse_symbol 
                ORDER BY nse."timestamp" 
                ROWS BETWEEN mov_avg_span PRECEDING AND CURRENT ROW
            ) AS smavolume,
            MAX(nse.high) OVER (PARTITION BY nse.nse_symbol, nse.date) AS day_high,
            MIN(nse.low) OVER (PARTITION BY nse.nse_symbol, nse.date) AS day_low
        FROM raw_data.nse_stock_data_non_fno nse
        WHERE stock_symbol IS NULL OR nse.nse_symbol = stock_symbol
    ),
    stock_with_ratio AS (
        SELECT
            sd.*,
            ROUND(sd.volume / sd.smavolume, 4) AS volume_ratio
            sd.day_high - sd.day_low AS day_range,
        FROM stock_data sd
    )
    SELECT
        sr.nse_symbol,
        sr."timestamp",
        sr.date,
        sr.open,
        sr.high,
        sr.low,
        sr.close,
        sr.volume,
        sr.smavolume,
        sr.volume_ratio,
        sr.day_range
        
        -- Now use the precomputed volume_ratio for smavol_ratio calculation
        ROUND(AVG(sr.volume_ratio) OVER (
            PARTITION BY sr.nse_symbol 
            ORDER BY sr."timestamp" 
            ROWS BETWEEN mov_avg_span PRECEDING AND CURRENT ROW
        ), 4) AS smavol_ratio,
        

        
        sr.close < (sr.day_range) * 0.3 + sr.day_low AS ls,
        sr.close > (sr.day_range) * 0.7 + sr.day_low AS hs,
        sr.close <= (sr.day_range) * 0.5 + sr.day_low AS lh,
        sr.close > (sr.day_range) * 0.5 + sr.day_low AS hh
    FROM stock_with_ratio sr;
    
    RAISE NOTICE 'Ending function execution: %', clock_timestamp();
END;
$$;


CREATE OR REPLACE FUNCTION compute_all_features(
    mov_avg_span INTEGER,
    scrip_symbol VARCHAR(50) DEFAULT NULL,
    range_thrs NUMERIC
)
RETURNS TABLE (
    nse_symbol VARCHAR(50),
    "timestamp" TIMESTAMP,
    date DATE,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume INTEGER,
    smavolume NUMERIC,
    volume_ratio NUMERIC,
    emavol_ratio NUMERIC,
    day_high NUMERIC,
    day_low NUMERIC,
    range NUMERIC,
    ls BOOLEAN,
    hs BOOLEAN,
    lh BOOLEAN,
    hh BOOLEAN,
    spk_t_vr INTEGER,
    spk_l_vr INTEGER,
    spk_m_vr INTEGER,
    spk_h_vr INTEGER,
    spk_t_svr NUMERIC,
    spk_l_svr NUMERIC,
    spk_m_svr NUMERIC,
    spk_h_svr NUMERIC,
    range_m NUMERIC,
    sr_lv INTEGER,
    srlv_time TEXT,
    low_sr_lv INTEGER,
    low_sr_lv_time TEXT,
    low_sr_lv_close NUMERIC,
    high_sr_lv INTEGER,
    high_sr_lv_time TEXT,
    high_sr_lv_close NUMERIC,
    med_sr_lv INTEGER,
    med_sr_lv_close NUMERIC,
    spk_t_l30 INTEGER,
    sr_lv_l30 INTEGER,
    srlv_volume_i NUMERIC,
    l30_volume_i NUMERIC
)
LANGUAGE plpgsql AS
$$
BEGIN
    RETURN QUERY
    WITH
    -- Step 1: Base Volume Processing
    stock_data AS (
        SELECT
            nse.nse_symbol,
            nse."timestamp",
            nse.date,
            nse.open,
            nse.high,
            nse.low,
            nse.close,
            nse.volume,
            AVG(nse.volume) OVER (
                PARTITION BY nse.nse_symbol 
                ORDER BY nse."timestamp" 
                ROWS BETWEEN mov_avg_span PRECEDING AND CURRENT ROW
            ) AS smavolume,
            MAX(nse.high) OVER (PARTITION BY nse.nse_symbol, nse.date) AS day_high,
            MIN(nse.low) OVER (PARTITION BY nse.nse_symbol, nse.date) AS day_low
        FROM raw_data.nse_stock_data_non_fno nse
        WHERE scrip_symbol IS NULL OR nse.nse_symbol = scrip_symbol
    ),
    -- Step 2: Compute Volume_Ratio, EMAVol_Ratio, Range, LS, HS, LH, HH
    volume_ratios AS (
        SELECT
            sd.*,
            ROUND(sd.volume / NULLIF(sd.smavolume, 0), 4) AS volume_ratio,
            ROUND(
                AVG(ROUND(sd.volume / NULLIF(sd.smavolume, 0), 4)) OVER (
                    PARTITION BY sd.nse_symbol 
                    ORDER BY sd."timestamp" 
                    ROWS BETWEEN mov_avg_span PRECEDING AND CURRENT ROW
                ) * 3.82, 4
            ) AS emavol_ratio,
            sd.day_high - sd.day_low AS range,
            sd.close < (sd.day_high - sd.day_low) * 0.3 + sd.day_low AS ls,
            sd.close > (sd.day_high - sd.day_low) * 0.7 + sd.day_low AS hs,
            sd.close <= (sd.day_high - sd.day_low) * 0.5 + sd.day_low AS lh,
            sd.close > (sd.day_high - sd.day_low) * 0.5 + sd.day_low AS hh
        FROM stock_data sd
    ),
    -- Step 3: Compute Threshold Features VR and SVR
    threshold_features AS (
        SELECT
            vr.*,
            -- Process_Threshold_VR
            CASE WHEN vr.volume_ratio > vr.emavol_ratio THEN 1 ELSE 0 END AS spk_t_vr,
            CASE WHEN vr.volume_ratio > vr.emavol_ratio AND vr.ls THEN 1 ELSE 0 END AS spk_l_vr,
            CASE WHEN vr.volume_ratio > vr.emavol_ratio AND NOT vr.ls AND NOT vr.hs THEN 1 ELSE 0 END AS spk_m_vr,
            CASE WHEN vr.volume_ratio > vr.emavol_ratio AND vr.hs THEN 1 ELSE 0 END AS spk_h_vr,
            -- Process_Threshold_SVR
            CASE WHEN vr.volume_ratio > vr.emavol_ratio THEN ROUND(vr.volume_ratio, 2) ELSE 0 END AS spk_t_svr,
            CASE WHEN vr.volume_ratio > vr.emavol_ratio AND vr.ls THEN ROUND(vr.volume_ratio, 2) ELSE 0 END AS spk_l_svr,
            CASE WHEN vr.volume_ratio > vr.emavol_ratio AND NOT vr.ls AND NOT vr.hs THEN ROUND(vr.volume_ratio, 2) ELSE 0 END AS spk_m_svr,
            CASE WHEN vr.volume_ratio > vr.emavol_ratio AND vr.hs THEN ROUND(vr.volume_ratio, 2) ELSE 0 END AS spk_h_svr
        FROM volume_ratios vr
    ),
    -- Step 4: Compute Range_m and related features
    range_volume_features AS (
        SELECT
            tf.*,
            ROUND(((tf.high - tf.low) / NULLIF((tf.high + tf.low)/2, 0)) * 100, 2) AS range_m,
            -- SR_LV
            CASE 
                WHEN tf.spk_t_vr = 1 AND ROUND(((tf.high - tf.low)/NULLIF((tf.high + tf.low)/2, 0))*100, 2) < range_thrs THEN 1 
                ELSE 0 
            END AS sr_lv,
            -- SRLV_Time
            CASE 
                WHEN (CASE 
                        WHEN tf.spk_t_vr = 1 AND ROUND(((tf.high - tf.low)/NULLIF((tf.high + tf.low)/2, 0))*100, 2) < range_thrs 
                      THEN 1 ELSE 0 
                      END) = 1 
                THEN TO_CHAR(tf."timestamp", 'HH24:MI') 
                ELSE '' 
            END AS srlv_time,
            -- Low_SR_LV
            CASE 
                WHEN tf.spk_l_vr = 1 AND ROUND(((tf.high - tf.low)/NULLIF((tf.high + tf.low)/2, 0))*100, 2) < range_thrs THEN 1 
                ELSE 0 
            END AS low_sr_lv,
            -- Low_SR_LV_Time
            CASE 
                WHEN (CASE 
                        WHEN tf.spk_l_vr = 1 AND ROUND(((tf.high - tf.low)/NULLIF((tf.high + tf.low)/2, 0))*100, 2) < range_thrs 
                      THEN 1 ELSE 0 
                      END) = 1 
                THEN srlv_time 
                ELSE '' 
            END AS low_sr_lv_time,
            -- Low_SR_LV_Close
            CASE 
                WHEN tf.low_sr_lv = 1 THEN tf.close 
                ELSE 0.0 
            END AS low_sr_lv_close,
            -- High_SR_LV
            CASE 
                WHEN tf.spk_h_vr = 1 AND ROUND(((tf.high - tf.low)/NULLIF((tf.high + tf.low)/2, 0))*100, 2) < range_thrs THEN 1 
                ELSE 0 
            END AS high_sr_lv,
            -- High_SR_LV_Time
            CASE 
                WHEN (CASE 
                        WHEN tf.spk_h_vr = 1 AND ROUND(((tf.high - tf.low)/NULLIF((tf.high + tf.low)/2, 0))*100, 2) < range_thrs 
                      THEN 1 ELSE 0 
                      END) = 1 
                THEN srlv_time 
                ELSE '' 
            END AS high_sr_lv_time,
            -- High_SR_LV_Close
            CASE 
                WHEN tf.high_sr_lv = 1 THEN tf.close 
                ELSE 0.0 
            END AS high_sr_lv_close,
            -- Med_SR_LV
            CASE 
                WHEN tf.sr_lv = 1 AND tf.high_sr_lv = 0 AND tf.low_sr_lv = 0 THEN 1 
                ELSE 0 
            END AS med_sr_lv,
            -- Med_SR_LV_Close
            CASE 
                WHEN tf.med_sr_lv = 1 THEN tf.close 
                ELSE 0.0 
            END AS med_sr_lv_close,
            -- Spk_T_L30
            CASE 
                WHEN tf.spk_t_vr = 1 AND EXTRACT(HOUR FROM tf."timestamp") = 15 THEN 1 
                ELSE 0 
            END AS spk_t_l30,
            -- SR_LV_L30
            CASE 
                WHEN tf.sr_lv = 1 AND EXTRACT(HOUR FROM tf."timestamp") = 15 THEN 1 
                ELSE 0 
            END AS sr_lv_l30,
            -- SRLV_Volume_I
            CASE 
                WHEN tf.sr_lv = 1 THEN tf.volume 
                ELSE 0 
            END AS srlv_volume_i,
            -- L30_Volume_I
            CASE 
                WHEN EXTRACT(HOUR FROM tf."timestamp") = 15 THEN tf.volume 
                ELSE 0 
            END AS l30_volume_i
        FROM threshold_features tf
    )
    SELECT
        nse_symbol,
        "timestamp",
        date,
        open,
        high,
        low,
        close,
        volume,
        smavolume,
        volume_ratio,
        emavol_ratio,
        day_high,
        day_low,
        range,
        ls,
        hs,
        lh,
        hh,
        spk_t_vr,
        spk_l_vr,
        spk_m_vr,
        spk_h_vr,
        spk_t_svr,
        spk_l_svr,
        spk_m_svr,
        spk_h_svr,
        range_m,
        sr_lv,
        srlv_time,
        low_sr_lv,
        low_sr_lv_time,
        low_sr_lv_close,
        high_sr_lv,
        high_sr_lv_time,
        high_sr_lv_close,
        med_sr_lv,
        med_sr_lv_close,
        spk_t_l30,
        sr_lv_l30,
        srlv_volume_i,
        l30_volume_i
    FROM range_volume_features;
END;
$$ LANGUAGE plpgsql;


