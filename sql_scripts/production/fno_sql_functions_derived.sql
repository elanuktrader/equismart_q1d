DROP FUNCTION IF EXISTS compute_derived_fno_summary(sd_rolling integer, stock_symbol character varying,fetch_date date,insert_date date);
CREATE OR REPLACE FUNCTION compute_derived_fno_summary(
    sd_rolling integer,
    stock_symbol character varying(50) default NULL,
    fetch_date DATE DEFAULT NULL,
    insert_date DATE DEFAULT NULL)

RETURNS TABLE (
    nse_symbol character varying(50),
    date date,
    avg_f_spk_t_svr float,
    sd_f_spk_t_svr float,
    avg_f_l30_volume numeric,
    sd_f_l30_volume numeric


    
) LANGUAGE plpgsql AS
$$
BEGIN
    RETURN QUERY
    WITH

        delta_summary_table AS(
            SELECT
                st.nse_symbol,
                st.date,
                AVG(st.f_spk_t_svr) OVER (
                    PARTITION BY st.nse_symbol
                    ORDER BY st.date
                    ROWS BETWEEN sd_rolling PRECEDING AND CURRENT ROW) as avg_f_spk_t_svr,
                STDDEV(st.f_spk_t_svr) OVER (
                    PARTITION BY st.nse_symbol
                    ORDER BY st.date
                    ROWS BETWEEN sd_rolling PRECEDING AND CURRENT ROW) as sd_f_spk_t_svr,

                AVG(st.f_l30_volume) OVER (
                    PARTITION BY st.nse_symbol
                    ORDER BY st.date
                    ROWS BETWEEN sd_rolling PRECEDING AND CURRENT ROW) as avg_f_l30_volume,
                STDDEV(st.f_l30_volume) OVER (
                    PARTITION BY st.nse_symbol
                    ORDER BY st.date
                    ROWS BETWEEN sd_rolling PRECEDING AND CURRENT ROW) as sd_f_l30_volume
                
                
              
            FROM summary.final_summary st
            WHERE ((st.nse_symbol = stock_symbol OR stock_symbol is NULL) AND (st.date >= fetch_date or fetch_date is NULL))
        )

    SELECT * from delta_summary_table;
END;
$$;

  
  
  
  
  
  /*st.f_spk_t_svr,
                st.t_spk_oi_form,
                st.t_spk_oi_cover,
                st.f_l30_volume,
                st.l30_oi_form,
                st.l30_oi_cover
                AVG(L30_Volume) OVER (
            PARTITION BY nse_symbol
            ORDER BY date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS avg_L30_Volume,
        STDDEV(L30_Volume) OVER (
            PARTITION BY nse_symbol
            ORDER BY date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS stddev_L30_Volume
    FROM raw_data_table

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



        )

)*/

