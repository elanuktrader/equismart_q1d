CREATE TABLE summary.final_summary (
    nse_symbol VARCHAR(50) NOT NULL, -- NSE symbol with max 50 characters
    date DATE NOT NULL, -- Timestamp column
  
    PRIMARY KEY (nse_symbol, date) -- Primary key combining nse_symbol, date
    
);

--CREATE INDEX idx_stock_fno_data_timestamp ON raw_data.nse_stock_fno_data ("timestamp");

-- Index for nse_symbol column
--CREATE INDEX idx_stock_fno_data_nse_symbol ON raw_data.nse_stock_fno_data (nse_symbol);



SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'final_summary';
