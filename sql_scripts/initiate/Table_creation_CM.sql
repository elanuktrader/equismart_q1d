CREATE TABLE raw_data.nse_stock_cm_data (
    nse_symbol VARCHAR(50) NOT NULL, -- NSE symbol with max 50 characters
    "timestamp" TIMESTAMP WITHOUT TIME ZONE NOT NULL, -- Timestamp column
    open REAL, -- Open price
    high REAL, -- High price
    low REAL, -- Low price
    close REAL, -- Close price
    vwap REAL, -- VWAP
    volume INTEGER, -- Volume
    cum_vol INTEGER, -- Cumulative volume
    PRIMARY KEY (nse_symbol, "timestamp"), -- Primary key combining nse_symbol and timestamp
    CONSTRAINT chk_positive_values CHECK (
        open > 0 AND 
        high > 0 AND 
        low > 0 AND 
        close > 0 AND 
        vwap >= 0 AND 
        volume >= 0 AND 
        cum_vol >= 0 -- Ensures no negative values
    )
);

CREATE INDEX idx_stock_cm_data_timestamp ON raw_data.nse_stock_cm_data ("timestamp");

-- Index for nse_symbol column
CREATE INDEX idx_stock_cm_data_nse_symbol ON raw_data.nse_stock_cm_data (nse_symbol);




