CREATE TYPE f_type AS ENUM ('F1', 'F2', 'F3');

CREATE TABLE raw_data.nse_stock_fno_data (
    nse_symbol VARCHAR(50) NOT NULL, -- NSE symbol with max 50 characters
    "timestamp" TIMESTAMP WITHOUT TIME ZONE NOT NULL, -- Timestamp column
    open REAL, -- Open price
    high REAL, -- High price
    low REAL, -- Low price
    close REAL, -- Close price
    vwap REAL, -- VWAP
    volume INTEGER, -- Volume
    cum_vol INTEGER, -- Cumulative volume
	coi INTEGER,
	oi INTEGER,
	fut_series f_type NOT NULL,
    PRIMARY KEY (nse_symbol, "timestamp",fut_series), -- Primary key combining nse_symbol, timestamp and fut_series
    CONSTRAINT chk_positive_values CHECK (
        oi >= 0 AND 
        volume >= 0 AND 
        cum_vol >= 0 -- Ensures no negative values
    )
);

CREATE INDEX idx_stock_fno_data_timestamp ON raw_data.nse_stock_fno_data ("timestamp");

-- Index for nse_symbol column
CREATE INDEX idx_stock_fno_data_nse_symbol ON raw_data.nse_stock_fno_data (nse_symbol);


