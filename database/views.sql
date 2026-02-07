-- View for latest prices
CREATE OR REPLACE VIEW received_data AS
SELECT DISTINCT ON (symbol) symbol, price_usd, fetched_at
FROM crypto_prices
ORDER BY symbol, fetched_at DESC;

-- View for price history with rolling average (example)
CREATE OR REPLACE VIEW price_history_with_ma AS
SELECT 
    symbol,
    fetched_at,
    price_usd,
    AVG(price_usd) OVER (
        PARTITION BY symbol 
        ORDER BY fetched_at 
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) as moving_avg_last_5_points
FROM crypto_prices;
