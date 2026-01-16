-- =====================================================
-- Financial Market Analytics
-- =====================================================

-- 1. Create view
CREATE OR REPLACE VIEW market_data AS
SELECT *
FROM read_parquet('data/processed/**/*.parquet');

-- 1. Average daily return per ticker
SELECT
    ticker,
    AVG(daily_return) AS avg_daily_return
FROM market_data
GROUP BY ticker
ORDER BY avg_daily_return DESC;

-- 2️. Max daily return per ticker
SELECT
    ticker,
    MAX(daily_return) AS max_daily_return
FROM market_data
GROUP BY ticker
ORDER BY max_daily_return DESC;

-- 3️. Max drawdown per ticker (approximation)
WITH daily_prices AS (
    SELECT
        ticker,
        date,
        close,
        MAX(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max
    FROM market_data
)
SELECT
    ticker,
    MIN((close - running_max)/running_max) AS max_drawdown
FROM daily_prices
GROUP BY ticker
ORDER BY max_drawdown;

-- 4️. Yearly volatility per ticker
SELECT
    ticker,
    year,
    STDDEV(daily_return) AS volatility
FROM market_data
GROUP BY ticker, year
ORDER BY ticker, year;

-- 5. Top 5 highest volume days per ticker
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY volume DESC) AS rn
    FROM market_data
) sub
WHERE rn <= 5
ORDER BY ticker, volume DESC;