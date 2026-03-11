-- =====================================================
-- Financial Market Analytics
-- =====================================================

-- 1. Average daily return per ticker
SELECT
    ticker,
    AVG(daily_return) AS avg_daily_return
FROM market_data
GROUP BY ticker
ORDER BY avg_daily_return DESC;


-- 2. Max daily return per ticker
SELECT
    ticker,
    MAX(daily_return) AS max_daily_return
FROM market_data
GROUP BY ticker
ORDER BY max_daily_return DESC;


-- 3. Max drawdown per ticker
WITH daily_prices AS (
    SELECT
        ticker,
        CAST(date AS DATE) AS date,
        TRY_CAST(close AS DOUBLE) AS close,  -- <- safe cast, NULL if fails
        MAX(TRY_CAST(close AS DOUBLE)) OVER (
            PARTITION BY ticker
            ORDER BY CAST(date AS DATE)
        ) AS running_max
    FROM market_data
    WHERE TRY_CAST(close AS DOUBLE) IS NOT NULL
)

SELECT
    ticker,
    MIN((close - running_max) / running_max) AS max_drawdown
FROM daily_prices
GROUP BY ticker
ORDER BY max_drawdown;


-- 4. Yearly volatility
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
           ROW_NUMBER() OVER (
               PARTITION BY ticker
               ORDER BY volume DESC
           ) AS rn
    FROM market_data
) t
WHERE rn <= 5
ORDER BY ticker, volume DESC;