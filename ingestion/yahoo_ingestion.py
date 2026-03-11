import logging
import time
from datetime import datetime
from typing import List

import pandas as pd
import yfinance as yf

from config.settings import RAW_DIR, TICKERS

# --------------------
# Config
# --------------------
RAW_DIR.mkdir(parents=True, exist_ok=True)
DATE_FORMAT = "%Y-%m-%d"

MAX_RETRIES = 3
RETRY_DELAY_SEC = 5

START_DATE = "2018-01-01"
END_DATE = datetime.today().strftime(DATE_FORMAT)

# --------------------
# Logging
# --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# --------------------
# Helpers
# --------------------
def normalize_column(col) -> str:
    """
    Normalize column names from yfinance.
    Handles both string and tuple (MultiIndex) columns.
    """
    if isinstance(col, tuple):
        col = col[0]
    return col.lower().replace(" ", "_")


def fetch_ticker_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch historical market data for a single ticker from Yahoo Finance.
    Raises ValueError if no data is returned.
    """
    logger.info(f"[INGESTION] Fetching data for ticker={ticker}")

    df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=False)
    if df.empty:
        raise ValueError(f"No data returned for ticker={ticker}")

    df.reset_index(inplace=True)
    df.columns = [normalize_column(col) for col in df.columns]
    df["ticker"] = ticker
    return df


def fetch_with_retry(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Retry wrapper around fetch_ticker_data with configurable retries.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fetch_ticker_data(ticker, start_date, end_date)
        except Exception as exc:
            logger.warning(f"[INGESTION] Attempt {attempt}/{MAX_RETRIES} failed for {ticker}: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SEC)
            else:
                logger.error(f"[INGESTION] All retries failed for ticker={ticker}")
                raise


def save_raw_data(df: pd.DataFrame, ticker: str) -> None:
    """
    Save raw CSV file partitioned by ticker in data/raw/ticker=<ticker>/data.csv
    """
    output_dir = RAW_DIR / f"ticker={ticker}"
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / "data.csv"
    df.to_csv(file_path, index=False)
    logger.info(f"[INGESTION] Saved raw data to {file_path}")


# --------------------
# Main ETL logic
# --------------------
def run_ingestion(tickers: List[str], start_date: str, end_date: str) -> None:
    """
    Run ingestion for a list of tickers within a date range.
    """
    logger.info(f"[INGESTION] Starting ingestion for tickers={tickers}, period={start_date} -> {end_date}")
    for ticker in tickers:
        df = fetch_with_retry(ticker, start_date, end_date)
        save_raw_data(df, ticker)
    logger.info("[INGESTION] Ingestion completed successfully")


def main():
    run_ingestion(
        tickers=TICKERS,
        start_date=START_DATE,
        end_date=END_DATE
    )


# --------------------
# Entry point
# --------------------
if __name__ == "__main__":
    main()
