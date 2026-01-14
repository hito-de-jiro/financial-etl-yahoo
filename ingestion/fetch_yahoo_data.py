import logging
import time
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
import yfinance as yf


# --------------------
# Config
# --------------------
RAW_DATA_PATH = Path("../data/raw")
RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)

DEFAULT_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
DATE_FORMAT = "%Y-%m-%d"

MAX_RETRIES = 3
RETRY_DELAY_SEC = 5


# --------------------
# Logging
# --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# --------------------
# Core logic
# --------------------
def fetch_ticker_data(
    ticker: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    Fetch historical market data for a single ticker from Yahoo Finance.
    """
    logger.info(f"Fetching data for ticker={ticker}")

    df = yf.download(
        ticker,
        start=start_date,
        end=end_date,
        progress=False,
        auto_adjust=False
    )

    if df.empty:
        raise ValueError(f"No data returned for ticker={ticker}")

    df.reset_index(inplace=True)
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    df["ticker"] = ticker

    return df


def fetch_with_retry(
    ticker: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    Retry wrapper around Yahoo Finance fetch.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fetch_ticker_data(ticker, start_date, end_date)
        except Exception as exc:
            logger.warning(
                f"Attempt {attempt}/{MAX_RETRIES} failed for {ticker}: {exc}"
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SEC)
            else:
                logger.error(f"All retries failed for ticker={ticker}")
                raise


def save_raw_data(df: pd.DataFrame, ticker: str) -> None:
    """
    Save raw data as CSV partitioned by ticker.
    """
    output_dir = RAW_DATA_PATH / f"ticker={ticker}"
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / "data.csv"
    df.to_csv(file_path, index=False)

    logger.info(f"Saved raw data to {file_path}")


def run_ingestion(
    tickers: List[str],
    start_date: str,
    end_date: str
) -> None:
    """
    Run ingestion for a list of tickers.
    """
    logger.info(
        f"Starting ingestion for tickers={tickers}, "
        f"period={start_date} -> {end_date}"
    )

    for ticker in tickers:
        df = fetch_with_retry(ticker, start_date, end_date)
        save_raw_data(df, ticker)

    logger.info("Ingestion completed successfully")


# --------------------
# Entry point
# --------------------
if __name__ == "__main__":
    START_DATE = "2018-01-01"
    END_DATE = datetime.today().strftime(DATE_FORMAT)

    run_ingestion(
        tickers=DEFAULT_TICKERS,
        start_date=START_DATE,
        end_date=END_DATE
    )
