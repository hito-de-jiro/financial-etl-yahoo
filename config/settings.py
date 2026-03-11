from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

SQL_DIR = BASE_DIR / "sql"

TICKERS = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"]