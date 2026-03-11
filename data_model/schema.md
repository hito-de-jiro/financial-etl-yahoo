# Data Model

This document describes the data schema used in the Financial ETL Yahoo project.

The project uses a simple data lake structure with three layers:

```
data/
 ├── raw/
 ├── processed/
 └── analytics/
 ```

Data is collected from Yahoo Finance, transformed using Apache Spark,
and queried using DuckDB.

---

## Raw Layer

Source: Yahoo Finance API

Format: CSV

Each ticker is stored in a separate folder.

Example path:

data/raw/ticker=AAPL/data.csv

Columns:

| Column | Type | Description |
|--------|--------|------------|
| Date | string | Trading date |
| Open | float | Opening price |
| High | float | Highest price |
| Low | float | Lowest price |
| Close | float | Closing price |
| Volume | integer | Trading volume |

Notes:

- Raw data is stored as received from API
- No transformations applied
- Used as ingestion layer

---

## Processed Layer

Engine: Apache Spark 3.5.2  
Format: Parquet

Path:

data/processed/

Schema:

| Column | Type | Description |
|--------|--------|------------|
| ticker | string | Stock ticker symbol |
| date | date | Trading date |
| open | double | Opening price |
| high | double | Highest price |
| low | double | Lowest price |
| close | double | Closing price |
| volume | long | Trading volume |

Transformations:

- Column names normalized to lowercase
- Date converted to date type
- Duplicates removed using (ticker, date)
- Data converted to Parquet

Partitioning:

partitioned by ticker

Example output:

data/processed/ticker=AAPL/part-0000.parquet

---

## Analytics Layer

Engine: DuckDB

Source:

data/processed/

Used for SQL analytics queries.

Example queries:

- average price per ticker
- max close price
- total volume per ticker
- daily statistics

---

## Data Quality Rules

- ticker must not be null
- date must not be null
- volume >= 0
- prices >= 0

Duplicates rule:

duplicate rows removed by

ticker + date

---

## Future Improvements

Possible production improvements:

- add partition by date
- use Delta Lake / Iceberg
- add Airflow orchestration
- add data validation
- store data in S3
- add dbt models