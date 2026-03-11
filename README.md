# Financial ETL Yahoo

Simple Data Engineering project that demonstrates ETL pipeline using Python, Spark, and DuckDB.

## Overview

This project loads financial market data from Yahoo Finance, stores raw CSV files, transforms them using Apache Spark, and runs analytics queries using DuckDB.

Pipeline:

Yahoo Finance API → Ingestion → Raw CSV → Spark → Parquet → DuckDB → SQL analytics

## Architecture

```
+------------------+
| Yahoo Finance API|
+--------+---------+
         |
         v
+------------------+
| Python Ingestion |
+--------+---------+
         |
         v
+------------------+
|   data/raw CSV   |
+--------+---------+
         |
         v
+------------------+
|  Spark Transform |
+--------+---------+
         |
         v
+------------------+
| data/processed   |
|    Parquet       |
+--------+---------+
         |
         v
+------------------+
|     DuckDB       |
+--------+---------+
         |
         v
+------------------+
|  analytics.sql   |
+------------------+
```

## Project structure

financial-etl-yahoo/

* data/raw – raw CSV data
* data/processed – parquet data
* ingestion/ – data ingestion scripts
* spark/ – Spark transformations
* scripts/ – analytics runners
* sql/ – SQL queries
* data_model/ – schema documentation

## Technologies

* Python 3.11
* Apache Spark 3.5.2
* DuckDB
* Pandas
* Parquet

## How to run

1. Run ingestion

python ingestion/yahoo_ingestion.py

2. Run Spark transform

spark-submit spark/transform_market_data.py

3. Run analytics

python scripts/run_analytics.py

## Data model

See data_model/schema.md

## Notes

This project is for portfolio purposes and demonstrates basic Data Engineering workflow:

* ingestion layer
* processed layer
* analytics layer
* schema documentation
* SQL analytics
