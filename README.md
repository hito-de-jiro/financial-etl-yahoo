# 📊 Financial ETL with Apache Spark

[![Python](https://img.shields.io/badge/python-3.11-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/spark-3.5.2-orange?logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A **strong junior Data Engineering portfolio project** that demonstrates a production-style ETL pipeline using **Python**, **PySpark**, and **Yahoo Finance** data.  

The project follows the **Medallion Architecture (Bronze → Silver)** and is fully reproducible locally on **Windows**.

---

## 🚀 Project Goals

* Build a **real-world ETL pipeline**
* Work with **immutable raw CSV data** and **clean Parquet tables**
* Perform **data validation, deduplication, and enrichment**
* Run **SQL analytics** on processed data
* Showcase **production-style project structure** for portfolio

---

## 🧱 Architecture

```text
Yahoo Finance API (CSV)
        ↓
Bronze (raw CSV / Parquet)
        ↓
Silver (cleaned, enriched Parquet)
        ↓
Bronze Layer: raw, append-only CSVs
        ↓
Silver Layer: deduplicated, typed, analytics-ready Parquet tables
        ↓
Partitioned by ticker and year for scalability
```
## 🗂 Project Structure
```aiignore
financial-etl-yahoo/
├── data/
│   ├── raw/
│   └── processed/
├── ingestion/
│   └── yahoo_ingestion.py
├── spark/
│   └── main.py
├── sql/
│   └── run_analytics.py
├── scripts/
│   └── run.py
├── config/
│   └── settings.py
├── data_model/
│   └── schema.md
├── README.md
└── .gitignore
```

## ⚙️ Tech Stack

* **Python 3.11.9**
* **Apache Spark 3.5.2**
* **Pandas / yfinance**
* **DuckDB (local SQL analytics)**
* **Parquet (analytics-ready storage)**

## ▶️ How to Run
### Prerequisites:

* Java 11
* Spark 3.5.2 installed
* Python virtual environment activated

---

### Spark

#### Windows user

1. Download [Apache Spark](https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz)
2. Unpack spark content to `<spark-folder-path>`.
3. Install [AdoptOpenJDK11](https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.15%2B10/OpenJDK11U-jdk_x64_windows_hotspot_11.0.15_10.msi)
4. Download [Hadoop binaries](https://github.com/cdarlint/winutils)
5. Add [environment variables](#environment-variables)
6. Restart your PC
- Debug: Read article [here](https://www.knowledgehut.com/blog/big-data/how-to-install-apache-spark-on-windows)

---

### Environment variables

If you want to run code locally you should define the following environment variables.

Required:
- SPARK_HOME: Path to the Spark application folder. Ex: `<spark-folder-path>`
- HADOOP_HOME: Path to the Hadoop 3.3.6 folder. Ex: `<winutils-folder-path>\hadoop-3.3.6`
- PYSPARK_PYTHON: python
- PATH=%PATH%;%HADOOP_HOME%\bin;%SPARK_HOME%\bin

---
### Run pipeline:
```shell
py run.py all
```
### ... or running a single module:
### 1️⃣ Ingestion
```shell
py run.py ingestion
```
- Downloads CSVs into data/raw/ticker=<TICKER>/data.csv.

---
### 2️⃣ Spark ETL
```shell
py run.py transform
```
- Transforms raw CSV → enriched Parquet in data/processed/.

---
### 3️⃣ Analytics
```shell
py run.py alytics
```
#### Executes SQL queries:
- Average daily return per ticker
- Max daily return per ticker
- Max drawdown per ticker
- Yearly volatility per ticker
- Top 5 highest volume days per ticker

Outputs results to CSV or console.

---
## 📈 Sample Insights
#### Average Daily Return per Ticker

| Ticker | Avg Daily Return |
| ------ | ---------------- |
| AAPL   | 0.12%            |
| MSFT   | 0.09%            |
| GOOGL  | 0.10%            |
| AMZN   | 0.08%            |
| TSLA   | 0.15%            |

#### Max Drawdown Example
#### Daily Return Distribution
- Tip: create these plots using matplotlib/seaborn from processed Parquet files.
---
## 📝 Tables
### Bronze (Raw CSV)
| Column    | Type   | Description          |
| --------- | ------ | -------------------- |
| date      | DATE   | Trading date         |
| open      | DOUBLE | Opening price        |
| high      | DOUBLE | High price           |
| low       | DOUBLE | Low price            |
| close     | DOUBLE | Closing price        |
| adj_close | DOUBLE | Adjusted close price |
| volume    | LONG   | Trading volume       |
| ticker    | STRING | Stock ticker         |

## Silver (Processed Parquet)
| Column       | Type   | Description           |
| ------------ | ------ | --------------------- |
| date         | DATE   | Trading date          |
| open         | DOUBLE | Opening price         |
| high         | DOUBLE | High price            |
| low          | DOUBLE | Low price             |
| close        | DOUBLE | Closing price         |
| adj_close    | DOUBLE | Adjusted close price  |
| volume       | LONG   | Trading volume        |
| ticker       | STRING | Stock ticker          |
| year         | INT    | Year of trading date  |
| month        | INT    | Month of trading date |
| daily_return | DOUBLE | `(close - open)/open` |
---
## 🔮 Future Improvements
- Add timestamped raw CSV versions for historical reproducibility
- Integrate with DuckDB or Postgres for persistent analytics
- Add unit tests for ingestion & ETL
- Extend Medallion architecture with Gold layer for BI
- Automated pipeline orchestration (Airflow / Prefect)
---
## 👤 Author

#### Alex — aspiring Data Engineer
#### Skills showcased:
- PySpark / Spark SQL
- ETL / Data Engineering
- Python backend scripting
- Portfolio-ready project structure

> This project is production-style yet fully local, easy to extend for cloud (AWS / Databricks) or real-time pipelines.
