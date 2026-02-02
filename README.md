# 📊 Financial ETL with Apache Spark & Iceberg

A local **Data Engineering portfolio project** that demonstrates a production‑style ETL pipeline using **Apache Spark**, **Apache Iceberg**, and real financial market data from **Yahoo Finance**.

The project follows the **Medallion Architecture (Bronze → Silver)** and is designed to run **locally on Windows** with Spark 3.5.2.

---

## 🚀 Project Goals

* Demonstrate real‑world **Data Engineering patterns**
* Work with **immutable raw data** and **clean analytical tables**
* Use **Apache Iceberg** for ACID tables, schema evolution, and future scalability
* Show Spark usage beyond toy examples (SQL + DataFrames)

---

## 🧱 Architecture

```
Yahoo Finance API (JSON)
        ↓
     Bronze (raw Iceberg tables)
        ↓
     Silver (clean, deduplicated Iceberg tables)
```

### Bronze Layer

* Raw data from the source
* Append‑only
* Minimal transformations
* Keeps ingestion metadata

### Silver Layer

* Cleaned and typed data
* Deduplicated records
* Ready for analytics and BI

---

## 🗂 Project Structure

```
financial-etl-yahoo/
├── data/
│   └── iceberg/              # Iceberg warehouse (local)
├── spark/
│   └── spark_session.py      # Spark + Iceberg config
├── bronze/
│   └── yahoo_bronze.py       # Yahoo ingestion logic
├── silver/
│   └── yahoo_silver.py       # Bronze → Silver transforms
├── scripts/
│   └── run_pipeline.py       # Pipeline orchestration
├── README.md
```

---

## ⚙️ Tech Stack

* **Python 3.11.9**
* **Apache Spark 3.5.2**
* **Apache Iceberg 1.5.x**
* **PySpark**
* **Yahoo Finance API**

---

## 🔧 Spark & Iceberg Configuration

Iceberg is configured using Spark Catalog with a **local Hadoop warehouse**:

* Catalog name: `iceberg`
* Catalog type: `hadoop`
* Warehouse path: `data/iceberg/`

Spark session configuration is defined in:

```
spark/spark_session.py
```

---

## 🧪 Tables

### Bronze Table

```sql
iceberg.bronze_yahoo_prices
```

| Column      | Type      | Description           |
| ----------- | --------- | --------------------- |
| symbol      | STRING    | Stock ticker          |
| ts          | LONG      | Unix timestamp        |
| open        | DOUBLE    | Opening price         |
| high        | DOUBLE    | High price            |
| low         | DOUBLE    | Low price             |
| close       | DOUBLE    | Closing price         |
| volume      | LONG      | Trading volume        |
| ingested_at | TIMESTAMP | Ingestion timestamp   |
| source      | STRING    | Data source (`yahoo`) |

---

### Silver Table

```sql
iceberg.silver_yahoo_prices
```

| Column      | Type      | Description                |
| ----------- | --------- | -------------------------- |
| symbol      | STRING    | Stock ticker               |
| price_date  | DATE      | Trading date               |
| open        | DOUBLE    | Opening price              |
| high        | DOUBLE    | High price                 |
| low         | DOUBLE    | Low price                  |
| close       | DOUBLE    | Closing price              |
| volume      | LONG      | Trading volume             |
| ingested_at | TIMESTAMP | Latest ingestion timestamp |
| source      | STRING    | Data source                |

---

## ▶️ How to Run

### 1️⃣ Prerequisites

* Java 11
* Spark 3.5.2 installed
* Python virtual environment activated

### Spark

#### Windows user

1. Download [Apache Spark](https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz)
2. Unpack spark content to `<spark-folder-path>`.
3. Install [AdoptOpenJDK11](https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.15%2B10/OpenJDK11U-jdk_x64_windows_hotspot_11.0.15_10.msi)
4. Download [Hadoop binaries](https://github.com/cdarlint/winutils)
5. Add [environment variables](#environment-variables)
6. Restart your PC
- Debug: Read article [here](https://www.knowledgehut.com/blog/big-data/how-to-install-apache-spark-on-windows)

### Environment variables

If you want to run code locally you should define the following environment variables.

Required:
- SPARK_HOME: Path to the Spark application folder. Ex: `<spark-folder-path>`
- HADOOP_HOME: Path to the Hadoop 3.3.6 folder. Ex: `<winutils-folder-path>\hadoop-3.3.6`
- PYSPARK_PYTHON: python
- PATH=%PATH%;%HADOOP_HOME%\bin;%SPARK_HOME%\bin

---

### 2️⃣ Run Pipeline

From the project root:

```powershell
spark-submit `
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 `
  --py-files . `
  scripts\run_pipeline.py
```

---

## ✅ What This Project Demonstrates

* Spark SQL & DataFrame API
* Iceberg catalogs and tables
* Incremental ingestion
* Deduplication using window functions
* Production‑style project structure
* Medallion architecture

---

## 🔮 Future Improvements

* Partitioning by `symbol` and `price_date`
* `MERGE INTO` instead of overwrite
* Time Travel queries
* Gold (analytics) layer
* dbt‑style SQL transformations

---

## 👤 Author

**Alex** — aspiring Data Engineer

Focused on:

* Apache Spark
* Data Engineering
* Backend Python

---

> This project is intentionally designed to be simple but production‑oriented.
> It can be extended to cloud environments (AWS / Databricks) with minimal changes.
