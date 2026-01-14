import glob
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    DoubleType,
    LongType,
    StringType,
)

# --------------------
# Paths
# --------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_PATH = PROJECT_ROOT / "data/raw"

PROCESSED_PATH = str(PROJECT_ROOT / "data/processed")

csv_files = glob.glob(str(RAW_PATH / "ticker=*/*.csv"))
if not csv_files:
    raise FileNotFoundError(f"No CSV files found in {RAW_PATH}")
# --------------------
# Spark session
# --------------------
spark = (
    SparkSession.builder
    .appName("FinancialMarketETL")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# --------------------
# Schema
# --------------------
market_schema = StructType([
    StructField("date", DateType(), nullable=False),
    StructField("open", DoubleType(), nullable=True),
    StructField("high", DoubleType(), nullable=True),
    StructField("low", DoubleType(), nullable=True),
    StructField("close", DoubleType(), nullable=True),
    StructField("adj_close", DoubleType(), nullable=True),
    StructField("volume", LongType(), nullable=True),
    StructField("ticker", StringType(), nullable=False),
])

# --------------------
# Read raw data
# --------------------
raw_df = (
    spark.read
    .option("header", "true")
    .schema(market_schema)
    .csv(csv_files)
)

# --------------------
# Basic validation
# --------------------
raw_df = raw_df.filter(F.col("date").isNotNull())
raw_df = raw_df.filter(F.col("ticker").isNotNull())

# --------------------
# Deduplication
# --------------------
dedup_df = raw_df.dropDuplicates(["ticker", "date"])

# --------------------
# Enrichments
# --------------------
enriched_df = (
    dedup_df
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn(
        "daily_return",
        (F.col("close") - F.col("open")) / F.col("open")
    )
)

# --------------------
# Write processed data
# --------------------
(
    enriched_df
    .write
    .mode("overwrite")
    .partitionBy("ticker", "year")
    .parquet(PROCESSED_PATH)
)

spark.stop()
