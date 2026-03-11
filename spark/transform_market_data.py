import glob

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, DateType, DoubleType, LongType, StringType
)

from config.settings import RAW_DIR, PROCESSED_DIR


def create_spark_session() -> SparkSession:
    """
    Initialize Spark session with UTC timezone for financial data processing.
    """
    spark = (
        SparkSession.builder
        .appName("FinancialMarketETL")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    return spark


def read_raw_data(spark: SparkSession) -> DataFrame:
    """
    Read raw CSV files from data/raw using predefined schema.
    Raises FileNotFoundError if no CSVs found.
    """
    csv_files = glob.glob(str(RAW_DIR / "ticker=*/*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {RAW_DIR}")

    # Define schema for raw market data
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

    df = (
        spark.read
        .option("header", True)
        .schema(market_schema)
        .csv(csv_files)
    )
    return df


def validate_data(df: DataFrame) -> DataFrame:
    """
    Basic validation: remove rows with missing date or ticker.
    """
    df = df.filter(F.col("date").isNotNull())
    df = df.filter(F.col("ticker").isNotNull())
    return df


def deduplicate_data(df: DataFrame) -> DataFrame:
    """
    Deduplicate data by ticker and date to ensure unique daily entries.
    """
    return df.dropDuplicates(["ticker", "date"])


def enrich_data(df: DataFrame) -> DataFrame:
    """
    Add calculated fields:
    - year, month
    - daily_return = (close - open)/open
    """
    df = (
        df
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("daily_return", (F.col("close") - F.col("open")) / F.col("open"))
    )
    return df


def write_processed(df: DataFrame):
    """
    Write processed data to Parquet, partitioned by ticker and year.
    Overwrites existing data for idempotency.
    """
    PROCESSED_DIR.mkdir(exist_ok=True, parents=True)
    df.write.mode("overwrite").partitionBy("ticker", "year").parquet(str(PROCESSED_DIR))
    print(f"[ETL] Processed data written to {PROCESSED_DIR}")


def main():
    """
    Main ETL pipeline: read -> validate -> deduplicate -> enrich -> write.
    """
    spark = create_spark_session()

    try:
        raw_df = read_raw_data(spark)
        validated_df = validate_data(raw_df)
        dedup_df = deduplicate_data(validated_df)
        enriched_df = enrich_data(dedup_df)
        write_processed(enriched_df)
        print("[ETL] Transform completed successfully")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
