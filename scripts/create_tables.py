import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from spark.spark_session import get_spark
spark = get_spark()

spark.sql("""
CREATE TABLE IF NOT EXISTS iceberg.prices (
    symbol STRING,
    date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT
)
USING iceberg
PARTITIONED BY (days(date))
""")

spark.sql("SHOW TABLES IN iceberg").show()
