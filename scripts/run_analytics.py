from pathlib import Path

import duckdb

from config.settings import SQL_DIR, PROCESSED_DIR

ANALYTICS_DIR = PROCESSED_DIR.parent / "analytics"
ANALYTICS_DIR.mkdir(exist_ok=True)


def main():
    db = duckdb.connect()

    processed_path = PROCESSED_DIR.as_posix()

    db.execute(
        f"""
        CREATE OR REPLACE VIEW market_data AS
        SELECT *
        FROM '{processed_path}/**/*.parquet'
        """
    )

    sql_file = Path(SQL_DIR / "analytics.sql").read_text(encoding="utf-8")

    queries = [q.strip() for q in sql_file.split(";") if q.strip()]

    for i, query in enumerate(queries, start=1):
        result_df = db.execute(query).df()
        if result_df.empty:
            print(f"[Query {i}] Result is empty")
            continue

        csv_path = ANALYTICS_DIR / f"query_{i}.csv"
        result_df.to_csv(csv_path, index=False)
        print(f"[Query {i}] Saved {len(result_df)} rows to {csv_path}")

        print(result_df.head(), "\n")


if __name__ == "__main__":
    main()
