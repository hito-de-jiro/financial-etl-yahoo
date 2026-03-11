from pathlib import Path

import duckdb


def main():
    con = duckdb.connect()

    sql_path = Path("../sql/analytics.sql")
    sql = sql_path.read_text(encoding="utf-8")

    # Execute all queries
    results = con.execute(sql).fetchall()
    print(results[:5])
    print("Analytics queries executed successfully")


if __name__ == "__main__":
    main()
