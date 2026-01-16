import duckdb
from pathlib import Path

# Connect
con = duckdb.connect()

# Path to processed data
processed_path = Path("data/processed")

# Read SQL file
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sql_file = Path("sql/analytics.sql").read_text(encoding='utf-8')

# Execute all queries
results = con.execute(sql_file).fetchall()

# Display first query result as exercise
print(results[:5])

# from pathlib import Path
# import duckdb
#
# def main():
#     con = duckdb.connect()
#
#     sql_path = Path("sql/analytics.sql")
#     sql = sql_path.read_text(encoding="utf-8")
#
#     con.execute(sql)
#     print("Analytics queries executed successfully")
#
# if __name__ == "__main__":
#     main()
