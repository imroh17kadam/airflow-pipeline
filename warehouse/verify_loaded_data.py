import duckdb

con = duckdb.connect("warehouse/cola_warehouse.duckdb")

# Example: check first 5 rows of sales
print(con.execute("SELECT * FROM raw.sales_daily LIMIT 5").fetchdf())

# Check row counts
tables = ["sales_daily", "marketing_spend_daily", "distribution_metrics_daily", "calendar_features"]
for t in tables:
    count = con.execute(f"SELECT COUNT(*) FROM raw.{t}").fetchone()[0]
    print(f"{t} rows: {count}")

con.close()