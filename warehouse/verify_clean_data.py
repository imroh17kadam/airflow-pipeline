import duckdb

con = duckdb.connect("warehouse/cola_warehouse.duckdb")

# Row counts
tables = ["sales_daily", "marketing_spend_daily", "distribution_metrics_daily", "calendar_features"]
for t in tables:
    count = con.execute(f"SELECT COUNT(*) FROM clean.{t}").fetchone()[0]
    print(f"{t} rows: {count}")

con.close()