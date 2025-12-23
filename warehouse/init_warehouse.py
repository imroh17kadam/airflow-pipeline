import duckdb

con = duckdb.connect("warehouse/cola_warehouse.duckdb")

# Create schemas
con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
con.execute("CREATE SCHEMA IF NOT EXISTS clean;")
con.execute("CREATE SCHEMA IF NOT EXISTS features;")

con.close()
print("✅ DuckDB warehouse initialized")