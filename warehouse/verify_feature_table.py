import duckdb

con = duckdb.connect("warehouse/cola_warehouse.duckdb")
print(con.execute("SELECT * FROM features.demand_forecasting LIMIT 5").fetchdf())
con.close()