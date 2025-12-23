import duckdb

con = duckdb.connect("warehouse/cola_warehouse.duckdb")
tables = con.execute("SHOW TABLES FROM raw;").fetchall()
print(tables)
con.close()