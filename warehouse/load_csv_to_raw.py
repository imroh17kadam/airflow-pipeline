import duckdb
import os

# Connect to DuckDB warehouse
con = duckdb.connect("warehouse/cola_warehouse.duckdb")

# Path to CSV folder
csv_folder = "data/raw/"

# Dictionary mapping CSV → RAW table
csv_to_table = {
    "sales_daily.csv": "raw.sales_daily",
    "marketing_spend_daily.csv": "raw.marketing_spend_daily",
    "distribution_metrics_daily.csv": "raw.distribution_metrics_daily",
    "calendar_features.csv": "raw.calendar_features"
}

for csv_file, table_name in csv_to_table.items():
    csv_path = os.path.join(csv_folder, csv_file)
    
    # Check if file exists
    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        continue

    # Load CSV into RAW table
    con.execute(f"""
    COPY {table_name}
    FROM '{csv_path}'
    (AUTO_DETECT TRUE, DELIMITER ',', HEADER TRUE);
    """)
    print(f"✅ Loaded {csv_file} into {table_name}")

con.close()
print("🎯 All CSVs loaded into RAW tables successfully")