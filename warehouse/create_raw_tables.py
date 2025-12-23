import duckdb

# Connect to DuckDB warehouse
con = duckdb.connect("warehouse/cola_warehouse.duckdb")

# RAW: sales_daily
con.execute("""
CREATE TABLE IF NOT EXISTS raw.sales_daily (
    date DATE,
    region VARCHAR,
    product VARCHAR,
    units_sold INTEGER,
    price DOUBLE,
    revenue DOUBLE,
    promotion_flag BOOLEAN,
    discount_pct DOUBLE,
    temperature DOUBLE,
    rainfall_mm DOUBLE,
    is_holiday BOOLEAN
);
""")

# RAW: marketing_spend_daily
con.execute("""
CREATE TABLE IF NOT EXISTS raw.marketing_spend_daily (
    date DATE,
    region VARCHAR,
    tv_spend DOUBLE,
    digital_spend DOUBLE,
    influencer_spend DOUBLE,
    outdoor_spend DOUBLE,
    radio_spend DOUBLE
);
""")

# RAW: distribution_metrics_daily
con.execute("""
CREATE TABLE IF NOT EXISTS raw.distribution_metrics_daily (
    date DATE,
    region VARCHAR,
    stores_active INTEGER,
    stockout_rate DOUBLE,
    delivery_delay_hours DOUBLE
);
""")

# RAW: calendar_features
con.execute("""
CREATE TABLE IF NOT EXISTS raw.calendar_features (
    date DATE,
    day_of_week INTEGER,
    week_of_year INTEGER,
    month INTEGER,
    is_weekend BOOLEAN,
    festival_name VARCHAR
);
""")

con.close()
print("✅ RAW tables created successfully")