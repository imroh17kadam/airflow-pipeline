import duckdb

con = duckdb.connect("warehouse/cola_warehouse.duckdb")

# ---- SALES ----
con.execute("""
CREATE TABLE IF NOT EXISTS clean.sales_daily AS
SELECT *
FROM raw.sales_daily
WHERE
    units_sold >= 0
    AND revenue >= 0
    AND discount_pct BETWEEN 0 AND 50
    AND temperature BETWEEN 10 AND 50
    AND rainfall_mm >= 0
""")
print("✅ Clean sales_daily populated")

# ---- MARKETING ----
con.execute("""
CREATE TABLE IF NOT EXISTS clean.marketing_spend_daily AS
SELECT *
FROM raw.marketing_spend_daily
WHERE tv_spend >= 0
  AND digital_spend >= 0
  AND influencer_spend >= 0
  AND outdoor_spend >= 0
  AND radio_spend >= 0
""")
print("✅ Clean marketing_spend_daily populated")

# ---- DISTRIBUTION ----
con.execute("""
CREATE TABLE IF NOT EXISTS clean.distribution_metrics_daily AS
SELECT *
FROM raw.distribution_metrics_daily
WHERE stores_active >= 0
  AND stockout_rate BETWEEN 0 AND 0.3
  AND delivery_delay_hours >= 0
""")
print("✅ Clean distribution_metrics_daily populated")

# ---- CALENDAR ----
con.execute("""
CREATE TABLE IF NOT EXISTS clean.calendar_features AS
SELECT *
FROM raw.calendar_features
WHERE day_of_week BETWEEN 0 AND 6
  AND week_of_year BETWEEN 1 AND 53
  AND month BETWEEN 1 AND 12
""")
print("✅ Clean calendar_features populated")

con.close()
print("🎯 RAW → CLEAN validation done successfully")