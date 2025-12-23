import duckdb

con = duckdb.connect("warehouse/cola_warehouse.duckdb")

# Drop old feature table if exists
con.execute("DROP TABLE IF EXISTS features.demand_forecasting;")

# Create ML-ready feature table
con.execute("""
CREATE TABLE features.demand_forecasting AS
SELECT
    s.date,
    s.region,
    s.product,
    s.units_sold,  -- target
    AVG(s.temperature) OVER (PARTITION BY s.region, s.product ORDER BY s.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_temp_7d,
    SUM(s.rainfall_mm) OVER (PARTITION BY s.region, s.product ORDER BY s.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rainfall_7d,
    SUM(CASE WHEN s.promotion_flag THEN 1 ELSE 0 END) OVER (PARTITION BY s.region, s.product ORDER BY s.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS promo_last_7d,
    AVG(s.discount_pct) OVER (PARTITION BY s.region, s.product ORDER BY s.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS discount_avg_7d,
    (SELECT SUM(tv_spend + digital_spend + influencer_spend + outdoor_spend + radio_spend)
     FROM clean.marketing_spend_daily m
     WHERE m.region = s.region
       AND m.date BETWEEN s.date - 6 AND s.date) AS marketing_total_7d,
    (SELECT AVG(stockout_rate)
     FROM clean.distribution_metrics_daily d
     WHERE d.region = s.region
       AND d.date BETWEEN s.date - 6 AND s.date) AS stockout_avg_7d,
    c.is_weekend,
    CASE WHEN c.festival_name IS NOT NULL THEN 1 ELSE 0 END AS is_festival
FROM clean.sales_daily s
LEFT JOIN clean.calendar_features c
ON s.date = c.date
""")

con.close()
print("✅ Feature table 'features.demand_forecasting' created successfully")