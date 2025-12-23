import numpy as np
import pandas as pd

np.random.seed(42)

# ----------------------------
# CONFIG
# ----------------------------
START_DATE = "2024-01-01"
END_DATE = "2024-12-31"

regions = ["North", "South", "East", "West"]
products = ["Cola_Classic", "Cola_Zero"]

base_price = {
    "Cola_Classic": 40,
    "Cola_Zero": 45
}

# ----------------------------
# DATE RANGE
# ----------------------------
dates = pd.date_range(start=START_DATE, end=END_DATE, freq="D")
calendar_df = pd.DataFrame({"date": dates})

# ----------------------------
# CALENDAR FEATURES
# ----------------------------
calendar_df["day_of_week"] = calendar_df["date"].dt.weekday
calendar_df["week_of_year"] = calendar_df["date"].dt.isocalendar().week.astype(int)
calendar_df["month"] = calendar_df["date"].dt.month
calendar_df["is_weekend"] = calendar_df["day_of_week"].isin([5, 6]).astype(int)

calendar_df["festival_name"] = np.where(
    calendar_df["month"].isin([10, 11]),
    "Festival_Season",
    None
)

# ----------------------------
# MARKETING SPEND
# ----------------------------
marketing_rows = []

for date in dates:
    for region in regions:
        seasonal_multiplier = 1.3 if date.month in [3,4,5,6] else 1.0
        festival_multiplier = 1.5 if date.month in [10,11] else 1.0

        marketing_rows.append({
            "date": date,
            "region": region,
            "tv_spend": np.random.uniform(20000, 40000) * seasonal_multiplier * festival_multiplier,
            "digital_spend": np.random.uniform(10000, 20000),
            "influencer_spend": np.random.uniform(3000, 8000) * festival_multiplier,
            "outdoor_spend": np.random.uniform(5000, 15000),
            "radio_spend": np.random.uniform(2000, 6000)
        })

marketing_df = pd.DataFrame(marketing_rows)

# ----------------------------
# DISTRIBUTION METRICS
# ----------------------------
distribution_rows = []

for date in dates:
    for region in regions:
        high_demand = 1 if date.month in [3,4,5,6] else 0

        distribution_rows.append({
            "date": date,
            "region": region,
            "stores_active": np.random.randint(800, 1200),
            "stockout_rate": np.clip(np.random.normal(0.05 + 0.03*high_demand, 0.02), 0, 0.2),
            "delivery_delay_hours": np.clip(np.random.normal(3 + 2*high_demand, 1), 0, None)
        })

distribution_df = pd.DataFrame(distribution_rows)

# ----------------------------
# SALES DATA (CORE TABLE)
# ----------------------------
sales_rows = []

for date in dates:
    for region in regions:
        for product in products:

            base_demand = 900 if product == "Cola_Classic" else 600

            # Seasonality
            seasonal_factor = 1.4 if date.month in [3,4,5,6] else 0.9 if date.month in [12,1] else 1.0

            # Promotion
            promotion_flag = np.random.binomial(1, 0.25)
            discount_pct = np.random.uniform(5, 20) if promotion_flag else 0

            promotion_lift = 1 + (discount_pct / 100) * 1.8

            # Weather
            temperature = np.random.normal(32 if date.month in [4,5,6] else 25, 3)
            rainfall = np.random.exponential(3 if date.month in [7,8] else 1)

            weather_lift = 1 + (temperature - 25) * 0.015 - rainfall * 0.01

            # Marketing lift
            daily_marketing = marketing_df[
                (marketing_df["date"] == date) &
                (marketing_df["region"] == region)
            ].iloc[0]

            marketing_spend = (
                daily_marketing["tv_spend"] +
                daily_marketing["digital_spend"] +
                daily_marketing["influencer_spend"]
            )

            marketing_lift = 1 + np.log1p(marketing_spend) * 0.03

            # Distribution penalty
            daily_distribution = distribution_df[
                (distribution_df["date"] == date) &
                (distribution_df["region"] == region)
            ].iloc[0]

            stockout_penalty = 1 - daily_distribution["stockout_rate"]

            # Final units sold
            units_sold = (
                base_demand *
                seasonal_factor *
                promotion_lift *
                weather_lift *
                marketing_lift *
                stockout_penalty
            )

            units_sold += np.random.normal(0, 50)
            units_sold = max(0, int(units_sold))

            price = base_price[product] * (1 - discount_pct/100)
            revenue = units_sold * price

            sales_rows.append({
                "date": date,
                "region": region,
                "product": product,
                "units_sold": units_sold,
                "price": round(price, 2),
                "revenue": round(revenue, 2),
                "promotion_flag": promotion_flag,
                "discount_pct": round(discount_pct, 2),
                "temperature": round(temperature, 2),
                "rainfall_mm": round(rainfall, 2),
                "is_holiday": 1 if date.month in [10,11] else 0
            })

sales_df = pd.DataFrame(sales_rows)

# ----------------------------
# SAVE FILES
# ----------------------------
calendar_df.to_csv("calendar_features.csv", index=False)
marketing_df.to_csv("marketing_spend_daily.csv", index=False)
distribution_df.to_csv("distribution_metrics_daily.csv", index=False)
sales_df.to_csv("sales_daily.csv", index=False)

print("✅ Synthetic production-grade data generated successfully.")