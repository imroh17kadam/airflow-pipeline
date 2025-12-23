import duckdb
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib

# Connect to DuckDB and fetch feature table
con = duckdb.connect("warehouse/cola_warehouse.duckdb")
df = con.execute("SELECT * FROM features.demand_forecasting").fetchdf()
con.close()

# Prepare features and target
target = 'units_sold'
exclude_cols = ['date']  # keep region/product for one-hot
X = df.drop(columns=exclude_cols)
y = df[target]

# One-hot encode categorical columns
X = pd.get_dummies(X, columns=['region', 'product'], drop_first=True)

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialize Random Forest
model = RandomForestRegressor(n_estimators=100, random_state=42)

# Train model
model.fit(X_train, y_train)

# Evaluate
y_pred = model.predict(X_test)
mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"MAE: {mae:.2f}, MSE: {mse:.2f}, R2: {r2:.2f}")

# Save the model
joblib.dump(model, "training/random_forest_model.pkl")
print("✅ Model trained and saved at 'training/random_forest_model.pkl'")