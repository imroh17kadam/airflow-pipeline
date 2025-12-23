from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2

# Path inside container
CSV_PATH = "/opt/airflow/data/sales_daily.csv"

def load_csv_to_postgres():
    # Load CSV
    df = pd.read_csv(CSV_PATH)
    df.columns = df.columns.str.strip()

    # Convert date column to proper datetime format (Postgres friendly)
    df['date'] = pd.to_datetime(df['date'], dayfirst=True)

    # Convert integer flags to boolean
    df['promotion_flag'] = df['promotion_flag'].astype(bool)
    df['is_holiday'] = df['is_holiday'].astype(bool)

    # Connect to Postgres
    conn = psycopg2.connect(
        host="postgres",
        database="cola_dw",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # Create schema and table if not exists
    cur.execute("""
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.sales_daily (
        date DATE,
        region TEXT,
        product TEXT,
        units_sold INT,
        price NUMERIC,
        revenue NUMERIC,
        promotion_flag BOOLEAN,
        discount_pct NUMERIC,
        temperature NUMERIC,
        rainfall_mm NUMERIC,
        is_holiday BOOLEAN
    );
    """)
    conn.commit()

    # Optional: truncate table before insert to avoid duplicates
    # cur.execute("TRUNCATE TABLE raw.sales_daily;")
    # conn.commit()

    # Insert rows
    for _, row in df.iterrows():
        cur.execute("""
        INSERT INTO raw.sales_daily (
            date, region, product, units_sold, price, revenue, 
            promotion_flag, discount_pct, temperature, rainfall_mm, is_holiday
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            row['date'], row['region'], row['product'], row['units_sold'], row['price'],
            row['revenue'], row['promotion_flag'], row['discount_pct'],
            row['temperature'], row['rainfall_mm'], row['is_holiday']
        ))

    # Commit and close
    conn.commit()
    cur.close()
    conn.close()
    print(f"{len(df)} rows loaded successfully into raw.sales_daily")

# DAG definition
with DAG(
    dag_id="csv_to_postgres_raw",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    ingest = PythonOperator(
        task_id="ingest_csv",
        python_callable=load_csv_to_postgres
    )