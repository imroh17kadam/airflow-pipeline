from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2

CSV_PATH = "/opt/airflow/data/sales_daily.csv"
DROP_COLUMNS = ['temperature', 'rainfall_mm']  # example columns to drop

def extract_csv(**kwargs):
    """Read CSV and push DataFrame to XCom"""
    df = pd.read_csv(CSV_PATH)
    df.columns = df.columns.str.strip()
    kwargs['ti'].xcom_push(key='raw_df', value=df.to_json())  # push as JSON

def transform_data(**kwargs):
    """Pull raw DF from XCom, transform it, push transformed DF back"""
    ti = kwargs['ti']
    raw_json = ti.xcom_pull(key='raw_df', task_ids='extract_task')
    df = pd.read_json(raw_json)
    
    # Example transformation: drop unwanted columns
    df = df.drop(columns=DROP_COLUMNS, errors='ignore')
    
    ti.xcom_push(key='transformed_df', value=df.to_json())

def load_to_postgres(**kwargs):
    """Pull transformed DF and insert into Postgres"""
    ti = kwargs['ti']
    transformed_json = ti.xcom_pull(key='transformed_df', task_ids='transform_task')
    df = pd.read_json(transformed_json)
    
    # Convert date column
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], dayfirst=True)
    
    # Convert boolean columns
    for col in ['promotion_flag', 'is_holiday']:
        if col in df.columns:
            df[col] = df[col].astype(bool)

    # Connect to Postgres
    conn = psycopg2.connect(
        host="postgres",
        database="cola_dw",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # Create schema & table
    cur.execute("""
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.etl_sales_daily (
        date DATE,
        region TEXT,
        product TEXT,
        units_sold INT,
        price NUMERIC,
        revenue NUMERIC,
        promotion_flag BOOLEAN,
        discount_pct NUMERIC,
        is_holiday BOOLEAN
    );
    """)
    conn.commit()

    # Insert rows
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO raw.etl_sales_daily (
                date, region, product, units_sold, price, revenue, 
                promotion_flag, discount_pct, is_holiday
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            row.get('date'), row.get('region'), row.get('product'), row.get('units_sold'), row.get('price'),
            row.get('revenue'), row.get('promotion_flag'), row.get('discount_pct'), row.get('is_holiday')
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"{len(df)} rows loaded successfully into raw.etl_sales_daily")


# DAG definition
with DAG(
    dag_id="csv_to_postgres_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_csv,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_to_postgres,
        provide_context=True
    )

    # Set task sequence
    extract_task >> transform_task >> load_task