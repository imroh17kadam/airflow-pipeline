import sys
sys.path.append("/opt/airflow")

import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data

API_URL = "https://jsonplaceholder.typicode.com/todos"
DB_CONFIG = {
    "host": os.environ.get("DB_HOST"),
    "dbname": os.environ.get("DB_NAME"),     
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "port": int(os.environ.get("DB_PORT", 5432))
}

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="basic_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["etl", "practice"]
) as dag:

    def extract_wrapper(**kwargs):
        # Call extract_data and push to XCom
        data = extract_data(api_url=API_URL)
        return data  # Automatically stored in XCom

    def transform_wrapper(**kwargs):
        # Pull from XCom
        ti = kwargs['ti']
        raw_data = ti.xcom_pull(task_ids='extract')
        return transform_data(raw_data)

    def load_wrapper(**kwargs):
        ti = kwargs['ti']
        transformed_data = ti.xcom_pull(task_ids='transform')
        load_data(transformed_data, db_config=DB_CONFIG)

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_wrapper,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_wrapper,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_wrapper,
        provide_context=True
    )

    extract_task >> transform_task >> load_task