FROM apache/airflow:2.8.1-python3.11

# Airflow home
ENV AIRFLOW_HOME=/opt/airflow

# Make src importable in DAGs
ENV PYTHONPATH="${AIRFLOW_HOME}:${AIRFLOW_HOME}/src"

USER root

# System dependencies
RUN apt-get update && \
    apt-get install -y gcc libpq-dev curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# DAGs and source code
COPY dags/ $AIRFLOW_HOME/dags/
COPY src/ $AIRFLOW_HOME/src/