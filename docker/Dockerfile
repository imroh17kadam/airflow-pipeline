FROM apache/airflow:2.8.1

# Switch to root to install system packages
USER root
RUN apt-get update && apt-get install -y \
        build-essential \
        g++ \
        && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch to airflow user for Python packages
USER airflow
RUN pip install --upgrade pip
COPY requirements.txt /
RUN pip install --no-cache-dir --default-timeout=1000 -r /requirements.txt

COPY dags /opt/airflow/dags
COPY plugins /opt/airflow/plugins
COPY data /opt/airflow/data