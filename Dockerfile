# 1️⃣ Base image
FROM apache/airflow:2.8.1-python3.11

# 2️⃣ Set environment
ENV AIRFLOW_HOME=/opt/airflow

# 3️⃣ Switch to root to install system dependencies
USER root

RUN apt-get update && \
    apt-get install -y gcc libpq-dev curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 4️⃣ Switch back to airflow user before installing Python packages
USER airflow

# 5️⃣ Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6️⃣ Copy DAGs and src code
COPY dags/ $AIRFLOW_HOME/dags/
COPY src/ $AIRFLOW_HOME/src/

# 7️⃣ Default command
ENTRYPOINT ["/entrypoint"]
CMD ["webserver"]