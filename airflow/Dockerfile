FROM apache/airflow:2.9.1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    kafka-python==2.2.15 \
    numpy==1.26.4 \
    pandas==2.2.2 \
    pyspark==3.5.8

COPY --chown=airflow:root start-airflow.sh /opt/airflow/start-airflow.sh
RUN chmod +x /opt/airflow/start-airflow.sh
