FROM apache/airflow:2.10.5-python3.12

# The MySQL provider ships with the official image; add a WebHDFS client.
RUN pip install --no-cache-dir "apache-airflow==2.10.5" "hdfs==2.7.3"
