"""
Orchestrates the batch ETL pipeline: historical real estate transactions.

This DAG has a single task on purpose, at this stage: it runs the exact
same "docker exec spark-master spark-submit ..." command that was already
validated manually. Airflow isn't changing WHAT runs -- it's adding WHO
watches it run: scheduling, retry on failure, execution history, and logs,
all visible in one place instead of a terminal session that disappears.

WHY schedule=None FOR NOW: this is a historical, one-time load (the whole
point of "batch" here is "load everything we have so far"). A recurring
ingestion pipeline (e.g. new transactions arriving daily) would use a cron
expression instead -- a natural next step, and a good talking point for
"how would you evolve this into a production pipeline."
"""

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="real_estate_batch_etl",
    description="Batch ETL: clean historical transactions, load raw zone (MinIO) + star schema (Postgres)",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["batch", "spark", "etl"],
) as dag:

    run_spark_batch_etl = BashOperator(
        task_id="run_spark_batch_etl",
        bash_command=(
            "docker exec spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3 "
            "/opt/spark-jobs/batch_etl_job.py"
        ),
    )