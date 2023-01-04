from __future__ import annotations

import os
import uuid
import datetime as dt

from airflow import DAG
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreateHiveJobOperator,
    DataprocCreateMapReduceJobOperator,
    DataprocCreatePysparkJobOperator,
    DataprocCreateSparkJobOperator,
    DataprocDeleteClusterOperator,
)

args = {
    "owner": "airflow",
}

with DAG(
    dag_id="update-dataset",
    default_args=args,
    schedule_interval="@once",
    start_date=dt.datetime(2021, 1, 1),
    tags=["API"],
) as dag:
    create_pyspark_job = DataprocCreatePysparkJobOperator(
        task_id="create_pyspark_job",
        cluster_id="c9qe7r6747r6hidd2p05",
        name="airflow-update",
        main_python_file_uri="s3a://yl-otus/update.py",
        python_file_uris=[
            "s3a://yl-otus/cfd.zip",
        ],
        # file_uris=[
        #     "s3a://data-proc-public/jobs/sources/data/config.json",
        # ],
        # archive_uris=[
        #     "s3a://yl-otus/venv.zip",
        # ],
        args=["30", "2022-01-01", "10"],
        # jar_file_uris=[
        #     "s3a://data-proc-public/jobs/sources/java/dataproc-examples-1.0.jar",
        #     "s3a://data-proc-public/jobs/sources/java/icu4j-61.1.jar",
        #     "s3a://data-proc-public/jobs/sources/java/commons-lang-2.6.jar",
        # ],
        properties={
            "spark.submit.deployMode": "client",
        },
        # packages=["org.slf4j:slf4j-simple:1.7.30"],
        # repositories=["https://repo1.maven.org/maven2"],
        # exclude_packages=["com.amazonaws:amazon-kinesis-client"],
    )
