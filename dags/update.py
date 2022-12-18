from __future__ import annotations

import os
import uuid
from datetime import datetime

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
    schedule_interval="*/30 * * * *",
    start_date=datetime(2021, 1, 1),
    tags=["API"],
) as dag:
    create_pyspark_job = DataprocCreatePysparkJobOperator(
        task_id="create_pyspark_job",
        main_python_file_uri="s3a://data-proc-public/jobs/sources/pyspark-001/main.py",
        python_file_uris=[
            "s3a://data-proc-public/jobs/sources/pyspark-001/geonames.py",
        ],
        file_uris=[
            "s3a://data-proc-public/jobs/sources/data/config.json",
        ],
        archive_uris=[
            "s3a://data-proc-public/jobs/sources/data/country-codes.csv.zip",
        ],
        args=[
            "s3a://data-proc-public/jobs/sources/data/cities500.txt.bz2",
            f"s3a://{S3_BUCKET_NAME_FOR_JOB_LOGS}/dataproc/job/results/${{JOB_ID}}",
        ],
        jar_file_uris=[
            "s3a://data-proc-public/jobs/sources/java/dataproc-examples-1.0.jar",
            "s3a://data-proc-public/jobs/sources/java/icu4j-61.1.jar",
            "s3a://data-proc-public/jobs/sources/java/commons-lang-2.6.jar",
        ],
        properties={
            "spark.submit.deployMode": "cluster",
        },
        packages=["org.slf4j:slf4j-simple:1.7.30"],
        repositories=["https://repo1.maven.org/maven2"],
        exclude_packages=["com.amazonaws:amazon-kinesis-client"],
    )
