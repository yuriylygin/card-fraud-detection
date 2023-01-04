from __future__ import annotations

import os
import uuid
import datetime as dt
import logging

from airflow import DAG
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreateHiveJobOperator,
    DataprocCreateMapReduceJobOperator,
    DataprocCreatePysparkJobOperator,
    DataprocCreateSparkJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.operators.python_operator import PythonOperator


logger = logging.getLogger("airflow.task")

def catch_time(scheduled_date, scheduled_time):
    logger.info(scheduled_date)
    logger.info(scheduled_time)


class CustomDataprocCreatePysparkJobOperator(DataprocCreatePysparkJobOperator):
    template_fields = ['args']


args = {
    "owner": "airflow",
}

with DAG(
    dag_id="update-dataset",
    default_args=args,
    schedule_interval="@monthly",
    start_date=dt.datetime(2021, 2, 1),
    tags=["data collecting"],
) as dag:
    catch_time_task = PythonOperator(
        task_id="preprocess",
        python_callable=catch_time,
        op_kwargs={"scheduled_date": "{{ ds }}", "scheduled_time": "{{ ts }}"},
    )
    update_dataset_task = CustomDataprocCreatePysparkJobOperator(
        depends_on_past=True,
        task_id="update-dataset",
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
        args=["15", "{{ ds }}", "10"],
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

    catch_time_task >> update_dataset_task
