"""
DAG: Transformação Silver
Executa jobs Spark de normalização, deduplicação e enriquecimento
sobre as tabelas Iceberg da camada Bronze → Silver.
Agendamento: a cada 15 minutos.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from helpers.lineage_emitter import lineage_callback

default_args = {
    "owner": "dlh",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_success_callback": lineage_callback,
}

with DAG(
    dag_id="dag_silver_transform",
    description="Normalização e enriquecimento Bronze → Silver (Spark + Iceberg)",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 4, 27),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["silver", "spark", "iceberg"],
) as dag:

    SPARK_CONF = {
        "spark.cores.max": "1",
        "spark.executor.cores": "1",
        "spark.executor.memory": "6g",
        "spark.executor.memoryOverhead": "1g",
        "spark.executor.extraJavaOptions": "-XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=512m",
        "spark.driver.memory": "2g",
        "spark.sql.shuffle.partitions": "4",
        "spark.default.parallelism": "4",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.iceberg.vectorization.enabled": "false",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lakehouse.type": "hive",
        "spark.sql.catalog.lakehouse.uri": "thrift://hive-metastore:9083",
        "spark.sql.catalog.lakehouse.warehouse": "s3a://lakehouse/warehouse",
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER", ""),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD", ""),
        "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
    }

    silver_gps = SparkSubmitOperator(
        task_id="silver_gps",
        conn_id="spark_default",
        application="/opt/spark-jobs/bronze_to_silver.py",
        application_args=["--tipo", "gps"],
        conf=SPARK_CONF,
    )
    silver_gps.executor = "spark"

    silver_sensor = SparkSubmitOperator(
        task_id="silver_sensor",
        conn_id="spark_default",
        application="/opt/spark-jobs/bronze_to_silver.py",
        application_args=["--tipo", "sensor"],
        conf=SPARK_CONF,
    )
    silver_sensor.executor = "spark"

    # --- Novas Funções de Combate ---

    silver_relt_intel = SparkSubmitOperator(
        task_id="silver_relt_intel",
        conn_id="spark_default",
        application="/opt/spark-jobs/bronze_to_silver.py",
        application_args=["--tipo", "relt_intel"],
        conf=SPARK_CONF,
    )
    silver_relt_intel.executor = "spark"

    silver_paf = SparkSubmitOperator(
        task_id="silver_paf",
        conn_id="spark_default",
        application="/opt/spark-jobs/bronze_to_silver.py",
        application_args=["--tipo", "paf"],
        conf=SPARK_CONF,
    )
    silver_paf.executor = "spark"

    silver_obstaculo = SparkSubmitOperator(
        task_id="silver_obstaculo",
        conn_id="spark_default",
        application="/opt/spark-jobs/bronze_to_silver.py",
        application_args=["--tipo", "obstaculo"],
        conf=SPARK_CONF,
    )
    silver_obstaculo.executor = "spark"

    silver_seg_area = SparkSubmitOperator(
        task_id="silver_seg_area",
        conn_id="spark_default",
        application="/opt/spark-jobs/bronze_to_silver.py",
        application_args=["--tipo", "seg_area"],
        conf=SPARK_CONF,
    )
    silver_seg_area.executor = "spark"

    silver_pessoal = SparkSubmitOperator(
        task_id="silver_pessoal",
        conn_id="spark_default",
        application="/opt/spark-jobs/bronze_to_silver.py",
        application_args=["--tipo", "pessoal"],
        conf=SPARK_CONF,
    )
    silver_pessoal.executor = "spark"

    silver_material = SparkSubmitOperator(
        task_id="silver_material",
        conn_id="spark_default",
        application="/opt/spark-jobs/bronze_to_silver.py",
        application_args=["--tipo", "material"],
        conf=SPARK_CONF,
    )
    silver_material.executor = "spark"

    # Execução sequencial para não sobrecarregar o Spark single-node
    silver_pessoal >> silver_material >> silver_seg_area >> silver_obstaculo >> silver_paf >> silver_relt_intel >> silver_sensor >> silver_gps
