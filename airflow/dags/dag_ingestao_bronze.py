"""
DAG: Ingestão Bronze
Monitora o bucket 'landing' no MinIO, converte JSONs para Parquet
e registra como tabela Iceberg na camada Bronze com metadados de proveniência.
Agendamento: a cada 5 minutos (simula chegada de lotes dos batalhões).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from helpers.lineage_emitter import lineage_callback

default_args = {
    "owner": "dlh",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "on_success_callback": lineage_callback,
}



with DAG(
    dag_id="dag_ingestao_bronze",
    description="Ingestão de lotes JSON do landing para camada Bronze (Iceberg)",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 4, 27),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bronze", "ingestao", "iceberg"],
) as dag:

    def verificar_bucket_landing(**context):
        import boto3
        import os
        from botocore.client import Config

        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.environ.get("MINIO_ROOT_USER"),
            aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD"),
            config=Config(signature_version="s3v4"),
        )
        resp = s3.list_objects_v2(Bucket="landing")
        arquivos = resp.get("Contents", [])
        if not arquivos:
            print("Landing vazio — nenhum arquivo encontrado.")
        else:
            print(f"Arquivos no landing ({len(arquivos)} total):")
            for obj in arquivos:
                print(f"  {obj['Key']}  ({obj['Size']} bytes)")
        return len(arquivos)

    verificar_landing = PythonOperator(
        task_id="verificar_bucket_landing",
        python_callable=verificar_bucket_landing,
    )

    ingerir_bronze = SparkSubmitOperator(
        task_id="ingerir_json_para_iceberg_bronze",
        application="/opt/spark-jobs/bronze_ingestor.py",
        conn_id="spark_default",
        name="bronze_ingestor",
        conf={
            "spark.cores.max": "1",
            "spark.executor.cores": "1",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.lakehouse.type": "hive",
            "spark.sql.catalog.lakehouse.uri": "thrift://hive-metastore:9083",
            "spark.sql.catalog.lakehouse.warehouse": "s3a://lakehouse/warehouse",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.access.key": os.environ.get("MINIO_ROOT_USER", ""),
            "spark.hadoop.fs.s3a.secret.key": os.environ.get("MINIO_ROOT_PASSWORD", ""),
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
        },
        application_args=["--logical-date", "{{ logical_date.isoformat() }}"],
        verbose=True,
    )

    verificar_landing >> ingerir_bronze
