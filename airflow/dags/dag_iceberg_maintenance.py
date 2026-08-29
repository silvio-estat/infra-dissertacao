"""
DAG: Manutenção Iceberg
Executa operações periódicas de manutenção das tabelas Iceberg:
- expire_snapshots: remove snapshots antigos
- remove_orphan_files: limpa arquivos sem referência nos manifestos
- rewrite_manifests: otimiza os arquivos de manifesto
- rewrite_data_files: compacta small files (compaction)
Agendamento: diário às 02:00.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "dlh",
    "retries": 0,
    "email_on_failure": False,
}

# Construção dinâmica do comando Spark Submit para herdar credenciais do ambiente
SPARK_MAINT = (
    f"spark-submit "
    f"--master spark://spark-master:7077 "
    f"--conf spark.cores.max=1 "
    f"--conf spark.executor.cores=1 "
    f"--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
    f"--conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog "
    f"--conf spark.sql.catalog.lakehouse.type=hive "
    f"--conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 "
    f"--conf spark.sql.catalog.lakehouse.warehouse=s3a://lakehouse/warehouse "
    f"--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    f"--conf spark.hadoop.fs.s3a.path.style.access=true "
    f"--conf spark.hadoop.fs.s3a.access.key={os.environ.get('MINIO_ROOT_USER', '')} "
    f"--conf spark.hadoop.fs.s3a.secret.key={os.environ.get('MINIO_ROOT_PASSWORD', '')} "
    f"--conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener "
)

TABELAS = ["lakehouse.bronze.dados", "lakehouse.silver.dados", "lakehouse.gold.posicionamento_atual"]

with DAG(
    dag_id="dag_iceberg_maintenance",
    description="Manutenção periódica das tabelas Iceberg (expire, compact, rewrite)",
    schedule="0 2 * * *",
    start_date=datetime(2026, 4, 27),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["iceberg", "manutencao"],
) as dag:

    expire_snapshots = BashOperator(
        task_id="expire_snapshots",
        bash_command=(
            SPARK_MAINT + "/opt/spark-jobs/iceberg_maintenance.py --op expire_snapshots "
            "--older-than-days 7"
        ),
    )

    remove_orphan_files = BashOperator(
        task_id="remove_orphan_files",
        bash_command=(
            SPARK_MAINT + "/opt/spark-jobs/iceberg_maintenance.py --op remove_orphan_files"
        ),
    )

    rewrite_manifests = BashOperator(
        task_id="rewrite_manifests",
        bash_command=(
            SPARK_MAINT + "/opt/spark-jobs/iceberg_maintenance.py --op rewrite_manifests"
        ),
    )

    compaction = BashOperator(
        task_id="compaction_small_files",
        bash_command=(
            SPARK_MAINT + "/opt/spark-jobs/iceberg_maintenance.py --op compaction"
        ),
    )

    expire_snapshots >> remove_orphan_files >> rewrite_manifests >> compaction
