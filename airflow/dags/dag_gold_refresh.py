"""
DAG: Refresh Gold
Atualiza as quatro visões Gold alinhadas aos processos doutrinários do C2:
  gold.coc       — Cenário Operacional Comum (EB70-MC-10.205)
  gold.pitcic    — Integração Terreno/Met./Inimigo/Civis (EB70-MC-10.336)
  gold.ppcot     — Planejamento e Condução das Op. Terrestres (EB70-MC-10.211)
  gold.avaliacao — Avaliação e Monitoramento da Condução (EB70-MC-10.211 Cap.V)
Agendamento: a cada 20 minutos.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from helpers.lineage_emitter import lineage_callback

default_args = {
    "owner": "dlh",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_success_callback": lineage_callback,
}

SPARK_SUBMIT_GOLD = (
    f"spark-submit "
    f"--master spark://spark-master:7077 "
    f"--conf spark.cores.max=1 "
    f"--conf spark.executor.cores=1 "
    f"--conf spark.executor.memory=6g "
    f"--conf spark.executor.memoryOverhead=1g "
    f"--conf 'spark.executor.extraJavaOptions=-XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=512m' "
    f"--conf spark.driver.memory=2g "
    f"--conf spark.sql.iceberg.vectorization.enabled=false "
    f"--conf spark.sql.shuffle.partitions=4 "
    f"--conf spark.sql.adaptive.enabled=true "
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

with DAG(
    dag_id="dag_gold_refresh",
    description="Visões Gold doutrinariamente orientadas: COC, PITCIC, PPCOT, Avaliação",
    schedule="*/20 * * * *",
    start_date=datetime(2026, 4, 27),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["gold", "coc", "pitcic", "ppcot", "spark", "iceberg"],
) as dag:

    # COC — situação integrada por subunidade (JOIN cross-função)
    coc = BashOperator(
        task_id="gold_coc",
        bash_command=SPARK_SUBMIT_GOLD + "/opt/spark-jobs/silver_to_gold.py --visao coc",
    )

    # PITCIC — análise do ambiente operacional por batalhão
    pitcic = BashOperator(
        task_id="gold_pitcic",
        bash_command=SPARK_SUBMIT_GOLD + "/opt/spark-jobs/silver_to_gold.py --visao pitcic",
    )

    # PPCOT — insumos para o Exame de Situação do Comandante
    ppcot = BashOperator(
        task_id="gold_ppcot",
        bash_command=SPARK_SUBMIT_GOLD + "/opt/spark-jobs/silver_to_gold.py --visao ppcot",
    )

    # Avaliação — MEF e MED por subunidade para monitoramento da condução
    avaliacao = BashOperator(
        task_id="gold_avaliacao",
        bash_command=SPARK_SUBMIT_GOLD + "/opt/spark-jobs/silver_to_gold.py --visao avaliacao",
    )

    # Sequencial para não disputar o único core Spark disponível
    coc >> pitcic >> ppcot >> avaliacao
