"""
DAG manutencao — arruma as tabelas Iceberg por dentro. Nao move dado.

    expire_snapshots ──► remove_orphan_files ──► rewrite_manifests ──► compaction_small_files

Uma tabela Iceberg e um monte de arquivos Parquet no MinIO mais um indice
(os manifestos) dizendo quais deles formam a versao atual. Cada escrita cria
uma versao nova — um snapshot — e guarda a anterior. Isso e o que permite
consultar a tabela como ela estava ontem, mas cobra espaco. As quatro tarefas,
em ordem: apagar snapshot velho, apagar arquivo que sobrou sem dono, reagrupar
o indice e juntar arquivos pequenos em grandes.

Em serie porque o worker Spark e um so, e nesta ordem porque cada uma limpa o
que a anterior deixou solto.

As tabelas nao estao listadas aqui: o job as tira da secao `entidades` do modelo
canonico. Tabela que ainda nao existe vira SKIP no log, nao erro.
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

# O driver do spark-submit roda DENTRO do Airflow, que nao tem o
# spark-defaults.conf da imagem do Spark — por isso tudo o que aquele arquivo
# ajusta esta repetido aqui (ver o comentario longo em 3_silver_evento.py).
SPARK_MAINT = (
    "spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.cores.max=1 "
    "--conf spark.executor.cores=1 "
    "--conf spark.executor.memory=4g "
    "--conf spark.driver.memory=2g "
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
    "--conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog "
    "--conf spark.sql.catalog.lakehouse.type=hive "
    "--conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 "
    "--conf spark.sql.catalog.lakehouse.warehouse=s3a://lakehouse/warehouse "
    "--conf spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.hadoop.HadoopFileIO "
    "--conf spark.sql.iceberg.vectorization.enabled=false "
    "--conf spark.sql.shuffle.partitions=4 "
    "--conf spark.default.parallelism=4 "
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
    f"--conf spark.hadoop.fs.s3a.access.key={os.environ.get('MINIO_ROOT_USER', '')} "
    f"--conf spark.hadoop.fs.s3a.secret.key={os.environ.get('MINIO_ROOT_PASSWORD', '')} "
)

with DAG(
    dag_id="dag_iceberg_maintenance",
    description="Manutencao periodica das tabelas Iceberg (expire, compact, rewrite)",
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
