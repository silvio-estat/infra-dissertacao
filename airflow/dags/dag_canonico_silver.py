"""
DAG canonico_silver — aplica o de/para do modelo canonico e escreve a Silver.

    conferir_pendentes ──► transformar_relper   (ha extracao de planilha sem evento)
                       └─► nada_a_fazer         (toda extracao ja virou evento)

O que a tarefa faz, em uma frase: abre a grade de celulas que a extracao gravou,
resolve cada campo pela regra declarada em canonico/modelo_canonico.yaml e grava
uma linha em EVENTO e uma em SITUACAO_UNIDADE por FRACAO — um arquivo de
planilha produz varias observacoes.

Nao ha regra de negocio aqui nem no job: elas estao todas no YAML. Integrar uma
fonte nova e acrescentar um bloco la e uma tarefa aqui.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from helpers.lineage_emitter import linhagem

# Extracao de planilha que ainda nao virou evento. A conta e pelo elo de
# linhagem: todo evento guarda o EXTRACAO_IDT de onde seus numeros sairam.
PENDENTES_SQL = """
    SELECT count(*)
    FROM iceberg.bronze.extracao x
    JOIN iceberg.bronze.arquivo a ON a.arquivo_idt = x.arquivo_idt
    LEFT JOIN iceberg.silver.evento e ON e.extracao_idt = x.extracao_idt
    WHERE a.modalidade_cod = 'PLANILHA' AND x.status_cod = 'OK'
      AND e.extracao_idt IS NULL
"""

CONF_SPARK = {
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
    "spark.hadoop.fs.s3a.aws.credentials.provider":
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}

BRONZE = ["bronze.extracao", "bronze.arquivo", "bronze.recepcao_bruta"]


def conferir_pendentes() -> str:
    import trino

    cur = trino.dbapi.connect(host="trino", port=8090, user="airflow", http_scheme="http").cursor()
    cur.execute(PENDENTES_SQL)
    pendentes = cur.fetchone()[0]
    print(f"extracoes de planilha sem evento: {pendentes}")
    return "transformar_relper" if pendentes else "nada_a_fazer"


with DAG(
    dag_id="canonico_silver",
    description="Bronze -> EVENTO + SITUACAO_UNIDADE pelo de/para do modelo canonico",
    schedule=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "dlh", "retries": 0, "retry_delay": timedelta(minutes=2)},
    tags=["silver", "canonico"],
) as dag:

    conferir = BranchPythonOperator(task_id="conferir_pendentes", python_callable=conferir_pendentes)
    nada_a_fazer = EmptyOperator(task_id="nada_a_fazer")

    transformar = SparkSubmitOperator(
        task_id="transformar_relper",
        application="/opt/spark-jobs/canonico_para_silver.py",
        application_args=["--fonte", "RELPER", "--tipo", "situacao"],
        conn_id="spark_default",
        name="canonico_silver_relper",
        env_vars={"MODELO_CANONICO": "/opt/canonico/modelo_canonico.yaml"},
        conf=CONF_SPARK,
        verbose=False,
        # Uma linhagem por tabela de destino: as colunas de cada uma sao diferentes.
        on_success_callback=[
            linhagem(le=BRONZE, escreve=["silver.evento"], colunas={
                "unidade_reportante_cod": [("bronze.extracao", "saida_txt")],
                "ocorrencia_data":        [("bronze.recepcao_bruta", "conteudo_json_txt")],
                "operacao_cod":           [("bronze.recepcao_bruta", "conteudo_json_txt")],
                "arquivo_idt":            [("bronze.arquivo", "arquivo_idt")],
                "extracao_idt":           [("bronze.extracao", "extracao_idt")],
            }),
            linhagem(le=BRONZE, escreve=["silver.situacao_unidade"], colunas={
                "ef_presente_qnt":    [("bronze.extracao", "saida_txt")],
                "vtr_operacional_qnt": [("bronze.extracao", "saida_txt")],
                "necessidade_txt":    [("bronze.extracao", "saida_txt")],
                "atributo_extra_txt": [("bronze.extracao", "saida_txt")],
                "turno_cod":          [("bronze.recepcao_bruta", "conteudo_json_txt")],
            }),
        ],
    )

    conferir >> [transformar, nada_a_fazer]
