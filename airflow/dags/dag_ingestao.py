"""
DAG ingestao — leva o que chegou em landing/ para a camada Bronze.

Duas tarefas, uma decisao:

    conferir_landing ──► ingerir_bronze      (ha arquivo que ainda nao esta na Bronze)
                     └─► nada_a_fazer        (tudo que esta em landing/ ja foi ingerido)

`conferir_landing` compara a lista de arquivos do MinIO com os enderecos ja
gravados em RECEPCAO_BRUTA e ARQUIVO. Um arquivo conta como ingerido se o seu
endereco esta na Bronze, OU se um arquivo de conteudo identico ja esta — porque
a Bronze deduplica pelo hash e um reenvio byte a byte nao gera linha nova.

`ingerir_bronze` chama o job Spark `ingestao_bronze.py`, que so cataloga
(RECEPCAO_BRUTA + ARQUIVO). Ler o conteudo dos binarios e trabalho das DAGs
de extracao.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

BUCKET = "lakehouse"
PREFIXO_LANDING = "landing/"
TABELAS_BRONZE = {                      # tabela Trino -> coluna com o endereco do arquivo
    "iceberg.bronze.recepcao_bruta": "origem_uri_txt",
    "iceberg.bronze.arquivo": "arquivo_uri_txt",
}


def listar_landing() -> dict:
    """{endereco s3a: etag}. O ETag e a impressao digital que o MinIO calcula
    do conteudo — dois arquivos identicos tem o mesmo ETag."""
    import boto3
    from botocore.client import Config

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        config=Config(signature_version="s3v4"),
    )
    arquivos = {}
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIXO_LANDING):
        for obj in pagina.get("Contents", []):
            arquivos[f"s3a://{BUCKET}/{obj['Key']}"] = obj["ETag"].strip('"')
    return arquivos


def enderecos_na_bronze() -> set:
    """Todos os enderecos de landing/ que a Bronze ja conhece.
    Se as tabelas ainda nao existem, nada foi ingerido: devolve vazio."""
    import trino

    conn = trino.dbapi.connect(host="trino", port=8090, user="airflow", http_scheme="http")
    enderecos = set()
    for tabela, coluna in TABELAS_BRONZE.items():
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT DISTINCT {coluna} FROM {tabela}")
            enderecos.update(linha[0] for linha in cur.fetchall())
        except Exception as erro:              # tabela inexistente = primeira rodada
            print(f"{tabela}: nao consultada ({type(erro).__name__}); tratando como vazia")
    return enderecos


def conferir_landing() -> str:
    landing = listar_landing()
    bronze = enderecos_na_bronze()

    etags_ingeridos = {etag for uri, etag in landing.items() if uri in bronze}
    pendentes = [uri for uri, etag in landing.items()
                 if uri not in bronze and etag not in etags_ingeridos]

    print(f"landing: {len(landing)} arquivos | ja na Bronze: {len(landing) - len(pendentes)} "
          f"| pendentes: {len(pendentes)}")
    for uri in pendentes[:20]:
        print("  ", uri)
    return "ingerir_bronze" if pendentes else "nada_a_fazer"


with DAG(
    dag_id="ingestao",
    description="landing/ -> Bronze (RECEPCAO_BRUTA + ARQUIVO), so se houver arquivo novo",
    schedule=None,                           # disparo manual; agendar quando o pipeline estiver inteiro
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "dlh", "retries": 0, "retry_delay": timedelta(minutes=2)},
    tags=["bronze", "ingestao", "canonico"],
) as dag:

    conferir = BranchPythonOperator(task_id="conferir_landing", python_callable=conferir_landing)

    nada_a_fazer = EmptyOperator(task_id="nada_a_fazer")

    ingerir = SparkSubmitOperator(
        task_id="ingerir_bronze",
        application="/opt/spark-jobs/ingestao_bronze.py",
        conn_id="spark_default",
        name="ingestao_bronze",
        env_vars={"MODELO_CANONICO": "/opt/canonico/modelo_canonico.yaml"},
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
        },
        verbose=False,
    )

    conferir >> [ingerir, nada_a_fazer]
