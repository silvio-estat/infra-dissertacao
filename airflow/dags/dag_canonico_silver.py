"""
DAG canonico_silver — aplica o de/para do modelo canonico e escreve a Silver.

    conferir_pendentes ──► c2a_posicao ──► c2a_mcc ──► relper_situacao
                       └─► nada_a_fazer

Cada tarefa processa UMA receita do YAML (um bloco dentro de `fontes:`). Elas
correm em serie porque o Spark tem um worker so.

O que uma tarefa faz, em uma frase: le a Bronze, resolve cada campo pela regra
declarada em canonico/modelo_canonico.yaml e grava em EVENTO — e, quando a
receita tem extensao, tambem na tabela dela.

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

# Registro da Bronze que ainda nao virou evento. Todo evento guarda o
# RECEPCAO_IDT de onde nasceu, entao a conta e uma juncao so.
# O filtro lista o que esta DAG SABE transformar hoje: as demais fontes entram
# quando suas DAGs de extracao existirem, e ate la nao contam como pendencia.
PENDENTES_SQL = """
    SELECT count(*)
    FROM iceberg.bronze.recepcao_bruta r
    LEFT JOIN iceberg.silver.evento e ON e.recepcao_idt = r.recepcao_idt
    WHERE e.recepcao_idt IS NULL
      AND (r.sistema_origem_cod = 'C2_A'
           OR (r.sistema_origem_cod = 'RELPER' AND r.modalidade_cod = 'PLANILHA'))
"""

CONF_SPARK = {
    "spark.cores.max": "1",
    # O spark-defaults.conf da imagem limita o executor a 1 GB, e o driver do
    # SparkSubmitOperator roda dentro do Airflow, que nem le esse arquivo. Um
    # MERGE que ATUALIZA (reprocessar dado que ja esta na Silver) reescreve a
    # tabela e nao cabe em 1 GB — o executor morre com codigo 134. O worker
    # oferece 20 GB; 4 basta.
    "spark.executor.memory": "4g",
    "spark.driver.memory": "2g",
    "spark.executor.cores": "1",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakehouse.type": "hive",
    "spark.sql.catalog.lakehouse.uri": "thrift://hive-metastore:9083",
    # ATENCAO: o driver do SparkSubmitOperator roda DENTRO do Airflow, que nao
    # tem o spark-defaults.conf da imagem do Spark. Tudo o que aquele arquivo
    # ajusta precisa ser repetido aqui, ou o job roda com outra configuracao —
    # sem io-impl e com 200 particoes, o leitor vetorizado do Iceberg estoura a
    # memoria FORA do heap e o executor morre sem excecao Java (codigo 134).
    "spark.sql.catalog.lakehouse.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
    "spark.sql.shuffle.partitions": "4",
    "spark.default.parallelism": "4",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.sql.catalog.lakehouse.warehouse": "s3a://lakehouse/warehouse",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.access.key": os.environ.get("MINIO_ROOT_USER", ""),
    "spark.hadoop.fs.s3a.secret.key": os.environ.get("MINIO_ROOT_PASSWORD", ""),
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider":
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}


def conferir_pendentes() -> str:
    import trino

    cur = trino.dbapi.connect(host="trino", port=8090, user="airflow", http_scheme="http").cursor()
    cur.execute(PENDENTES_SQL)
    pendentes = cur.fetchone()[0]
    print(f"registros da Bronze sem evento: {pendentes}")
    return "c2a_posicao" if pendentes else "nada_a_fazer"


def transformar(nome: str, fonte: str, receita: str, callbacks: list) -> SparkSubmitOperator:
    """Uma tarefa por receita. Muda so a fonte, a receita e a linhagem declarada."""
    return SparkSubmitOperator(
        task_id=nome,
        application="/opt/spark-jobs/canonico_para_silver.py",
        application_args=["--fonte", fonte, "--tipo", receita],
        conn_id="spark_default",
        name=f"canonico_silver_{nome}",
        env_vars={"MODELO_CANONICO": "/opt/canonico/modelo_canonico.yaml"},
        conf=CONF_SPARK,
        verbose=False,
        on_success_callback=callbacks,
    )


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

    # C2_A chega como JSON ja estruturado: o registro bruto e a unica entrada.
    posicao = transformar("c2a_posicao", "C2_A", "posicao", [
        linhagem(le=["bronze.recepcao_bruta"], escreve=["silver.evento"], colunas={
            "geometria_wkt":          [("bronze.recepcao_bruta", "conteudo_json_txt")],
            "unidade_reportante_cod": [("bronze.recepcao_bruta", "conteudo_json_txt")],
            "operacao_cod":           [("bronze.recepcao_bruta", "conteudo_json_txt")],
            "recepcao_idt":           [("bronze.recepcao_bruta", "recepcao_idt")],
        }),
    ])
    mcc = transformar("c2a_mcc", "C2_A", "mcc", [
        linhagem(le=["bronze.recepcao_bruta"], escreve=["silver.evento"], colunas={
            "geometria_wkt":      [("bronze.recepcao_bruta", "conteudo_json_txt")],
            "tipo_cod":           [("bronze.recepcao_bruta", "conteudo_json_txt")],
            "funcao_combate_cod": [("bronze.recepcao_bruta", "conteudo_json_txt")],
        }),
    ])

    # RELPER chega como planilha: os numeros so existem depois da extracao, e por
    # isso a linhagem tem tres entradas. Escreve em duas tabelas.
    bronze_do_arquivo = ["bronze.extracao", "bronze.arquivo", "bronze.recepcao_bruta"]
    relper = transformar("relper_situacao", "RELPER", "situacao", [
        linhagem(le=bronze_do_arquivo, escreve=["silver.evento"], colunas={
            "unidade_reportante_cod": [("bronze.extracao", "saida_txt")],
            "ocorrencia_data":        [("bronze.recepcao_bruta", "conteudo_json_txt")],
            "operacao_cod":           [("bronze.recepcao_bruta", "conteudo_json_txt")],
            "arquivo_idt":            [("bronze.arquivo", "arquivo_idt")],
            "extracao_idt":           [("bronze.extracao", "extracao_idt")],
        }),
        linhagem(le=bronze_do_arquivo, escreve=["silver.situacao_unidade"], colunas={
            "ef_presente_qnt":     [("bronze.extracao", "saida_txt")],
            "vtr_operacional_qnt": [("bronze.extracao", "saida_txt")],
            "necessidade_txt":     [("bronze.extracao", "saida_txt")],
            "atributo_extra_txt":  [("bronze.extracao", "saida_txt")],
            "turno_cod":           [("bronze.recepcao_bruta", "conteudo_json_txt")],
        }),
    ])

    conferir >> [posicao, nada_a_fazer]
    posicao >> mcc >> relper
