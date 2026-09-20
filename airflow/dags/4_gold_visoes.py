"""
DAG gold_visoes — le a Silver e reescreve as visoes da Gold.

    pitcic ──► problema_dados ──► ppcot ──► coc ──► amc

Uma tarefa por visao. Cada visao e UMA tabela na Gold, com os elementos do processo
doutrinario empilhados pelas fases (coluna FASE_NRO). Os parametros — raio da
corroboracao entre fontes, siglas do MD33-M-02 — estao em `visoes:` do modelo
canonico, e nao aqui.

A tabela e reescrita inteira a cada execucao: a Gold e funcao da Silver, e a versao
anterior fica no historico do Iceberg. Por isso nao ha conferencia de pendencia —
rodar de novo nunca duplica. Rode depois da 3_silver_evento.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from helpers.lineage_emitter import linhagem

# A mesma configuracao da 3_silver_evento, e pelos mesmos motivos (ver la): o
# driver do SparkSubmitOperator roda dentro do Airflow e nao le o spark-defaults.conf.
CONF_SPARK = {
    "spark.cores.max": "1",
    "spark.executor.memory": "4g",
    "spark.driver.memory": "2g",
    "spark.executor.cores": "1",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakehouse.type": "hive",
    "spark.sql.catalog.lakehouse.uri": "thrift://hive-metastore:9083",
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


def visao(nome: str, callbacks: list) -> SparkSubmitOperator:
    """Uma tarefa por visao. Muda so o nome e a linhagem declarada."""
    return SparkSubmitOperator(
        task_id=nome,
        application="/opt/spark-jobs/gold_visoes.py",
        application_args=["--visao", nome],
        conn_id="spark_default",
        name=f"gold_{nome}",
        env_vars={"MODELO_CANONICO": "/opt/canonico/modelo_canonico.yaml"},
        conf=CONF_SPARK,
        verbose=False,
        on_success_callback=callbacks,
    )


with DAG(
    dag_id="4_gold_visoes",
    description="Silver -> Gold: PITCIC, problemas de dados, PPCOT, COC e AMC",
    schedule=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args={"owner": "dlh", "retries": 0, "retry_delay": timedelta(minutes=2)},
    tags=["gold", "canonico"],
) as dag:

    # PITCIC: lugares e medidas da Silver, eventos da ameaca com o lugar de referencia
    # e a corroboracao entre fontes diferentes.
    pitcic = visao("pitcic", [
        linhagem(le=["silver.evento", "silver.medida_coordenacao", "silver.ref_gazetteer", "silver.ref_operacao"],
                 escreve=["gold.pitcic"], colunas={
            "elemento_nome":         [("silver.medida_coordenacao", "medida_nome"), ("silver.ref_gazetteer", "local_nome")],
            "geometria_wkt":         [("silver.evento", "geometria_wkt"), ("silver.ref_gazetteer", "geometria_wkt")],
            "local_referencia_nome": [("silver.ref_gazetteer", "local_nome")],
            "tipo_txt":              [("silver.evento", "tipo_cod")],
            "fonte_qnt":             [("silver.evento", "sistema_origem_cod")],
            "modalidade_txt":        [("silver.evento", "modalidade_origem_cod")],
            "extracao_modelo_nome":  [("silver.evento", "extracao_modelo_nome")],
            "evento_idt":            [("silver.evento", "evento_idt")],
        }),
    ])

    # PROBLEMA_DADOS: o que chegou errado e o que NAO chegou. Le tambem a Bronze,
    # porque o binario sem sidecar nunca virou evento — nao existe na Silver.
    problema_dados = visao("problema_dados", [
        linhagem(le=["bronze.arquivo", "bronze.recepcao_bruta", "bronze.rejeicao",
                     "silver.evento", "silver.situacao_unidade", "silver.ref_unidade"],
                 escreve=["gold.problema_dados"], colunas={
            "objeto_txt":      [("bronze.arquivo", "arquivo_uri_txt"),
                                ("bronze.recepcao_bruta", "origem_uri_txt"),
                                ("bronze.rejeicao", "origem_uri_txt")],
            "objeto_idt":      [("bronze.arquivo", "arquivo_idt"),
                                ("bronze.recepcao_bruta", "recepcao_idt"),
                                ("bronze.rejeicao", "rejeicao_idt")],
            "unidade_cod":     [("silver.ref_unidade", "unidade_cod"),
                                ("silver.evento", "unidade_reportante_cod")],
            "referencia_data": [("silver.evento", "ocorrencia_data")],
            "turno_cod":       [("silver.situacao_unidade", "turno_cod")],
            "modalidade_cod":  [("bronze.arquivo", "modalidade_cod"),
                                ("bronze.recepcao_bruta", "modalidade_cod")],
        }),
    ])

    # PPCOT: reune os dados disponiveis e indica onde podem ajudar no exame de
    # situacao; nao precisa representar todas as fases do processo.
    ppcot = visao("ppcot", [
        linhagem(le=["gold.pitcic", "silver.evento", "silver.situacao_unidade",
                     "silver.medida_coordenacao", "silver.ref_operacao", "silver.ref_unidade"],
                 escreve=["gold.ppcot"], colunas={
            "fator_decisao_cod": [("gold.pitcic", "etapa_cod"),
                                   ("silver.evento", "tipo_cod")],
            "insumo_tipo_cod":  [("gold.pitcic", "elemento_tipo_cod"),
                                   ("silver.evento", "tipo_cod"),
                                   ("silver.medida_coordenacao", "medida_especie_cod")],
            "insumo_nome":      [("gold.pitcic", "elemento_nome"),
                                   ("silver.medida_coordenacao", "medida_nome"),
                                   ("silver.ref_unidade", "unidade_nome")],
            "geometria_wkt":    [("gold.pitcic", "geometria_wkt"),
                                   ("silver.evento", "geometria_wkt"),
                                   ("silver.ref_operacao", "area_wkt")],
            "evento_idt":       [("gold.pitcic", "evento_idt"),
                                   ("silver.evento", "evento_idt")],
        }),
    ])

    # COC: o retrato corrente compartilhado. Posicao e situacao usam a ultima
    # observacao; fatos episodicos usam a janela declarada no modelo canonico.
    coc = visao("coc", [
        linhagem(le=["gold.pitcic", "silver.evento", "silver.situacao_unidade",
                     "silver.medida_coordenacao", "silver.ref_unidade"],
                 escreve=["gold.coc"], colunas={
            "elemento_nome":    [("gold.pitcic", "elemento_nome"),
                                   ("silver.medida_coordenacao", "medida_nome"),
                                   ("silver.ref_unidade", "unidade_nome")],
            "ocorrencia_data":  [("gold.pitcic", "inicio_data"),
                                   ("silver.evento", "ocorrencia_data")],
            "geometria_wkt":    [("gold.pitcic", "geometria_wkt"),
                                   ("silver.evento", "geometria_wkt")],
            "ef_disponibilidade_pctl": [("silver.situacao_unidade", "ef_presente_qnt"),
                                          ("silver.situacao_unidade", "ef_previsto_qnt")],
            "vtr_disponibilidade_pctl": [("silver.situacao_unidade", "vtr_operacional_qnt"),
                                           ("silver.situacao_unidade", "vtr_total_qnt")],
            "suprimento_minimo_dias": [("silver.situacao_unidade", "sup_cl_i_dias"),
                                         ("silver.situacao_unidade", "sup_cl_iii_dias"),
                                         ("silver.situacao_unidade", "sup_cl_v_dias"),
                                         ("silver.situacao_unidade", "sup_cl_viii_dias")],
            "evento_idt":       [("gold.pitcic", "evento_idt"),
                                   ("silver.evento", "evento_idt")],
        }),
    ])

    # AMC: series observadas e tendencia. Metas e classificacao como eficacia ou
    # desempenho dependem do plano e, por isso, nao sao inferidas pelo codigo.
    amc = visao("amc", [
        linhagem(le=["gold.pitcic", "silver.evento", "silver.situacao_unidade"],
                 escreve=["gold.amc"], colunas={
            "indicador_cod":    [("silver.situacao_unidade", "evento_idt"),
                                   ("gold.pitcic", "elemento_tipo_cod")],
            "periodo_data":     [("silver.evento", "ocorrencia_data"),
                                   ("gold.pitcic", "inicio_data")],
            "valor_nro":        [("silver.situacao_unidade", "ef_presente_qnt"),
                                   ("silver.situacao_unidade", "vtr_operacional_qnt"),
                                   ("silver.situacao_unidade", "combustivel_pctl"),
                                   ("gold.pitcic", "evento_qnt")],
            "fonte_qnt":        [("silver.evento", "sistema_origem_cod")],
            "evento_idt":       [("silver.evento", "evento_idt"),
                                   ("gold.pitcic", "evento_idt")],
        }),
    ])

    # Em serie, e nao em paralelo: o worker Spark e unico (ver 3_silver_evento).
    pitcic >> problema_dados >> ppcot >> coc >> amc
